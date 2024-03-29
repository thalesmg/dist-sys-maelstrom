#!/usr/bin/env elixir

Mix.install([:jason, :hallux])

defmodule SparseSeq do
  alias Hallux.Seq

  defstruct [
    n: 0,
    is: Seq.new(),
    xs: Seq.new(),
  ]

  def new() do
    %__MODULE__{}
  end

  def new(ixs) do
    Enum.reduce(ixs, new(), fn {i, x}, acc ->
      insert(acc, i, x)
    end)
  end

  def insert(ss = %__MODULE__{}, i, x) do
    %{ss | n: ss.n + 1, is: Seq.snoc(ss.is, i), xs: Seq.snoc(ss.xs, x)}
  end

  def slice_at(ss = %__MODULE__{}, i) do
    i = find_idx(ss, i, 0, ss.n - 1)
    {_lis, ris} = Seq.split_at(ss.is, i)
    {_lxs, rxs} = Seq.split_at(ss.xs, i)
    n = Seq.size(ris)
    %__MODULE__{n: n, is: ris, xs: rxs}
  end

  def concat(ss1 = %__MODULE__{}, ss2 = %__MODULE__{}) do
    %__MODULE__{
      n: ss1.n + ss2.n,
      is: Seq.concat(ss1.is, ss2.is),
      xs: Seq.concat(ss1.xs, ss2.xs),
    }
  end

  defp find_idx(ss = %__MODULE__{}, target, i, i) do
    if Enum.at(ss.is, i) < target do
      i + 1
    else
      i
    end
  end
  defp find_idx(ss = %__MODULE__{}, target, il, ir) do
    i = div(il + ir, 2)
    case Enum.at(ss.is, i) do
      ^target ->
        i

      j when j < target ->
        find_idx(ss, target, i + 1, ir)

      _ ->
        find_idx(ss, target, il, i)
    end
  end
end

defmodule SparseSeq2 do
  alias Hallux.OrderedMap, as: OM

  defdelegate new(ixs \\ []), to: OM

  defdelegate insert(ss, k, v), to: OM

  def slice_at(ss, i) do
    {_less, _eq, greater} = OM.split(ss, i)
    greater
  end
end

defmodule SparseSeq3 do
  alias Hallux.OrderedMap, as: OM

  # keys are integers
  defstruct [
    contiguous: OM.new(),
    staging: :gb_trees.empty(),
  ]

  # assumes ixs is contiguous (and non-duplicated)
  def new(ixs \\ []) do
    %__MODULE__{
      contiguous: OM.new(ixs),
      staging: :gb_trees.empty(),
    }
  end

  def insert(ss = %__MODULE__{}, k, v) when is_integer(k) do
    {last_k, _last_v} = Enum.at(ss.contiguous, -1)
    if k == last_k + 1 do
      ss = %{ss | contiguous: OM.insert(ss.contiguous, k, v)}
      maybe_merge_staging(ss)
    else
      %{ss | staging: :gb_trees.insert(k, v, ss.staging)}
    end
  end

  def slice_at(ss = %__MODULE__{contiguous: contiguous}, i) do
    {_less, _eq, greater} = OM.split(contiguous, i)
    greater
  end

  defp maybe_merge_staging(ss = %__MODULE__{contiguous: contiguous, staging: staging}) do
    with false <- :gb_trees.is_empty(staging),
         {first_k, first_v, staging} <- :gb_trees.take_smallest(staging),
         {last_k, _last_v} = Enum.at(contiguous, -1),
         true <- first_k == last_k + 1 do
      contiguous = OM.insert(contiguous, first_k, first_v)
      ss = %{ss | contiguous: contiguous, staging: staging}
      maybe_merge_staging(ss)
    else
      _ ->
        ss
    end
  end
end

defmodule Kafka do
  use GenServer

  alias SparseSeq2, as: SS

  @offset "offset"

  def start_link() do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def raw(raw) do
    GenServer.cast(__MODULE__, {:raw, raw})
  end

  def message(msg) do
    GenServer.cast(__MODULE__, {:msg, msg})
  end

  def init(nil) do
    state = %{
      node_id: nil,
      nodes: [],
      offsets: %{},
      topics: %{},
      committed: %{},
      pending: %{},
      # -1 so that 0 goes straight into the contiguous collection.
      last_offsets: %{},
      staging: %{},
    }
    {:ok, state}
  end

  def handle_cast({:raw, raw}, state) do
    with {:ok, msg} <- Jason.decode(raw) do
      message(msg)
    end
    {:noreply, state}
  end
  #
  # external
  #
  def handle_cast({:msg, %{"body" => %{"type" => "init"}} = msg}, state) do
    log(msg, "got init")
    state = %{state | node_id: msg["body"]["node_id"], nodes: msg["body"]["node_ids"]}
    reply!(state.node_id, msg, %{type: "init_ok"})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "send"}} = msg}, state) do
    log(msg, "got send req")
    %{"key" => topic, "msg" => v, "msg_id" => orig_msg_id} = msg["body"]
    {offset, state} = get_known_offset(state, topic)
    %{body: %{"msg_id" => msg_id}} = kv_cas!(@offset <> "_" <> topic, offset, offset + 1, state)
    orig_key = {:orig, msg["src"], orig_msg_id}
    state =
      state
      |> put_in([:pending, msg_id], {:bump_offset, topic, {:send_ok, topic, v, orig_key}})
      |> put_in([:pending, orig_key], %{orig_msg: msg})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "poll"}} = msg}, state) do
    log(msg, "got poll req")
    offs = msg["body"]["offsets"]
    offsets = Enum.reduce(offs, %{}, fn {topic, off}, acc ->
      case Map.fetch(state.topics, topic) do
        {:ok, offset} ->
          os =
            offset
            |> SS.slice_at(off - 1)
            |> serialize_sparse()
          Map.put(acc, topic, os)

        :error ->
          acc
      end
    end)
    reply!(state.node_id, msg, %{type: "poll_ok", msgs: offsets})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "list_committed_offsets"}} = msg}, state) do
    log(msg, "got list_committed_offsets req")
    topics = msg["body"]["keys"]
    offsets = Enum.reduce(topics, %{}, fn topic, acc ->
      case Map.fetch(state.committed, topic) do
        {:ok, o} ->
          Map.put(acc, topic, o)

        :error ->
          acc
      end
    end)
    reply!(state.node_id, msg, %{type: "list_committed_offsets_ok", offsets: offsets})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "commit_offsets"}} = msg}, state) do
    log(msg, "got commit_offsets req")
    offsets = msg["body"]["offsets"]
    committed = Enum.reduce(offsets, state.committed, fn {topic, i}, acc ->
      Map.update(acc, topic, i, &max(&1, i))
    end)
    state = %{state | committed: committed}
    state.nodes
    |> Stream.filter(& &1 != state.node_id)
    |> Enum.each(fn n ->
      body = %{
        type: "replicate_commit",
        committed: committed,
      }
      send!(state.node_id, n, body)
    end)
    reply!(state.node_id, msg, %{type: "commit_offsets_ok"})
    {:noreply, state}
  end
  #
  # internal
  #
  def handle_cast({:msg, %{"body" => %{"type" => "replicate_send"}} = msg}, state) do
    log(msg, "got replicate_send req")
    %{"k" => topic, "v" => v, "offset" => offset} = msg["body"]
    # offset + 1 because offset is already used up here
    new_offset = max(state.offsets[topic] || 0, offset + 1)
    state =
      state
      |> insert(topic, offset, v)
      |> put_in([:offsets, topic], new_offset)
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "replicate_commit"}} = msg}, state) do
    log(msg, "got replicate_commit req")
    %{"committed" => committed} = msg["body"]
    committed = Enum.reduce(committed, state.committed, fn {topic, i}, acc ->
      Map.update(acc, topic, i, &max(&1, i))
    end)
    state = %{state | committed: committed}
    {:noreply, state}
  end
  #
  # lin-kv
  #
  def handle_cast({:msg, %{"body" => %{"type" => "read_ok"}} = msg}, state) do
    log(msg, "seq-kv read_ok")
    in_reply_to = msg["body"]["in_reply_to"]
    val = msg["body"]["value"]
    case pop_in(state, [:pending, in_reply_to]) do
      {{:bump_offset, topic, next_action}, state} ->
        offset = val
        state = put_in(state, [:offsets, topic], offset)
        %{body: %{"msg_id" => msg_id}} = kv_cas!(@offset <> "_" <> topic, offset, offset + 1, state)
        state = put_in(state, [:pending, msg_id], {:bump_offset, topic, next_action})
        {:noreply, state}
    end
  end
  def handle_cast({:msg, %{"body" => %{"type" => "cas_ok"}} = msg}, state) do
    log(msg, "seq-kv cas_ok")
    in_reply_to = msg["body"]["in_reply_to"]
    case pop_in(state, [:pending, in_reply_to]) do
      {{:bump_offset, topic, next_action}, state} ->
        # Optimistically, next offset would be ours.
        state = update_in(state, [:offsets, topic], & &1 + 1)
        state = handle_next_action(state, next_action)
        {:noreply, state}
    end
  end
  def handle_cast({:msg, %{"body" => %{"type" => "error", "code" => 22, "in_reply_to" => orig_msg_id}} = msg}, state) do
    log(msg, "seq-kv write or cas error (stale view)")
    in_reply_to = msg["body"]["in_reply_to"]
    case pop_in(state, [:pending, in_reply_to]) do
      {{:bump_offset, topic, next_action}, state} ->
        cur_val = get_current_value_hack(msg)
        state = put_in(state, [:offsets, topic], cur_val)
        %{body: %{"msg_id" => msg_id}} = kv_cas!(@offset <> "_" <> topic, cur_val, cur_val + 1, state)
        state = put_in(state, [:pending, msg_id], {:bump_offset, topic, next_action})
        {:noreply, state}
    end
  end

  defp insert(state, topic, offset, v) do
    last_offset = state.last_offsets[topic] || -1
    if last_offset + 1 == offset do
      state
      |> insert1(topic, offset, v)
      |> update_in([:staging, topic], fn
        nil ->
          :gb_trees.empty()

        s ->
          s
      end)
      |> maybe_merge_staging(topic)
    else
      update_in(state, [:staging, topic], & :gb_trees.insert(offset, v, &1))
    end
  end

  defp insert1(state, topic, offset, v) do
    state
    |> update_in([:last_offsets, topic], & (&1 || - 1) + 1)
    |> Map.update!(:topics, fn old ->
      Map.update(old, topic, SS.new([{offset, v}]), & SS.insert(&1, offset, v))
    end)
  end

  defp maybe_merge_staging(state, topic) do
    with false <- :gb_trees.is_empty(state.staging[topic]),
         {first_k, first_v, staging} <- :gb_trees.take_smallest(state.staging[topic]),
         true <- first_k == state.last_offsets[topic] + 1 do
      state
      |> put_in([:staging, topic], staging)
      |> insert1(topic, first_k, first_v)
      |> maybe_merge_staging(topic)
    else
      _ ->
        state
    end
  end

  defp get_known_offset(state, topic) do
    case state.offsets do
      %{^topic => offset} ->
        {offset, state}

      _ ->
        state =
          state
          |> put_in([:offsets, topic], 0)
          |> put_in([:last_offsets, topic], -1)
          |> put_in([:staging, topic], :gb_trees.empty())
        {0, state}
    end
  end

  defp get_current_value_hack(%{"body" => %{"text" => "current value " <> txt}}) do
    {cur_val, _} = Integer.parse(txt)
    cur_val
  end

  defp handle_next_action(state, {:send_ok, topic, v, orig_key}) do
    # Our current offset has just been cas'ed and reserved for us.
    # But we also just bumped it internally.
    offset = state.offsets[topic] - 1
    state = insert(state, topic, offset, v)
    state.nodes
    |> Stream.filter(& &1 != state.node_id)
    |> Enum.each(fn n ->
      body = %{
        type: "replicate_send",
        k: topic,
        v: v,
        offset: offset,
      }
      send!(state.node_id, n, body)
    end)
    {%{orig_msg: orig_msg}, state} = pop_in(state, [:pending, orig_key])
    reply!(state.node_id, orig_msg, %{type: "send_ok", offset: offset})
    state
  end

  defp serialize_sparse(ss) do
    Enum.map(ss, fn {i, x} -> [i, x] end)
  end

  defp reply!(from, original_msg, body) do
    body = Map.merge(
      body,
      %{
        "msg_id" => next_message_id(),
        in_reply_to: original_msg["body"]["msg_id"],
      }
    )
    send!(from, original_msg["src"], body)
  end

  defp send!(from, to, body) do
    body = Map.put(body, "msg_id", next_message_id())
    msg0 = %{src: from, dest: to, body: body}
    msg = Jason.encode!(%{src: from, dest: to, body: body})
    log(msg, "gonna send")
    IO.puts(msg)
    msg0
  end

  defp kv_read!(k, state) do
    body = %{
      type: "read",
      key: k,
    }
    send!(state.node_id, "lin-kv", body)
  end

  defp kv_write!(k, v, state) do
    body = %{
      type: "write",
      key: k,
      value: v,
    }
    send!(state.node_id, "lin-kv", body)
  end

  defp kv_cas!(k, v0, v1, state) do
    body = %{
      type: "cas",
      key: k,
      from: v0,
      to: v1,
      create_if_not_exists: true,
    }
    send!(state.node_id, "lin-kv", body)
  end


  defp next_message_id() do
    case Process.get(:msg_id) do
      nil ->
        Process.put(:msg_id, 1)
        0
      n ->
        Process.put(:msg_id, n + 1)
        n
    end
  end

  defp log(x, label) do
    IO.inspect(:stderr, x, label: label)
    # _ = x
    # _ = label
    # nil
  end
end

defmodule Loop do
  def loop() do
    IO.stream()
    |> Stream.each(fn l ->
      # IO.inspect(:stderr, l, label: "got")
      Kafka.raw(l)
    end)
    |> Stream.run()
  end
end

spawn(&Loop.loop/0)
{:ok, _broadcast} = Kafka.start_link()

receive do
  _ -> :ok
end

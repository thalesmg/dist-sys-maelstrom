#!/usr/bin/env elixir

Mix.install([:jason])

defmodule GCounter do
  use GenServer

  @retry_interval 500
  @kv_vsn "vsn"

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
      vsn: 0,
      x: 0,
      nodes: [],
      node_id: nil,
      pending: %{},
    }
    {:ok, state}
  end

  def handle_cast({:raw, raw}, state) do
    with {:ok, msg} <- Jason.decode(raw) do
      message(msg)
    end
    {:noreply, state}
  end
  ## external msgs
  def handle_cast({:msg, %{"body" => %{"type" => "init"}} = msg}, state) do
    log(msg, "got init")
    state = %{state | node_id: msg["body"]["node_id"], nodes: msg["body"]["node_ids"]}
    kv_write!(state.node_id, 0, state)
    reply!(state.node_id, msg, %{type: "init_ok"})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "add"}} = msg}, state) do
    delta = msg["body"]["delta"]
    state = %{state | x: state.x + delta}
    # kv_write!(state.node_id, state.x, state)
    state = bump_vsn(state)
    kv_cas!(state.node_id, state.x - delta, state.x, state)
    reply!(state.node_id, msg, %{type: "add_ok"})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "read"}} = msg}, state) do
    log(msg, "got read req")
    orig_msg_id = msg["body"]["msg_id"]
    pending =
      state.nodes
      # |> Stream.filter(& &1 != state.node_id)
      |> Enum.reduce(state.pending, fn n, acc ->
        %{body: %{"msg_id" => msg_id}} = kv_read!(n, state)
        acc
        # |> Map.update(orig_msg_id, %{pending: MapSet.new([msg_id]), orig_msg: msg, x: state.x}, fn old ->
        |> Map.update(orig_msg_id, %{pending_replies: MapSet.new([msg_id]), orig_msg: msg, x: 0}, fn old ->
          Map.update!(old, :pending_replies, &MapSet.put(&1, msg_id))
        end)
        |> Map.put(msg_id, {:read, orig_msg_id})
      end)
    state = %{state | pending: pending}
    # reply!(state.node_id, msg, %{type: "read_ok", value: -1})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "read_ok"}} = msg}, state) do
    log(msg, "seq-kv read_ok")
    in_reply_to = msg["body"]["in_reply_to"]
    val = msg["body"]["value"]
    case pop_in(state, [:pending, in_reply_to]) do
      {{:read, orig_msg_id}, state} ->
        handle_read_reply(state, orig_msg_id, in_reply_to, val)

      {:read_vsn, state} ->
        bump_vsn(state, val)
    end
  end
  def handle_cast({:msg, %{"body" => %{"type" => "write_ok"}} = msg}, state) do
    log(msg, "seq-kv write_ok")
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "cas_ok"}} = msg}, state) do
    log(msg, "seq-kv cas_ok")
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "error", "code" => 22, "in_reply_to" => orig_msg_id}} = msg}, state) do
    log(msg, "seq-kv write error")
    # retry(msg)
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "error", "code" => 20}} = msg}, state) do
    log(msg, "seq-kv read or cas error?")
    # retry(msg)
    in_reply_to = msg["body"]["in_reply_to"]
    case pop_in(state, [:pending, in_reply_to]) do
      {:bump_vsn, state} ->
        state = read_vsn(state)
        {:noreply, state}

      {:read_vsn, state} ->
        state = read_vsn(state)
        {:noreply, state}

      {{:read, orig_msg_id}, state} ->
        val = 0
        handle_read_reply(state, orig_msg_id, in_reply_to, val)
    end
  end
  ## internal msgs
  # def handle_cast({:add, x}, state) do
  #   log(x, "add")

  #   {:noreply, state}
  # end

  def handle_info({:retry, pending_key = {n, _broadcast_id}}, state) do
    log(pending_key, "gonna retry?")
    log(state, "gonna retry? state>>")
    with {:ok, body} <- Map.fetch(state.pending, pending_key) do
      log(pending_key, "retrying...")
      send!(state.node_id, n, body)
      start_retry_timer(pending_key)
    else
      _ -> log(pending_key, "removed")
    end
    {:noreply, state}
  end

  defp handle_read_reply(state, orig_msg_id, in_reply_to, val) do
    state = maybe_reply_read(state, in_reply_to, val, orig_msg_id)
    {:noreply, state}
  end

  defp maybe_reply_read(state, in_reply_to, val, orig_msg_id) do
    %{pending_replies: p, orig_msg: orig_msg, x: x} = state.pending[orig_msg_id]
    p = MapSet.delete(p, in_reply_to)
    if MapSet.size(p) == 0 do
      log(x + val, "#{orig_msg_id} is done, replying")
      body = %{type: "read_ok", value: x + val}
      reply!(state.node_id, orig_msg, body)
      {_, state} = pop_in(state, [:pending, orig_msg_id])
      state
    else
      log(p, "remaining for #{orig_msg_id}")
      update_in(state, [:pending, orig_msg_id], fn old ->
        old
        |> Map.put(:pending_replies, p)
        |> Map.update!(:x, & &1 + val)
      end)
    end
  end

  defp bump_vsn(state) do
    bump_vsn(state, state.vsn)
  end

  defp bump_vsn(state, curr_val) do
    %{body: %{"msg_id" => msg_id}} = kv_cas!(@kv_vsn, curr_val, curr_val + 1, state)
    state
    |> Map.put(:vsn, curr_val + 1)
    |> put_in([:pending, msg_id], :bump_vsn)
  end

  defp read_vsn(state) do
    %{body: %{"msg_id" => msg_id}} = kv_read!(@kv_vsn, state)
    put_in(state, [:pending, msg_id], :read_vsn)
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

  defp kv_read!(k, state) do
    body = %{
      type: "read",
      key: k,
    }
    send!(state.node_id, "seq-kv", body)
  end

  defp kv_write!(k, v, state) do
    body = %{
      type: "write",
      key: k,
      value: v,
    }
    send!(state.node_id, "seq-kv", body)
  end

  defp kv_cas!(k, v0, v1, state) do
    body = %{
      type: "cas",
      key: k,
      from: v0,
      to: v1,
      create_if_not_exists: true,
    }
    send!(state.node_id, "seq-kv", body)
  end

  defp send!(from, to, body) do
    body = Map.put(body, "msg_id", next_message_id())
    msg0 = %{src: from, dest: to, body: body}
    msg = Jason.encode!(%{src: from, dest: to, body: body})
    log(msg, "gonna send")
    IO.puts(msg)
    msg0
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

  defp start_retry_timer(pending_key) do
    Process.send_after(self(), {:retry, pending_key}, @retry_interval)
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
      GCounter.raw(l)
    end)
    |> Stream.run()
  end
end

spawn(&Loop.loop/0)
{:ok, _broadcast} = GCounter.start_link()

receive do
  _ -> :ok
end

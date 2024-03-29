#!/usr/bin/env elixir

Mix.install([:jason])

defmodule GCounter do
  use GenServer

  @retry_interval 500
  @vsn "vsn"
  @val "value"

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
    reply!(state.node_id, msg, %{type: "init_ok"})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "add"}} = msg}, state) do
    log(msg, "got add req")
    delta = msg["body"]["delta"]
    %{body: %{"msg_id" => msg_id}} = kv_cas!(@vsn, state.vsn, state.vsn + 1, state)
    state = put_in(state, [:pending, msg_id], {:bump, {:add, delta}})
    reply!(state.node_id, msg, %{type: "add_ok"})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "read"}} = msg}, state) do
    log(msg, "got read req")
    orig_msg_id = msg["body"]["msg_id"]
    %{body: %{"msg_id" => msg_id}} = kv_cas!(@vsn, state.vsn, state.vsn + 1, state)
    orig_key = {:orig, orig_msg_id}
    state =
      state
      |> put_in([:pending, msg_id], {:bump, {:read, orig_key}})
      |> put_in([:pending, orig_key], %{orig_msg: msg})

    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "read_ok"}} = msg}, state) do
    log(msg, "seq-kv read_ok")
    in_reply_to = msg["body"]["in_reply_to"]
    val = msg["body"]["value"]
    case pop_in(state, [:pending, in_reply_to]) do
      {{:read, orig_key}, state} ->
        state = %{state | x: max(val, state.x)}
        # assert the version so that we try again if there were
        # concurrent updates
        %{body: %{"msg_id" => msg_id}} = kv_cas!(@vsn, state.vsn, state.vsn, state)
        state = put_in(state, [:pending, msg_id], {:assert_vsn, {:read, orig_key}, {:finalize_read, orig_key}})
        {:noreply, state}

      {{:read_vsn, next_action}, state} ->
        state = %{state | vsn: val}
        %{body: %{"msg_id" => msg_id}} = kv_cas!(@vsn, state.vsn, state.vsn + 1, state)
        state = put_in(state, [:pending, msg_id], {:bump, next_action})
        {:noreply, state}
    end
  end
  def handle_cast({:msg, %{"body" => %{"type" => "write_ok"}} = msg}, state) do
    log(msg, "seq-kv write_ok")
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "cas_ok"}} = msg}, state) do
    log(msg, "seq-kv cas_ok")
    in_reply_to = msg["body"]["in_reply_to"]
    case pop_in(state, [:pending, in_reply_to]) do
      {{:add, delta}, state} ->
        # added successfully; just drop and "commit" the state
        state = %{state | x: state.x + delta}
        {:noreply, state}

      {{:bump, next_action}, state} ->
        state = %{state | vsn: state.vsn + 1}
        state = handle_next_action(state, next_action)
        {:noreply, state}

      {{:assert_vsn, retry_action, next_action}, state} ->
        state = handle_next_action(state, next_action)
        {:noreply, state}
    end
  end
  def handle_cast({:msg, %{"body" => %{"type" => "error", "code" => 22, "in_reply_to" => orig_msg_id}} = msg}, state) do
    log(msg, "seq-kv write or cas error (stale view)")
    in_reply_to = msg["body"]["in_reply_to"]
    case pop_in(state, [:pending, in_reply_to]) do
      {{:add, delta}, state} ->
        # sync
        state = %{state | vsn: state.vsn + 1}
        %{body: %{"msg_id" => msg_id}} = kv_cas!(@vsn, state.vsn, state.vsn + 1, state)
        state = put_in(state, [:pending, msg_id], {:bump, {:add, delta}})
        {:noreply, state}

      {{:read, orig_key}, state} ->
        # read current value and try again
        state = %{state | vsn: state.vsn + 1}
        %{body: %{"msg_id" => msg_id}} = kv_cas!(@vsn, state.vsn, state.vsn + 1, state)
        state = put_in(state, [:pending, msg_id], {:bump, {:read, orig_key}})
        {:noreply, state}

      {{:bump, next_action}, state} ->
        %{body: %{"msg_id" => msg_id}} = kv_read!(@vsn, state)
        state = put_in(state, [:pending, msg_id], {:read_vsn, next_action})
        {:noreply, state}

      {{:assert_vsn, retry_action, next_action}, state} ->
        %{body: %{"msg_id" => msg_id}} = kv_read!(@vsn, state)
        state = put_in(state, [:pending, msg_id], {:read_vsn, retry_action})
        {:noreply, state}
    end
  end
  def handle_cast({:msg, %{"body" => %{"type" => "error", "code" => 20}} = msg}, state) do
    log(msg, "seq-kv read or cas error (key doesn't exist)")
    # this can only be the response to a read, since we're using
    # "create-if-not-exists" during cas operations.
    in_reply_to = msg["body"]["in_reply_to"]
    case pop_in(state, [:pending, in_reply_to]) do
      {{:read, orig_msg_id}, state} ->
        val = 0
        handle_read_reply(state, orig_msg_id, val)
    end
  end

  defp handle_read_reply(state, orig_msg_id, val) do
    {%{orig_msg: orig_msg}, state} = pop_in(state, [:pending, orig_msg_id])
    body = %{type: "read_ok", value: val}
    reply!(state.node_id, orig_msg, body)
    {:noreply, state}
  end

  defp handle_next_action(state, {:add, delta}) do
    %{body: %{"msg_id" => msg_id}} = kv_cas!(@val, state.x, state.x + delta, state)
    put_in(state, [:pending, msg_id], {:add, delta})
  end
  defp handle_next_action(state, {:read, orig_key}) do
    %{body: %{"msg_id" => msg_id}} = kv_read!(@val, state)
    put_in(state, [:pending, msg_id], {:read, orig_key})
  end
  defp handle_next_action(state, {:assert_vsn, retry_action, next_action}) do
    %{body: %{"msg_id" => msg_id}} = kv_cas!(@vsn, state.vsn, state.vsn, state)
    state = put_in(state, [:pending, msg_id], {:assert_vsn, retry_action, next_action})
  end
  defp handle_next_action(state, {:finalize_read, orig_key}) do
    # fixme
    {:noreply, state} = handle_read_reply(state, orig_key, state.x)
    state
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

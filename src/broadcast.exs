#!/usr/bin/env elixir

Mix.install([:jason])

defmodule Broadcast do
  use GenServer

  @retry_interval 500

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
      neighbors: [],
      node_id: nil,
      pending: %{},
      messages: :sets.new(version: 2)
    }
    {:ok, state}
  end

  def handle_cast({:raw, raw}, state) do
    with {:ok, msg} <- Jason.decode(raw) do
      message(msg)
    end
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "init"}} = msg}, state) do
    # log(msg, "got init")
    state = %{state | node_id: msg["body"]["node_id"]}
    reply!(state.node_id, msg, %{type: "init_ok"})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "topology"}} = msg}, state) do
    # log(msg, "got topology")
    state = %{state | neighbors: msg["body"]["topology"][state.node_id]}
    reply!(state.node_id, msg, %{type: "topology_ok"})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "read"}} = msg}, state) do
    # log(msg, "got read")
    reply!(state.node_id, msg, %{type: "read_ok", messages: :sets.to_list(state.messages)})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "broadcast"} = body} = msg}, state) do
    # log(msg, "got broadcast")
    broadcast_id = body["broadcast_id"] || body["msg_id"]
    e = body["message"]
    state = if :sets.is_element(e, state.messages) do
      state
    else
      state.neighbors
      |> Stream.filter(& &1 != msg["src"])
      |> Enum.reduce(state, fn n, state ->
        pbody = Map.put(body, "broadcast_id", broadcast_id)
        send!(state.node_id, n, pbody)
        pending_key = {n, broadcast_id}
        start_retry_timer(pending_key)
        Map.update!(state, :pending, &Map.put(&1, pending_key, pbody))
      end)
      |> Map.update!(:messages, &:sets.add_element(e, &1))
    end
    if msg["src"] in state.neighbors do
      reply!(state.node_id, msg, %{type: "broadcast_ok", broadcast_id: broadcast_id})
    else
      reply!(state.node_id, msg, %{type: "broadcast_ok"})
    end
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "broadcast_ok"}} = msg}, state) do
    log(msg, "got broadcast_ok")
    pending_key = {msg["src"], msg["body"]["broadcast_id"]}
    state = Map.update!(state, :pending, &Map.delete(&1, pending_key))
    {:noreply, state}
  end

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
    msg = Jason.encode!(%{src: from, dest: to, body: body})
    log(msg, "gonna send")
    IO.puts(msg)
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
      Broadcast.raw(l)
    end)
    |> Stream.run()
  end
end

spawn(&Loop.loop/0)
{:ok, _broadcast} = Broadcast.start_link()

receive do
  _ -> :ok
end

#!/usr/bin/env elixir

Mix.install([:jason])

defmodule Broadcast do
  use GenServer

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
    IO.inspect(:stderr, msg, label: "got init")
    state = %{state | node_id: msg["body"]["node_id"]}
    reply!(state.node_id, msg, %{type: "init_ok"})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "topology"}} = msg}, state) do
    IO.inspect(:stderr, msg, label: "got topology")
    state = %{state | neighbors: msg["body"]["topology"][state.node_id]}
    reply!(state.node_id, msg, %{type: "topology_ok"})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "read"}} = msg}, state) do
    IO.inspect(:stderr, msg, label: "got read")
    reply!(state.node_id, msg, %{type: "read_ok", messages: :sets.to_list(state.messages)})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "broadcast"} = body} = msg}, state) do
    IO.inspect(:stderr, msg, label: "got broadcast")
    e = msg["body"]["message"]
    state = if :sets.is_element(e, state.messages) do
      state
    else
      Enum.each(state.neighbors, fn n ->
        if n != msg["src"] do
          send!(state.node_id, n, body)
        end
      end)
      Map.update!(state, :messages, &:sets.add_element(e, &1))
    end
    reply!(state.node_id, msg, %{type: "broadcast_ok"})
    {:noreply, state}
  end
  def handle_cast({:msg, %{"body" => %{"type" => "broadcast_ok"}} = msg}, state) do
    IO.inspect(:stderr, msg, label: "got broadcast_ok")
    {:noreply, state}
  end

  def reply!(from, original_msg, body) do
    body = Map.merge(
      body,
      %{
        in_reply_to: original_msg["body"]["msg_id"],
        msg_id: next_message_id(),
      }
    )
    send!(from, original_msg["src"], body)
  end

  defp send!(from, to, body) do
    msg = Jason.encode!(%{src: from, dest: to, body: body})
    IO.inspect(:stderr, msg, label: "gonna send")
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
end

defmodule Loop do
  def loop() do
    IO.stream()
    |> Stream.each(fn l ->
      IO.inspect(:stderr, l, label: "got")
      Broadcast.raw(l)
    end)
    |> Stream.run()
  end
end

spawn(&Loop.loop/0)
{:ok, broadcast} = Broadcast.start_link()

receive do
  _ -> :ok
end

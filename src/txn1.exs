#!/usr/bin/env elixir

Mix.install([:jason, :hallux])

defmodule Txn do
  use GenServer

  alias Hallux.Seq

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
      tlog: Seq.new(),
      db: %{},
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
  def handle_cast({:msg, %{"body" => %{"type" => "txn"}} = msg}, state) do
    log(msg, "got #{msg["body"]["type"]} req")
    %{"txn" => ops} = msg["body"]
    {resp, state} = process_ops(state, ops)
    reply!(state.node_id, msg, %{type: "txn_ok", txn: resp})
    {:noreply, state}
  end

  defp process_ops(state, ops) do
    Enum.map_reduce(ops, state, fn
      ["r", k, nil], state ->
        v = get_in(state, [:db, k])
        {["r", k, v], state}

      ["w", k, v], state ->
        state = put_in(state, [:db, k], v)
        {["w", k, v], state}
    end)
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
      Txn.raw(l)
    end)
    |> Stream.run()
  end
end

spawn(&Loop.loop/0)
{:ok, _broadcast} = Txn.start_link()

receive do
  _ -> :ok
end

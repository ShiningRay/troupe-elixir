defmodule Troupe.Roster do
  @moduledoc """
  Roster（剧团花名册）：Stage（BEAM 节点）成员管理。

  成员与故障探测**完全委托 Erlang distribution**：成员 = `[node() | Node.list()]`，
  连通性由 net_kernel 心跳维护（这是 Elixir 版相对 Troupe.js 自建 gossip/ping 协议的
  最大简化红利——分布式心跳、断连检测、消息路由都是运行时原生能力）。

  成员变更（nodeup/nodedown）推送给 Playbill 重建哈希环；环漂移期由 Router 的
  转发（跳数上限 2）兜底。加入集群：`cluster: [seeds: [:"a@host", ...]]`。

  production 要素（固定端口 / advertise / 必须入团）由节点的 VM 启动参数承担
  （`--sname/--name` + cookie），框架校验"必须成功入团"与角色集合一致性。
  """

  use GenServer
  alias Troupe.Time

  def child_spec(opts) do
    troupe_name = Keyword.fetch!(opts, :troupe_name)

    %{
      id: {__MODULE__, troupe_name},
      start: {__MODULE__, :start_link, [[troupe_name: troupe_name, opts: opts]]}
    }
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :troupe_name)
    GenServer.start_link(__MODULE__, %{troupe_name: name, opts: opts}, name: Troupe.Names.via(name, :roster))
  end

  @impl true
  def init(%{troupe_name: troupe_name, opts: opts}) do
    cluster = Keyword.get(opts, :cluster) || []
    seeds = Keyword.get(cluster, :seeds, []) |> List.wrap()
    production? = Keyword.get(cluster, :mode, :dev) == :production

    :net_kernel.monitor_nodes(true, node_type: :all)

    Process.send_after(self(), :troupe_join_seeds, 0)

    {:ok,
     %{
       troupe_name: troupe_name,
       seeds: seeds,
       production?: production?,
       joined: false,
       connected: [],
       reconnect_ref: nil,
       last_change: Time.now_ms()
     }}
  end

  @impl true
  def handle_info(:troupe_join_seeds, s) do
    connected = join_seeds(s.seeds, [])

    s = %{
      s
      | connected: [node() | Node.list()],
        joined: true,
        last_change: Time.now_ms()
    }

    if s.production? and s.seeds != [] and connected == [] do
      raise "production mode: failed to join cluster via seeds #{inspect(s.seeds)}"
    end

    publish(s)
    {:noreply, s}
  end

  def handle_info({:nodeup, _node, _info}, s) do
    s = %{s | connected: [node() | Node.list()], last_change: Time.now_ms()}
    publish(s)
    {:noreply, s}
  end

  def handle_info({:nodedown, lost, _info}, s) do
    s = %{s | connected: [node() | Node.list()], last_change: Time.now_ms()}
    publish(s)

    # seed 掉线：自动重连（指数退避），保持环成员自愈
    if lost in s.seeds and s.reconnect_ref == nil do
      ref = Process.send_after(self(), :troupe_reconnect_seed, 1_000)
      {:noreply, %{s | reconnect_ref: ref}}
    else
      {:noreply, s}
    end
  end

  def handle_info(:troupe_reconnect_seed, s) do
    missing = Enum.filter(s.seeds, &(&1 not in [node() | Node.list()]))

    cond do
      missing == [] ->
        {:noreply, %{s | reconnect_ref: nil}}

      true ->
        for seed <- missing do
          Task.Supervisor.start_child(Troupe.GlobalTaskSup, fn ->
            if Node.alive?(), do: Node.connect(seed)
          end)
        end

        next = Process.send_after(self(), :troupe_reconnect_seed, 3_000)
        {:noreply, %{s | reconnect_ref: next}}
    end
  end

  @impl true
  def handle_call(:members, _from, s) do
    {:reply, [node() | Node.list()], s}
  end

  defp join_seeds([], acc), do: acc

  defp join_seeds([seed | rest], acc) do
    case connect_node(seed) do
      true -> join_seeds(rest, [seed | acc])
      false -> join_seeds(rest, acc)
    end
  end

  defp connect_node(seed) do
    task =
      Task.Supervisor.async_nolink(Troupe.GlobalTaskSup, fn ->
        if Node.alive?(), do: Node.connect(seed), else: false
      end)

    case Task.yield(task, 5_000) || Task.shutdown(task) do
      {:ok, result} -> result == true
      _ -> false
    end
  end

  defp publish(s) do
    case Troupe.Names.lookup(s.troupe_name, :playbill) do
      nil -> :ok
      playbill -> GenServer.cast(playbill, {:troupe_members, [node() | Node.list()]})
    end
  end
end

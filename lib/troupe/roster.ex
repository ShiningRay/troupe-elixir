defmodule Troupe.Roster do
  @moduledoc """
  Roster（剧团花名册）：Stage（BEAM 节点）成员管理。

  成员与故障探测**完全委托 Erlang distribution**：成员 = `[node() | Node.list()]`，
  连通性由 net_kernel 心跳维护（这是 Elixir 版相对 Troupe.js 自建 gossip/ping 协议的
  最大简化红利——分布式心跳、断连检测、消息路由都是运行时原生能力）。

  成员变更（nodeup/nodedown）推送给 Playbill 重建哈希环；环漂移期由 Router 的
  转发（跳数上限 2）兜底。加入集群：`cluster: [seeds: [:"a@host", ...]]`。

  ## 入团协商（Issue #2）

  每个节点在与其他节点建立连接后交换 **manifest**（协议版本 + namespace + 角色集合）：

  * **production**：manifest 不兼容（协议不同 / namespace 不同 / 角色集合不同 /
    对端未运行同名 Troupe）→ 明确断开该节点（fail-fast，不入环），全部 seed 被拒则
    入团失败 raise；且 seed 连不上即 raise（必须成功入团）
  * **dev**：不兼容仅记录 warning，保持实验性集群语义（混角色环可用但定位实验）

  production 要素（固定端口 / advertise）由节点的 VM 启动参数承担（`--sname/--name` + cookie）；
  "必须成功入团"与角色集合一致性由本模块校验。
  """

  use GenServer
  require Logger

  alias Troupe.Time

  # 入团 manifest 协议版本：变更即视为不兼容（未来扩展放这里）
  @protocol 1
  @handshake_timeout 5_000

  @doc "入团 manifest 协议版本。"
  def protocol, do: @protocol

  def child_spec(opts) do
    troupe_name = Keyword.fetch!(opts, :troupe_name)

    # opts 展平透传（cluster/manifest 经同一级 init 读取；嵌套包装会导致取不到）
    %{id: {__MODULE__, troupe_name}, start: {__MODULE__, :start_link, [opts]}}
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :troupe_name)
    GenServer.start_link(__MODULE__, %{troupe_name: name, opts: opts}, name: Troupe.Names.via(name, :roster))
  end

  # -- 入团协商（erpc 目标端：对端节点发起，本节点校验并回传自身 manifest） --------

  @doc """
  对端入团握手：校验远端 manifest 与本节点同名 Troupe 是否兼容。

  返回 `{:ok, local_manifest}`（兼容，远端可入环）或 `{:error, reason}`
  （`:troupe_not_running` / `{:incompatible, detail}`）。经 erpc 调用。
  """
  def handshake(troupe_name, remote_manifest) when is_atom(troupe_name) and is_map(remote_manifest) do
    case Troupe.by_name(troupe_name) do
      nil ->
        {:error, :troupe_not_running}

      troupe ->
        local = manifest_of(troupe)

        case compare(local, remote_manifest) do
          :ok -> {:ok, local}
          {:error, _} = err -> err
        end
    end
  end

  @doc "本节点 manifest：协议版本 + namespace + 角色集合。"
  def manifest_of(%Troupe{} = troupe) do
    %{
      protocol: @protocol,
      namespace: troupe.config.namespace,
      roles: troupe.repertoire.by_role |> Map.keys() |> Enum.sort()
    }
  end

  @doc "manifest 兼容性裁决：协议、namespace、角色集合任一不同即不兼容。"
  def compare(local, remote) do
    cond do
      local.protocol != remote.protocol ->
        {:error, {:incompatible, {:protocol, local.protocol, remote.protocol}}}

      local.namespace != remote.namespace ->
        {:error, {:incompatible, {:namespace, local.namespace, remote.namespace}}}

      MapSet.new(local.roles) != MapSet.new(Map.get(remote, :roles, [])) ->
        {:error, {:incompatible, {:roles, Enum.sort(local.roles), Enum.sort(Map.get(remote, :roles, []))}}}

      true ->
        :ok
    end
  end

  # -- GenServer -------------------------------------------------------------

  @impl true
  def init(%{troupe_name: troupe_name, opts: opts}) do
    cluster = Keyword.get(opts, :cluster) || []
    seeds = Keyword.get(cluster, :seeds, []) |> List.wrap()
    production? = Keyword.get(cluster, :mode, :dev) == :production
    manifest = Keyword.fetch!(opts, :manifest)

    :net_kernel.monitor_nodes(true, node_type: :all)

    Process.send_after(self(), :troupe_join_seeds, 0)

    {:ok,
     %{
       troupe_name: troupe_name,
       seeds: seeds,
       production?: production?,
       manifest: manifest,
       joined: false,
       rejected: [],
       connected: [],
       reconnect_ref: nil,
       last_change: Time.now_ms()
     }}
  end

  @impl true
  def handle_info(:troupe_join_seeds, s) do
    {connected, rejected} = join_seeds(s.seeds, s, [], [])

    s = %{
      s
      | connected: [node() | Node.list()],
        joined: true,
        rejected: rejected,
        last_change: Time.now_ms()
    }

    if s.production? and s.seeds != [] and connected == [] do
      raise "production mode: failed to join cluster via seeds #{inspect(s.seeds)} (rejected: #{inspect(rejected)})"
    end

    publish(s)
    {:noreply, s}
  end

  def handle_info({:nodeup, new_node, _info}, s) do
    s = %{s | connected: [node() | Node.list()], last_change: Time.now_ms()}

    # 对端连入：反向握手校验（两端各自发起，幂等纯读比较）
    s =
      if new_node != node() do
        case negotiate(new_node, s) do
          {:ok, _} ->
            s

          {:error, reason} ->
            Logger.warning(
              "troupe: node #{inspect(new_node)} failed roster negotiation (#{inspect(reason)})" <>
                if(s.production?, do: " — disconnecting", else: "")
            )

            # dev 同样记录（观测一致）；production 额外断连 fail-fast
            s2 = %{s | rejected: Keyword.update(s.rejected, new_node, [reason], &[reason | &1])}

            if s.production?, do: Node.disconnect(new_node)

            s2
        end
      else
        s
      end

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

  def handle_call(:negotiation_state, _from, s),
    do: {:reply, %{rejected: s.rejected, production?: s.production?}, s}

  # -- 协商 ------------------------------------------------------------------

  # 连接 seeds 并逐一握手；返回 {成功入环节点, 被拒节点}
  defp join_seeds([], _s, connected, rejected), do: {connected, rejected}

  defp join_seeds([seed | rest], s, connected, rejected) do
    case connect_node(seed) do
      true ->
        case negotiate(seed, s) do
          {:ok, _remote} ->
            join_seeds(rest, s, [seed | connected], rejected)

          {:error, reason} ->
            Logger.warning(
              "troupe: seed #{inspect(seed)} rejected in roster negotiation (#{inspect(reason)})" <>
                if(s.production?, do: " — disconnecting", else: "")
            )

            if s.production?, do: Node.disconnect(seed)

            join_seeds(rest, s, connected, Keyword.update(rejected, seed, [reason], &[reason | &1]))
        end

      false ->
        join_seeds(rest, s, connected, Keyword.update(rejected, seed, [:unreachable], &[:unreachable | &1]))
    end
  end

  # 与对端节点协商：erpc handshake + 兼容性裁决
  defp negotiate(peer, s) do
    try do
      case :erpc.call(peer, Troupe.Roster, :handshake, [s.troupe_name, s.manifest], @handshake_timeout) do
        {:ok, _remote_manifest} -> {:ok, :compatible}
        {:error, reason} -> {:error, reason}
      end
    catch
      :exit, {:erpc, reason} -> {:error, {:erpc, reason}}
      :error, {:erpc, reason} -> {:error, {:erpc, reason}}
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

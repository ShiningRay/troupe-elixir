defmodule Troupe.Director do
  @moduledoc """
  Director（导演）：系统级内省角色。CLI / 观测工具都是它的瘦客户端；
  集群级查询向其他 Stage 的 Director 扇出聚合（经 erlang distribution，cookie 即鉴权边界——
  受控网络 + cookie 是集群部署前提），客户端永远只连一个点。

  管理操作同样受 Actor 调度约束（经 CallBoard），不旁路串行规则。
  """

  use GenServer
  alias Troupe.{Agent, Names, Time}

  def child_spec(opts) do
    troupe_name = Keyword.fetch!(opts, :troupe_name)
    %{id: {__MODULE__, troupe_name}, start: {__MODULE__, :start_link, [[troupe_name: troupe_name]]}}
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :troupe_name)
    GenServer.start_link(__MODULE__, name, name: Names.via(name, :director))
  end

  @impl true
  def init(troupe_name), do: {:ok, troupe_name}

  # ==========================================================================
  # 本地命令（也可被远端 erpc 调用）
  # ==========================================================================

  @doc false
  def api(troupe_name, cmd, args \\ []) do
    case cmd do
      :status ->
        status(troupe_name)

      :roster ->
        roster(troupe_name)

      :actors ->
        actors(troupe_name, args)

      :inspect ->
        inspect(troupe_name, Enum.fetch!(args, 0), Enum.fetch!(args, 1))

      :off ->
        off(troupe_name, Enum.fetch!(args, 0), Enum.fetch!(args, 1))

      :invoke ->
        invoke(
          troupe_name,
          Enum.fetch!(args, 0),
          Enum.fetch!(args, 1),
          Enum.fetch!(args, 2),
          Enum.fetch!(args, 3)
        )

      :internal_call ->
        internal_call(
          troupe_name,
          Enum.fetch!(args, 0),
          Enum.fetch!(args, 1),
          Enum.fetch!(args, 2),
          Enum.fetch!(args, 3)
        )

      :cues ->
        cues(troupe_name)

      :ring ->
        ring(troupe_name)

      :methods ->
        methods(troupe_name, Enum.fetch!(args, 0))

      :shutdown_local ->
        shutdown_local(troupe_name)

      other ->
        {:error, {:unknown_command, other}}
    end
  end

  @doc "集群总览。"
  def status(troupe_name) do
    nodes = troupe_cluster(troupe_name)

    stage_stats =
      for n <- nodes do
        case rpc(n, troupe_name, :actors, [[]]) do
          {:ok, actors} -> %{node: n, actors: length(actors), reachable: true}
          {:error, _} -> %{node: n, actors: 0, reachable: false}
        end
      end

    %{
      troupe: troupe_name,
      node: node(),
      stages: length(nodes),
      on_stage: Enum.sum(for(s <- stage_stats, s.reachable, do: s.actors)),
      stages_detail: stage_stats,
      metrics: Troupe.Metrics.snapshot(troupe_name)
    }
  end

  @doc "Stage 列表。"
  def roster(troupe_name) do
    for n <- troupe_cluster(troupe_name) do
      base = %{node: n, self?: n == node()}

      case rpc(n, troupe_name, :actors, [[]]) do
        {:ok, actors} -> Map.merge(base, %{reachable: true, actors: length(actors)})
        {:error, reason} -> Map.merge(base, %{reachable: false, error: inspect(reason)})
      end
    end
  end

  @doc "在场 Actor 列表（本节点；集群聚合用 cluster_actors/2）。"
  def actors(troupe_name, filter) do
    troupe = Troupe.by_name!(troupe_name)

    for {{role_id, stage_name}, pid} <- Names.local_actors(troupe_name),
        match_role?(filter, role_id) do
      state = safe_state(pid)

      Map.merge(state || %{role_id: role_id, stage_name: stage_name, lifecycle: :gone}, %{node: node()})
      |> Map.put(:troupe, troupe.name)
    end
  end

  @doc "集群聚合的在场 Actor 列表；不可达/无此 troupe 的节点跳过（不毁整个聚合视图）。"
  def cluster_actors(troupe_name, filter \\ []) do
    for n <- troupe_cluster(troupe_name),
        {:ok, list} <- [rpc(n, troupe_name, :actors, [filter])],
        a <- list do
      a
    end
  end

  @doc "Actor 详情（含 Props 快照）——路由到归属 Stage。"
  def inspect(troupe_name, role_id, stage_name) do
    troupe = Troupe.by_name!(troupe_name)

    case Names.find_actor(troupe_name, role_id, stage_name) do
      nil ->
        pid = maybe_remote_state(troupe, role_id, stage_name)
        pid

      pid ->
        safe_state(pid) |> Map.put(:node, node())
    end
  end

  @doc "强制下场（路由到归属 Stage），验证生命周期。"
  def off(troupe_name, role_id, stage_name) do
    troupe = Troupe.by_name!(troupe_name)

    case Names.find_actor(troupe_name, role_id, stage_name) do
      pid when is_pid(pid) ->
        GenServer.call(pid, :troupe_off, 15_000)

      nil ->
        key = {troupe.config.namespace, role_id, stage_name}

        case Troupe.Playbill.owner(troupe.playbill, key) do
          n when n == node() or n == nil -> {:error, :not_on_stage}
          n -> rpc(n, troupe_name, :off, [role_id, stage_name])
        end
    end
  end

  @doc "管理直调（走普通 RPC 白名单）。"
  def invoke(troupe_name, role_id, stage_name, method, args) do
    troupe = Troupe.by_name!(troupe_name)
    agent = Agent.build(troupe_name, role_id, stage_name)
    Troupe.call(troupe, agent, method, args)
  end

  @doc false
  def internal_call(troupe_name, role_id, stage_name, method, args) do
    troupe = Troupe.by_name!(troupe_name)
    agent = Agent.build(troupe_name, role_id, stage_name)
    Troupe.internal_call(troupe, agent, method, args)
  end

  @doc "全部持久提醒。"
  def cues(troupe_name) do
    troupe = Troupe.by_name!(troupe_name)

    if troupe.cue_store do
      for rec <- Troupe.CueStore.list(troupe.cue_store) do
        %{
          role_id: elem(rec.key, 1),
          stage_name: elem(rec.key, 2),
          name: rec.name,
          spec: rec.spec,
          next_due: rec.next_due,
          due_in_ms: rec.next_due - Time.now_ms()
        }
      end
    else
      []
    end
  end

  @doc "哈希环视图与均衡度。"
  def ring(troupe_name) do
    troupe = Troupe.by_name!(troupe_name)
    Troupe.Playbill.ring_info(troupe.playbill)
  end

  @doc "角色的 RPC 方法面（方法名与参数个数）。"
  def methods(troupe_name, role_id) do
    troupe = Troupe.by_name!(troupe_name)

    case troupe.repertoire.by_role[role_id] do
      %{module: m} ->
        m.module_info(:functions)
        |> Enum.filter(fn {f, a} -> f in methods_of_module(m) and a >= 1 end)

      nil ->
        {:error, :unknown_role}
    end
  end

  defp methods_of_module(m) do
    Troupe.Repertoire.surface(m) |> MapSet.to_list() |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
  end

  # ==========================================================================
  # 集群扇出
  # ==========================================================================

  @doc false
  def shutdown_local(troupe_name) do
    for {_k, pid} <- Names.local_actors(troupe_name) do
      try do
        GenServer.call(pid, :troupe_shutdown, 15_000)
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  @doc false
  def shutdown_cluster(troupe_name) do
    for n <- troupe_cluster(troupe_name) do
      try do
        if n == node() do
          shutdown_local(troupe_name)
        else
          :erpc.call(n, Troupe.Director, :api, [troupe_name, :shutdown_local])
        end
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  defp troupe_cluster(troupe_name) do
    troupe = Troupe.by_name!(troupe_name)

    if troupe.config.cluster do
      [node() | Node.list()]
    else
      [node()]
    end
  end

  defp rpc(node, troupe_name, cmd, args) do
    if node == node() do
      {:ok, api(troupe_name, cmd, args)}
    else
      try do
        {:ok, :erpc.call(node, Troupe.Director, :api, [troupe_name, cmd, args])}
      catch
        :exit, {:erpc, reason} -> {:error, reason}
        :error, {:erpc, reason} -> {:error, reason}
        # erpc 会把远端 raise 的异常在本地重放（ErlangError 等）——聚合视图按不可达处理
        kind, reason -> {:error, {kind, reason}}
      end
    end
  end

  defp maybe_remote_state(troupe, role_id, stage_name) do
    key = {troupe.config.namespace, role_id, stage_name}

    case Troupe.Playbill.owner(troupe.playbill, key) do
      n when n == node() or n == nil -> nil
      n -> rpc(n, troupe.name, :inspect, [role_id, stage_name]) |> elem(1)
    end
  end

  defp safe_state(pid) do
    try do
      GenServer.call(pid, :troupe_state, 5_000)
    catch
      :exit, _ -> nil
    end
  end

  defp match_role?(filter, role_id) do
    case Keyword.fetch(filter, :role) do
      {:ok, r} -> r == role_id
      :error -> true
    end
  end
end

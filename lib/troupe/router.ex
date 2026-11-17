defmodule Troupe.Router do
  @moduledoc """
  路由与位置透明：Call → Playbill 判定归属节点 → 本地执行或经 erlang distribution
  跨节点转发（漂移期转发跳数上限 2，防环）。

  跨节点传输使用 BEAM 原生 term 序列化——**本地、远程、Rehearsal 三种模式天然同一
  数据边界**（消息即拷贝），Troupe.js 用 Codec 手工保证的语义在 Elixir 是运行时馈赠。
  """

  alias Troupe.{Agent, Casting, Time}

  @max_hops 2

  # -- 入口（Troupe.call 调用） -----------------------------------------------

  @doc false
  def dispatch(troupe, %Agent{} = agent, fun, args, deadline_at, hops \\ 0)
      when is_map(troupe) do
    key = Agent.key(agent, troupe.config.namespace)
    owner = Troupe.Playbill.owner(troupe.playbill, key)

    cond do
      # :nonode@nohost = 环组建于 distribution 启动之前（单机 troupe 在 --name 之前 form）
      owner == node() or owner == nil or owner == :nonode@nohost ->
        local(troupe, agent, fun, args, deadline_at)

      hops >= @max_hops ->
        {:error,
         Troupe.CallRejectedError.exception(
           reason: :drift,
           role_id: agent.role_id,
           stage_name: agent.stage_name,
           detail: "forward hop limit (#{hops}) exceeded — ring view may be stale",
           retryable: true
         )}

      true ->
        remote(owner, troupe.name, agent, fun, args, deadline_at, hops + 1)
    end
  end

  # 远程入口（erpc 目标）：在归属节点上本地执行
  @doc false
  def enter(troupe_name, role_id, stage_name, fun, args, deadline_at, hops) do
    troupe = Troupe.by_name!(troupe_name)
    agent = Agent.build(troupe_name, role_id, stage_name)
    dispatch(troupe, agent, fun, args, deadline_at, hops)
  end

  defp local(troupe, agent, fun, args, deadline_at) do
    case Casting.ensure_active(troupe, agent) do
      {:ok, pid} ->
        timeout =
          if deadline_at,
            # 小宽限：让排队过期的显式拒绝能赶在调用方放弃之前送达
            do: max(deadline_at - Time.now_ms(), 0) + 500,
            else: 60_000

        try do
          GenServer.call(pid, {:troupe_call, fun, args, deadline_at, caller_ctx(), false}, timeout)
        catch
          :exit, {:timeout, {GenServer, :call, [_pid, _, _]}} ->
            {:error,
             Troupe.UnknownOutcomeError.exception(
               role_id: agent.role_id,
               stage_name: agent.stage_name,
               method: fun
             )}

          :exit, {:noproc, _} ->
            # 进程在下场边缘：重激活一次
            with {:ok, pid2} <- Casting.ensure_active(troupe, agent) do
              GenServer.call(pid2, {:troupe_call, fun, args, deadline_at, caller_ctx(), false}, timeout)
            end
        end

      {:error, cause} ->
        {:error,
         Troupe.ActivationError.exception(
           role_id: agent.role_id,
           stage_name: agent.stage_name,
           cause: cause
         )}
    end
  end

  defp remote(node, troupe_name, agent, fun, args, deadline_at, hops) do
    try do
      :erpc.call(node, Troupe.Router, :enter, [
        troupe_name,
        agent.role_id,
        agent.stage_name,
        fun,
        args,
        deadline_at,
        hops
      ])
    catch
      # 连接断开/节点下线/目标端异常：结果未知（勿假定失败）
      :exit, {:erpc, reason} ->
        {:error, unknown(agent, fun, reason)}

      :error, {:erpc, reason} ->
        {:error, unknown(agent, fun, reason)}

      kind, reason ->
        # erpc 把远端 raise 的异常在本地重放——对调用方同样是"结果未知"
        {:error, unknown(agent, fun, {kind, reason})}
    end
  end

  defp unknown(agent, fun, reason) do
    Troupe.UnknownOutcomeError.exception(
      role_id: agent.role_id,
      stage_name: agent.stage_name,
      method: fun,
      message: "call outcome unknown (#{inspect(reason)}) — do not assume failure"
    )
  end

  defp caller_ctx do
    case Process.get(:troupe_ctx) do
      nil -> :client
      ctx -> ctx
    end
  end
end

defmodule Troupe.Casting do
  @moduledoc """
  Casting（选角）：虚拟激活的单飞入口。

  Registry 命中（已激活）→ 直取 pid；未激活 → 经本 GenServer 串行启动 ActorServer
  （via 注册名互斥 + 单进程串行 = 激活 single-flight：并发首调只登台一次）。
  所有权检查在 ActorServer 激活内部执行（启用 ownership 时）。

  放置策略一期仅 HashCasting（由 Playbill 直接决定）——与 Troupe.js DESIGN §6.4 一致。
  """

  use GenServer

  def child_spec(opts) do
    troupe_name = Keyword.fetch!(opts, :troupe_name)
    %{id: {__MODULE__, troupe_name}, start: {__MODULE__, :start_link, [[troupe_name: troupe_name]]}}
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :troupe_name)
    GenServer.start_link(__MODULE__, name, name: Troupe.Names.via(name, :casting))
  end

  @doc "确保 Actor 在场；并发首调共享同一次激活（single-flight）。"
  @spec ensure_active(map(), Troupe.Agent.t()) :: {:ok, pid()} | {:error, term()}
  def ensure_active(troupe, agent) do
    case Troupe.Names.find_actor(troupe.name, agent.role_id, agent.stage_name) do
      pid when is_pid(pid) ->
        {:ok, pid}

      nil ->
        casting = Troupe.Names.lookup!(troupe.name, :casting)
        GenServer.call(casting, {:ensure, troupe, agent}, 30_000)
    end
  end

  @impl true
  def init(troupe_name), do: {:ok, troupe_name}

  @impl true
  def handle_call({:ensure, troupe, agent}, _from, s) do
    case Troupe.Names.find_actor(troupe.name, agent.role_id, agent.stage_name) do
      pid when is_pid(pid) ->
        {:reply, {:ok, pid}, s}

      nil ->
        arg = %{
          troupe: troupe,
          module: module_of(troupe, agent),
          role_id: agent.role_id,
          stage_name: agent.stage_name
        }

        spec = %{
          id: {agent.role_id, agent.stage_name},
          start:
            {Troupe.ActorServer, :start_link,
             [
               arg,
               [name: Troupe.Names.actor_via(troupe.name, agent.role_id, agent.stage_name)]
             ]},
          restart: :temporary,
          significant: false
        }

        activator = Troupe.Names.lookup!(troupe.name, :activator)

        case DynamicSupervisor.start_child(activator, spec) do
          {:ok, pid} -> {:reply, {:ok, pid}, s}
          {:error, {:already_started, pid}} -> {:reply, {:ok, pid}, s}
          {:error, {:already_started, pid, _meta}} -> {:reply, {:ok, pid}, s}
          {:error, reason} -> {:reply, {:error, reason}, s}
        end
    end
  end

  defp module_of(troupe, agent) do
    case troupe.repertoire.by_role[agent.role_id] do
      %{module: module} ->
        module

      nil ->
        raise Troupe.UnknownRoleError,
          module_or_role: agent.role_id,
          repertoire: Map.keys(troupe.repertoire.by_role)
    end
  end
end

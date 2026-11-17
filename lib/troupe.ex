defmodule Troupe do
  @moduledoc """
  Troupe（剧团）——Elixir 版 Virtual Actor 框架（Troupe.js 的 BEAM 移植）。

  ## 核心语义

  * **永远存在**：Actor 无需显式创建，用 `(role_id, stage_name)` 全局寻址
  * **按需激活（OnStage）**：第一条 Call 到达时由运行时在集群某 Stage 激活
  * **自动钝化（OffStage）**：空闲超过 Intermission 自动下场，Props 归还 PropStore
  * **位置透明**：调用方持有 Agent，不关心目标在哪个 Stage（哈希环放置）
  * **Turn-based 并发**：默认严格串行（一次一条 Call，天然无锁）；
    `read_only`/`improv` 标注方法交错执行
  * **故障即重激活**：Stage 死亡后，下一条 Call 让 Actor 在健康 Stage 重新登台

  ## 快速开始

  ```elixir
  defmodule MyApp.CartActor do
    use Troupe.Actor, initial_props: fn -> %{items: []} end

    def add(props, item) do
      props = Map.update!(props, :items, &[item | &1])
      {:save, :ok, props}
    end

    def peek(props), do: {:reply, length(props.items)}
  end

  {:ok, troupe} = Troupe.form(actors: [MyApp.CartActor])
  cart = Troupe.cast(troupe, MyApp.CartActor, "user-123")
  :ok = Troupe.call!(cart, :add, ["apple"])
  1 = Troupe.call!(cart, :peek)
  ```
  """

  alias Troupe.{Agent, Config, Names, Repertoire, Time}

  defstruct [:name, :root, :config, :repertoire, :store, :cue_store, :ownership, :playbill, :show]

  @type t :: %__MODULE__{}

  @default_name :troupe

  # ==========================================================================
  # 组团 / 停机
  # ==========================================================================

  @doc """
  组团（启动一个 Troupe 运行时）。零配置：`Troupe.form(actors: [MyActor])`。

  主要选项（显式传参 > 环境变量 > 约定默认值）：

  | 选项 | 默认 | 说明 |
  |---|---|---|
  | `actors` | 必填 | Actor 模块列表（显式注册，fail-fast 校验） |
  | `name` | `:troupe`（被占自动加后缀） | Troupe 逻辑名（跨节点一致） |
  | `store` | `:dets` | `:memory` / `:dets` / `{module, ctx}` |
  | `namespace` | `"default"` | 存储键命名空间（`TROUPE_NAMESPACE`） |
  | `intermission` | `"5m"` | 空闲下场时长 |
  | `default_timeout` | `"30s"` | Call 超时（`TROUPE_TIMEOUT`） |
  | `queue_depth` | 1024 | 每 Actor CallBoard 深度上限 |
  | `drain_policy` | `:wait` | 排空期间新请求 `:wait` / `:reject` |
  | `cues` | false | `true` 启用持久提醒（CueStore + 扫描器）；`[tick: ms, persist: :dets]` |
  | `show` | false | `true` 启用 Show（`:pg` pub/sub） |
  | `ownership` | 未启用 | `true` / `[lease_ms: 5000]`（多节点生产建议启用） |
  | `cluster` | 无 | `[seeds: [:"a@host"], mode: :dev | :production]` |
  | `props_dir` | `".troupe"` | DETS 目录（`TROUPE_PROPS_DIR`） |
  | `shutdown_grace` | `"10s"` | 优雅停机宽限 |
  """
  @spec form(keyword()) :: {:ok, t()}
  def form(opts \\ []) do
    config = Config.build(opts)

    repertoire =
      opts |> Keyword.fetch!(opts_actors_key(opts)) |> Repertoire.build() |> tap(&Repertoire.validate!/1)

    name = pick_name(Keyword.get(opts, :name))
    cues? = Keyword.get(opts, :cues, false) != false
    show? = Keyword.get(opts, :show, false) != false

    production_cluster? = cluster_mode(config.cluster) == :production
    store_mod = Troupe.PropStore.module_of(config.store)
    {shared_own?, own_mod} = ownership_child(config)
    cue_spec = if cues?, do: cue_spec(opts, config, name, store_mod), else: []

    if production_cluster? do
      # 能力门禁（Issue #2/#3）：production 集群拒绝无法提供节点级担保的组合
      unless Troupe.PropStore.cross_node_safe?(store_mod) do
        raise ArgumentError,
              "production cluster mode requires a cross-node safe store " <>
                "(e.g. {Troupe.PropStore.Postgres, [url: ...]}), got: #{inspect(config.store)}"
      end

      if config.ownership == nil do
        raise ArgumentError,
              "production cluster mode requires ownership (single-activation fencing), " <>
                "enable with ownership: true"
      end

      unless Troupe.Ownership.cross_node_safe?(own_mod) do
        raise ArgumentError,
              "production cluster mode requires a cross-node safe ownership provider, " <>
                "got: #{inspect(own_mod)} — use a shared store provider (Postgres)"
      end
    end

    manifest = %{
      protocol: Troupe.Roster.protocol(),
      namespace: config.namespace,
      roles: repertoire.by_role |> Map.keys() |> Enum.sort()
    }

    children =
      ([
         Troupe.PropStore.child_spec(config.store, [props_dir: config.props_dir], name),
         Troupe.Activator.child_spec(troupe_name: name),
         {Task.Supervisor, name: Troupe.Names.via(name, :task_sup)},
         Troupe.Casting.child_spec(troupe_name: name)
       ] ++
         cue_spec ++
         maybe_child(show?, fn -> [Troupe.Show.child_spec(troupe_name: name)] end) ++
         maybe_child(config.ownership != nil and not shared_own?, fn ->
           [Troupe.Ownership.Memory.child_spec(troupe_name: name)]
         end) ++
         maybe_child(config.cluster != nil, fn ->
           [Troupe.Roster.child_spec(troupe_name: name, cluster: config.cluster, manifest: manifest)]
         end) ++
         [Troupe.Playbill.child_spec(troupe_name: name, cluster: config.cluster)] ++
         [Troupe.Director.child_spec(troupe_name: name)])
      |> List.flatten()
      |> Enum.reject(&is_nil/1)

    {:ok, root} =
      Supervisor.start_link(children, strategy: :one_for_one, name: Names.via(name, :root))

    # 运行时独立于组建者进程存活；停机走 Troupe.shutdown（显式、有序）
    Process.unlink(root)

    store = Troupe.PropStore.resolve(config.store, name)

    {own_module, own_ctx} =
      if config.ownership do
        if shared_own? do
          {own_mod, store_mod.ownership_ctx(store)}
        else
          {Troupe.Ownership.Memory, Names.lookup!(name, :ownership)}
        end
      else
        {nil, nil}
      end

    troupe = %__MODULE__{
      name: name,
      root: root,
      config: config,
      repertoire: repertoire,
      store: store,
      cue_store: (cues? && cue_ctx(opts, config, store, name)) || nil,
      ownership:
        (config.ownership &&
           %{
             module: own_module,
             ctx: own_ctx,
             lease_ms: Keyword.get(config.ownership, :lease_ms, 5_000)
           }) || nil,
      playbill: Names.lookup!(name, :playbill),
      show: (show? && name) || nil
    }

    :persistent_term.put({__MODULE__, :running, name}, troupe)

    if :persistent_term.get({__MODULE__, :default}, nil) == nil do
      :persistent_term.put({__MODULE__, :default}, name)
    end

    {:ok, troupe}
  end

  defp opts_actors_key(opts) do
    if Keyword.has_key?(opts, :actors),
      do: :actors,
      else: raise(ArgumentError, "Troupe.form requires actors:")
  end

  defp cluster_mode(nil), do: nil
  defp cluster_mode(cluster), do: Keyword.get(cluster, :mode, :dev)

  # 所有权 Provider 解析：共享存储模块导出 ownership_module/0 时复用同一连接
  # （同一事务/行锁模型，fencing 与 Props 写一致）；否则回退节点级 Memory。
  defp ownership_child(config) do
    store_mod = Troupe.PropStore.module_of(config.store)
    Code.ensure_loaded?(store_mod)
    shared? = config.ownership != nil and function_exported?(store_mod, :ownership_module, 0)
    {shared?, (shared? && store_mod.ownership_module()) || Troupe.Ownership.Memory}
  end

  # cues: true | [persist: :memory | :dets] | [provider: :store] | [provider: {module, opts}]
  defp cue_opts(opts) do
    case Keyword.get(opts, :cues) do
      kw when is_list(kw) -> kw
      _ -> []
    end
  end

  # 监督树子进程：本地模式含 CueStore GenServer；共享模式连接由 store / 自定义驱动承载
  defp cue_spec(opts, config, name, store_mod) do
    case cue_opts(opts)[:provider] do
      nil ->
        [
          Troupe.CueStore.child_spec(troupe_name: name, mode: Keyword.get(cue_opts(opts), :persist, :memory)),
          Troupe.CueRunner.child_spec(troupe_name: name, tick_ms: config.cue_tick_ms)
        ]

      :store ->
        unless function_exported?(store_mod, :cue_ctx, 1) do
          raise ArgumentError,
                "cues provider :store requires a shared store with cue support " <>
                  "(Postgres), got: #{inspect(config.store)}"
        end

        [Troupe.CueRunner.child_spec(troupe_name: name, tick_ms: config.cue_tick_ms)]

      {mod, cue_opts} ->
        base = if function_exported?(mod, :child_spec, 2), do: [mod.child_spec(cue_opts, name)], else: []
        base ++ [Troupe.CueRunner.child_spec(troupe_name: name, tick_ms: config.cue_tick_ms)]
    end
  end

  defp cue_ctx(opts, config, store, name) do
    case cue_opts(opts)[:provider] do
      nil -> %{server: Names.lookup!(name, :cue_store)}
      :store -> Troupe.PropStore.module_of(config.store).cue_ctx(store)
      {mod, cue_opts} -> mod.init(cue_opts)
    end
  end

  defp maybe_child(true, fun), do: fun.()
  defp maybe_child(false, _fun), do: []

  defp pick_name(nil) do
    # 原子计数（全局 ETS）：并发 form 多个 troupe 不会撞名
    n = :ets.update_counter(:troupe_globals, :troupe_count, {2, 1}, {:troupe_count, 0})

    if n == 1 do
      @default_name
    else
      :"#{@default_name}_#{n}"
    end
  end

  defp pick_name(name) do
    if :persistent_term.get({__MODULE__, :running, name}, nil) do
      raise ArgumentError, "troupe #{inspect(name)} is already running"
    end

    name
  end

  @doc "按逻辑名取运行中的 Troupe（Router/远程入口用）。"
  @spec by_name(atom()) :: t() | nil
  def by_name(name), do: :persistent_term.get({__MODULE__, :running, name}, nil)

  @doc "按逻辑名取运行中的 Troupe；未运行 raise。"
  @spec by_name!(atom()) :: t()
  def by_name!(name) do
    by_name(name) || raise("troupe #{inspect(name)} is not running on #{inspect(node())}")
  end

  @doc "默认 Troupe（第一个 form 的实例）。"
  @spec default() :: t() | nil
  def default do
    case :persistent_term.get({__MODULE__, :default}, nil) do
      nil -> nil
      name -> by_name(name)
    end
  end

  @doc """
  优雅停机：全员 offStage+saveProps → 停止运行时。
  存量 Call settle 或超时；宽限默认 10s。
  """
  @spec shutdown(t(), keyword()) :: :ok
  def shutdown(troupe, opts \\ []) do
    grace = opts[:grace] || troupe.config.shutdown_grace_ms
    Troupe.Director.shutdown_cluster(troupe.name)

    try do
      Supervisor.stop(troupe.root, :shutdown, grace)
    rescue
      _ -> :ok
    catch
      :exit, _ -> :ok
    end

    :persistent_term.erase({__MODULE__, :running, troupe.name})

    if :persistent_term.get({__MODULE__, :default}, nil) == troupe.name do
      :persistent_term.erase({__MODULE__, :default})
    end

    :ok
  end

  @doc "销毁运行时且**不**保存（模拟 kill -9，仅测试）：在场 Actor 直接击杀。"
  @spec destroy(t()) :: :ok
  def destroy(troupe) do
    for pid <- Troupe.Activator.which_children(troupe.name) do
      Process.exit(pid, :kill)
    end

    :ok
  end

  # ==========================================================================
  # 选角 / 调用
  # ==========================================================================

  @doc """
  选角：`(module | role_id, stage_name)` → Agent。传模块引用是默认路径（零字符串）；
  字符串 role_id 为逃生舱（契约先行场景）。未注册角色 fail-fast（列出当前 Repertoire）。
  """
  @spec cast(t(), module() | String.t(), term()) :: Agent.t()
  def cast(troupe, module_or_role, stage_name) do
    entry = Repertoire.fetch!(troupe.repertoire, module_or_role)
    Agent.build(troupe.name, entry.role_id, stage_name)
  end

  @doc """
  调用 Actor 方法（发一条 Call）。返回 `{:ok, reply}` / `{:error, exception}`。

  * deadline：`opts[:deadline]`（毫秒）> 角色 `timeouts:` 声明 > 全局默认（30s）
  * **排队过期**：deadline 已过的排队 Call 被拒绝（`Troupe.CallTimeoutError` phase: :queued，
    零副作用，可安全重试）
  * **执行中超时/断连**：`Troupe.UnknownOutcomeError` ——**结果未知，勿假定失败**，
    按幂等键重试
  """
  @spec call(Agent.t() | t(), atom(), [term()], keyword()) :: {:ok, term()} | {:error, Exception.t()}
  def call(agent_or_troupe, fun, args, opts \\ [])

  def call(%Agent{} = agent, fun, args, opts) when is_atom(fun) and is_list(args) do
    troupe = by_name!(agent.troupe_name)
    call(troupe, agent, fun, args, opts)
  end

  def call(%__MODULE__{} = troupe, %Agent{} = agent, fun, args) when is_atom(fun) and is_list(args) do
    call(troupe, agent, fun, args, [])
  end

  def call(%__MODULE__{} = troupe, %Agent{} = agent, fun, args, opts) do
    deadline_at =
      case opts[:deadline] do
        nil -> nil
        ms -> Time.now_ms() + Time.parse(ms)
      end

    Troupe.Router.dispatch(troupe, agent, fun, args, deadline_at)
  end

  @doc "`call/4` 的 raise 版本：成功返回结果，失败 raise 异常。"
  @spec call!(Agent.t(), atom(), [term()], keyword()) :: term()
  def call!(agent_or_troupe, fun, args, opts \\ []) do
    case call(agent_or_troupe, fun, args, opts) do
      {:ok, reply} -> reply
      {:error, e} -> raise e
    end
  end

  @doc false
  # 框架内部调用（提醒投递等）：绕过 RPC 白名单，但同样经调度串行
  def internal_call(troupe, %Agent{} = agent, fun, args) do
    key = Agent.key(agent, troupe.config.namespace)

    case Troupe.Playbill.owner(troupe.playbill, key) do
      n when n == node() or n == nil ->
        with {:ok, pid} <- Troupe.Casting.ensure_active(troupe, agent) do
          GenServer.call(pid, {:troupe_call, fun, args, nil, :internal, true}, 60_000)
        end

      n ->
        :erpc.call(n, Troupe.Director, :api, [
          troupe.name,
          :internal_call,
          [agent.role_id, agent.stage_name, fun, args]
        ])
    end
  end

  # ==========================================================================
  # 生命周期管理
  # ==========================================================================

  @doc "强制指定 Actor 下场（保存 Props），验证生命周期。"
  @spec off(t() | Agent.t()) :: :ok | {:error, term()}
  def off(%Agent{} = agent), do: off(by_name!(agent.troupe_name), agent)

  def off(%__MODULE__{} = troupe, %Agent{} = agent),
    do: Troupe.Director.off(troupe.name, agent.role_id, agent.stage_name)

  @doc """
  请求协作取消（advisory）：置位该 Actor 的取消标志，在飞/下一 Turn 内
  `Troupe.Actor.cancel_requested?/0` 返回 true；Turn settle 后标志自动清除。

  不可强行终止——strict Turn 内联执行，方法自行观察并决定收束（可提前 settle
  提交部分状态）。外部副作用不受保护（同 ownership 边界警示）。

  置位直接写节点标志（不经 ActorServer 信箱——在飞 strict Turn 会阻塞 GenServer
  调用，正是要取消的对象）；不在场的 Actor 置位对其下一 Turn 生效。
  """
  @spec cancel(Agent.t()) :: :ok
  def cancel(%Agent{} = agent) do
    troupe = by_name!(agent.troupe_name)

    :persistent_term.put(
      {Troupe.ActorServer, :cancel, troupe.name, agent.role_id, agent.stage_name},
      true
    )

    :ok
  end

  @doc "全体在场 Actor 下场（保存 Props）——测试清理用。"
  @spec drain_all(t(), keyword()) :: :ok
  def drain_all(troupe, opts \\ []) do
    timeout = opts[:timeout] || 5_000
    deadline = Time.now_ms() + timeout

    for {_k, pid} <- Troupe.Activator.which_children(troupe.name) do
      try do
        GenServer.call(pid, :troupe_off, 15_000)
      catch
        :exit, _ -> :ok
      end
    end

    wait_actors_gone(troupe, deadline)
  end

  defp wait_actors_gone(troupe, deadline) do
    if Time.now_ms() < deadline and Troupe.Activator.which_children(troupe.name) != [] do
      Process.sleep(20)
      wait_actors_gone(troupe, deadline)
    else
      :ok
    end
  end

  # ==========================================================================
  # Show / Audience
  # ==========================================================================

  @doc "发布事件到话题（Show 需启用）。尽力扇出，返回投递成员数。"
  @spec publish(t(), term(), term()) :: non_neg_integer()
  def publish(troupe, topic, event) do
    Troupe.Show.publish(troupe, topic, event)
  end

  @doc "Actor 进程订阅话题（事件作为内部 Call 投递，默认 on_event/2）。"
  @spec subscribe(t(), term(), pid()) :: :ok
  def subscribe(troupe, topic, pid) when is_pid(pid), do: Troupe.Show.subscribe(troupe, topic, pid)

  @doc "外部 {module, function}（或 {m, f, args}）订阅（Audience 代理）；返回退订函数。"
  @spec subscribe_mf(t(), term(), {module(), atom()} | {module(), atom(), [term()]}) :: (-> :ok)
  def subscribe_mf(troupe, topic, mf_args) do
    Troupe.Show.subscribe_mf(troupe, topic, mf_args)
  end

  @doc "退订（Actor 进程）。"
  @spec unsubscribe(t(), term(), pid()) :: :ok
  def unsubscribe(troupe, topic, pid), do: Troupe.Show.unsubscribe(troupe, topic, pid)

  # ==========================================================================
  # 观测
  # ==========================================================================

  @doc "Director（导演）内省入口（模块级 API）。"
  @spec director(t()) :: module()
  def director(_troupe), do: Troupe.Director

  @doc "每角色指标快照：calls/errors/rejections/cue_skipped + 延迟分位数。"
  @spec metrics(t() | atom()) :: map()
  def metrics(troupe_or_name) do
    name = name_of(troupe_or_name)
    Troupe.Metrics.snapshot(name)
  end

  @doc "最近 n 条 Trace 事件（节点级环形缓冲）。"
  @spec traces(t() | atom(), non_neg_integer(), keyword()) :: [map()]
  def traces(troupe_or_name, n \\ 50, filter \\ []) do
    Troupe.Trace.traces(name_of(troupe_or_name), n, filter)
  end

  @doc "订阅实时 Trace 事件（消息 `{:troupe_trace, event}` 发往 self()）。返回取消函数。"
  @spec subscribe_trace(keyword()) :: (-> :ok)
  def subscribe_trace(filter \\ []), do: Troupe.Trace.subscribe_trace(filter)

  defp name_of(%__MODULE__{name: name}), do: name
  defp name_of(name) when is_atom(name), do: name
end

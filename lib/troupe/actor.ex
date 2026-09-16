defmodule Troupe.Actor do
  @moduledoc """
  Actor（演员）定义入口：`use Troupe.Actor`。

  ## 设计要点（与 Troupe.js 的对应）

  Troupe.js 把"状态全在 props、实例字段不存业务状态"定为 swap 守则第一条；
  Elixir 的不可变数据让这条守则成为语言事实——**Actor 方法是纯函数**：

  ```
  def method(props, arg1, arg2)  ->  {reply, new_props}
  ```

  方法签名第一个参数是 Props 快照，返回 `{回复, 新 Props}`。无隐式状态、无实例字段，
  不经进程也能单测。运行时由 `Troupe.ActorServer`（舞台监督）驱动：

  * **严格串行（默认）**：上一 Turn settle 前不取下一 Call，天然无锁
  * `read_only: [..]` 标注：只读方法在并发进程中以 Props 快照执行，可与其它
    标注方法交错；返回裸值（Props 不可变，改动天然不生效）
  * `improv: [..]` 标注：即兴方法在并发进程中执行，提交按完成序应用（整 Props 覆盖）；
    跨多步一致性请用 strict，或把变更压缩为一次提交
  * **写饥饿防护**：strict 排队后不再启动新标注 Turn；strict 启动仍按 FIFO

  ## 声明

  ```elixir
  defmodule MyApp.CartActor do
    use Troupe.Actor,
      intermission: "10m",                 # 空闲自动下场，默认 "5m"

      read_only: [:peek],
      timeouts: [add: 5_000],
      cues: [flush: "2s"]                  # 内存 Cue：每 2s 调 flush/1

    def add(props, item) do
      props = Map.update!(props, :items, &[item | &1])
      {:save, :ok, props}                  # 持久成功：提交成功后才回复
    end

    def peek(props), do: {:reply, length(props.items)}
    def flush(props), do: {:save, :ok, props}
  end
  ```

  ## 返回形状（strict / improv 方法与 on_cue / on_event）

  | 返回 | 语义 |
  |---|---|
  | `{:reply, result}` | 回复 result，Props 不变 |
  | `{:reply, result, props}` | 回复 result，提交新 Props（内存） |
  | `{:reply, result, props, opts}` | 同上 + `opts: [effects: [...]]` |
  | `{:save, result, props}` | **持久成功**：CAS 落盘成功后才回复 |
  | `{:save, result, props, opts}` | 同上 + effects |
  | read_only 方法 | 返回裸值（不做解析） |

  ## 生命周期回调（可选覆写）

  * `on_stage(props, ctx)` —— 登台后（Props 已就绪）；返回 `{:ok, props}` / `{:ok, props, effects}` / `props` / `:ok`
  * `off_stage(props, ctx)` —— 下场前；返回同上（框架在排空后总是持久化 Props）
  * `on_cue(props, name, payload)` —— 持久提醒入口（使用提醒必须定义）
  * `on_event(props, event)` —— Show 订阅默认回调
  * `migrate_props(stored_props, from_version)` —— schemaVersion 逐级迁移
  * `validate_props(props)` —— 读入校验；`:ok` / `{:error, reason}` / raise

  ## Effects（纯函数的副作用出口）

  方法体不直接产生副作用，而是声明 effects 由框架在提交后执行：
  `{:reminder, name, spec}`、`{:cancel_reminder, name}`、`{:publish, topic, event}`、
  `{:subscribe, topic}`、`{:unsubscribe, topic}`。
  """

  @moduledoc since: "0.1.0"

  # 生命周期/集成回调（均可选；`use` 提供默认实现，覆写即生效）
  @callback on_stage(props :: map(), ctx :: map()) ::
              :ok | map() | {:ok, map()} | {:ok, map(), effects :: [term()]}
  @callback off_stage(props :: map(), ctx :: map()) ::
              :ok | map() | {:ok, map()} | {:ok, map(), effects :: [term()]}
  @callback on_cue(props :: map(), name :: String.t(), payload :: term()) :: term()
  @callback on_event(props :: map(), event :: term()) :: term()
  @callback migrate_props(stored_props :: map(), from_version :: non_neg_integer()) :: map()
  @callback validate_props(props :: map()) :: :ok | {:error, term()}

  @optional_callbacks on_stage: 2, off_stage: 2, on_cue: 3, on_event: 2, migrate_props: 2, validate_props: 1

  @reserved [
    :__troupe__,
    :__troupe_role_id__,
    :__troupe_intermission__,
    :__troupe_initial_props__,
    :__troupe_schema_version__,
    :__troupe_kind__,
    :__troupe_timeout__,
    :__troupe_cues__,
    :__troupe_rpc__,
    :on_stage,
    :off_stage,
    :on_cue,
    :on_event,
    :migrate_props,
    :validate_props,
    :initial_props,
    :module_info,
    :behaviour_info
  ]

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Troupe.Actor
      import Troupe.Actor, only: [ctx: 0, idempotent: 4]

      @troupe_opts opts

      # -- 默认生命周期实现（defoverridable：用户定义即覆写） --
      def on_stage(props, _ctx), do: {:ok, props}
      def off_stage(props, _ctx), do: {:ok, props}

      def on_event(_props, event) do
        raise "show event #{inspect(event)} delivered but on_event/2 is not overridden"
      end

      def on_cue(_props, name, _payload) do
        raise "persistent reminder #{inspect(name)} fired but on_cue/3 is not overridden"
      end

      defoverridable on_stage: 2, off_stage: 2, on_event: 2, on_cue: 3

      @before_compile Troupe.Actor
    end
  end

  # -- 编译期元数据 --------------------------------------------------------

  @doc false
  defmacro __before_compile__(env) do
    opts = validate_opts(Module.get_attribute(env.module, :troupe_opts) || [])
    module = env.module
    role_id = derive_role_id(module, Keyword.get(opts, :role_id))

    kinds =
      for name <- Keyword.get(opts, :read_only, []), do: {name, :read_only}

    kinds =
      kinds ++ for name <- Keyword.get(opts, :improv, []), do: {name, :improv}

    kind_clauses =
      Enum.map(kinds, fn {name, kind} ->
        quote do
          def __troupe_kind__(unquote(name)), do: unquote(kind)
        end
      end)

    fallback_kind =
      quote do
        def __troupe_kind__(_name), do: :strict
      end

    timeout_kw = Keyword.get(opts, :timeouts, [])

    timeout_clauses =
      Enum.map(timeout_kw, fn {name, ms} ->
        quote do
          def __troupe_timeout__(unquote(name)), do: unquote(Troupe.Time.parse(ms))
        end
      end)

    fallback_timeout =
      quote do
        def __troupe_timeout__(_name), do: nil
      end

    cues =
      opts
      |> Keyword.get(:cues, [])
      |> Enum.map(fn {method, spec} ->
        {every, cue_name} =
          case spec do
            opts2 when is_list(opts2) ->
              {Troupe.Time.parse(Keyword.fetch!(opts2, :every)),
               Keyword.get(opts2, :name) || to_string(method)}

            every ->
              {Troupe.Time.parse(every), to_string(method)}
          end

        {cue_name, method, every}
      end)

    rpc = Keyword.get(opts, :rpc, :all)

    declared =
      Enum.map(kinds, fn {name, kind} -> {name, kind} end) ++
        Enum.map(timeout_kw, fn {name, _} -> {name, :timeout} end) ++
        Enum.map(cues, fn {_cue, method, _every} -> {method, :cue} end)

    quote do
      def __troupe_role_id__, do: unquote(role_id)

      def __troupe_intermission__,
        do: unquote(Troupe.Time.parse(Keyword.get(opts, :intermission, "5m")))

      def __troupe_schema_version__, do: unquote(Keyword.get(opts, :schema_version, 1))
      def __troupe_rpc__, do: unquote(Macro.escape(rpc))
      def __troupe_declared__, do: unquote(Macro.escape(declared))

      unquote_splicing(kind_clauses)
      unquote(fallback_kind)
      unquote_splicing(timeout_clauses)
      unquote(fallback_timeout)
      def __troupe_cues__, do: unquote(Macro.escape(cues))
    end
  end

  defp validate_opts(opts) do
    if Keyword.has_key?(opts, :initial_props) do
      raise ArgumentError,
            "initial_props is declared as a public 0-arity function on the actor module:" <>
              "  def initial_props, do: %{...}  (structure-less actors may omit it; default %{})"
    end

    ro = Keyword.get(opts, :read_only, [])
    imp = Keyword.get(opts, :improv, [])
    overlap = MapSet.intersection(MapSet.new(ro), MapSet.new(imp))

    if MapSet.size(overlap) > 0 do
      raise ArgumentError,
            "methods cannot be both read_only and improv: #{inspect(MapSet.to_list(overlap))}"
    end

    opts
  end

  @doc false
  def derive_role_id(module, explicit)

  def derive_role_id(_module, explicit) when is_binary(explicit), do: explicit

  def derive_role_id(module, nil) do
    case module |> Module.split() |> List.last() |> String.replace_suffix("Actor", "") do
      "" ->
        raise ArgumentError,
              "cannot derive role_id from #{inspect(module)} — set `role_id:` explicitly"

      derived ->
        derived
    end
  end

  @doc "保留函数（生命周期/元数据）——永不在 RPC 白名单（信任边界，Troupe.js DESIGN §6.1）。"
  def reserved, do: @reserved

  # -- 运行时助手（在 Actor 方法体内可用） ---------------------------------

  @doc "当前执行上下文：`%{troupe_name:, role_id:, stage_name:}`。等价 JS 的 this.*。"
  def ctx do
    case Process.get(:troupe_ctx) do
      nil -> raise "Troupe.Actor.ctx/0 called outside of actor execution context"
      ctx -> ctx
    end
  end

  @doc """
  幂等执行（Troupe.js DESIGN §6.5）：同键同指纹重放原结果；同键异指纹抛
  `Troupe.IdempotencyConflictError`。幂等记录随 Props 原子提交——调用方负责以
  `{:save, ...}` 返回（fn 内不要落盘）。记录保留期限由业务自管（可加清理 Cue）。
  """
  @spec idempotent(map(), term(), term(), (map() -> {result :: term(), map()})) ::
          {:executed, term(), map()} | {:replayed, term(), map()}
  def idempotent(props, key, fingerprint, fun) when is_map(props) and is_function(fun, 1) do
    records = Map.get(props, :__troupe_idempotency__, %{})

    case Map.fetch(records, key) do
      {:ok, {^fingerprint, result}} ->
        {:replayed, result, props}

      {:ok, {other, _result}} ->
        raise Troupe.IdempotencyConflictError, key: key, fingerprint: other

      :error ->
        {result, props2} = fun.(props)
        records2 = Map.put(records, key, {fingerprint, result})
        props3 = Map.put(props2, :__troupe_idempotency__, records2)
        {:executed, result, props3}
    end
  end
end

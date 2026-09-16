defmodule Troupe.ActorServer do
  @moduledoc """
  舞台监督（StageManager）：每个在场 Actor 一个进程，承载执行模型的核心契约。

  ## 执行模型（Troupe.js DESIGN §5.1 / §5.4）

  * **严格串行（默认）**：账本空闲时 strict 直接在本进程内执行（快路径）；
    忙碌时按 FIFO 入队 CallBoard，队列深度上限即背压。
  * **排队过期不执行**：出队时 deadline 已过的 Call 直接拒绝（零副作用，可安全重试）。
  * **执行占用与响应期限分离**：调用方超时收到"结果未知"，但旧 Turn 继续执行到
    settle，串行通道不放行——后续调用不会与未结束的旧任务交错修改 Props。
  * **交错执行**：`read_only`/`improv` 方法由账本派发到并发进程（快照 + 完成序提交）；
    strict 入队后停止启动新标注 Turn（写饥饿防护）。
  * **生命周期**：未激活 → 激活中 → 运行中 → 排空中 → 失效；激活即单飞
    （进程即互斥）；保存失败 → 失效 → 下次调用从最后已提交快照重载。
  * **Intermission**：空闲超时排空——off_stage 钩子 → CAS 落盘 → 停止；
    排空中到达的请求在排空完成后重新激活并继续服务（`drain_policy: :wait`）。

  账本（ledger）= `strict`（在执行独占通道）+ `in_flight`（标注并发任务）+
  `pending`（FIFO 队列）。所有完成事件经消息驱动 `:troupe_dispatch` 循环。
  """

  use GenServer, restart: :temporary

  require Logger

  alias Troupe.{Metrics, Repertoire, Time, Trace}

  @enforce_keys [:troupe, :module, :role_id, :stage_name]
  defstruct [
    :troupe,
    :module,
    :role_id,
    :stage_name,
    :key,
    :props,
    :revision,
    :schema_version,
    :lifecycle,
    :invalid_reason,
    :pending,
    :pending_n,
    :pending_strict,
    :in_flight,
    :intermission_ms,
    :intermission_ref,
    :last_activity,
    :rpc,
    :cues,
    :cue_timers,
    :cue_running,
    :ownership_epoch,
    :renew_ref,
    :shutting_down
  ]

  @type from :: {pid(), term()} | nil

  # ==========================================================================
  # 启动 / 激活
  # ==========================================================================

  @doc false
  @spec start_link(%{troupe: map(), module: module(), role_id: String.t(), stage_name: String.t()}, keyword()) ::
          {:ok, pid()} | {:error, term()}
  def start_link(arg, opts) do
    GenServer.start_link(__MODULE__, arg, opts)
  end

  @impl true
  def init(arg) do
    # on_stage 及后续 Turn 内的 Troupe.Actor.ctx/0 依赖该进程字典
    Process.put(:troupe_ctx, %{troupe_name: arg.troupe.name, role_id: arg.role_id, stage_name: arg.stage_name})

    s =
      %__MODULE__{
        troupe: arg.troupe,
        module: arg.module,
        role_id: arg.role_id,
        stage_name: arg.stage_name,
        lifecycle: :activating,
        pending: :queue.new(),
        pending_n: 0,
        pending_strict: 0,
        in_flight: %{},
        cue_timers: %{},
        cue_running: MapSet.new(),
        shutting_down: false
      }
      |> put_key()
      |> set_meta()

    # 注册后进程即可被发现；激活失败不崩进程，进 :invalid 等下次调用重试
    case activate(s) do
      {:ok, s2} ->
        {:ok, s2}

      {:error, reason, %__MODULE__{} = s2} ->
        {:ok, %__MODULE__{s2 | lifecycle: :invalid, invalid_reason: reason}}
    end
  end

  defp put_key(%__MODULE__{} = s),
    do: %__MODULE__{s | key: {s.troupe.config.namespace, s.role_id, s.stage_name}}

  defp set_meta(%__MODULE__{} = s) do
    %__MODULE__{
      s
      | intermission_ms: s.module.__troupe_intermission__(),
        rpc: Repertoire.fetch!(s.troupe.repertoire, s.module),
        cues: s.module.__troupe_cues__()
    }
  end

  # ==========================================================================
  # 激活：加载 Props → 迁移 → 校验 → 所有权 → on_stage
  # ==========================================================================

  defp activate(%__MODULE__{} = s) do
    {store_mod, store_ctx} = s.troupe.store

    with {:ok, s, props, revision, sv} <- load(store_mod, store_ctx, s),
         {:ok, s} <- acquire_ownership(s),
         {:ok, props, effects} <- run_on_stage(s, props) do
      s =
        %__MODULE__{
          s
          | lifecycle: :running,
            props: props,
            revision: revision,
            schema_version: sv,
            invalid_reason: nil,
            last_activity: Time.now_ms()
        }

      s = schedule_intermission(s)
      s = schedule_cues(s)
      s = schedule_ownership_renewal(s)
      process_effects(s, effects)
      {:ok, s}
    end
  end

  # load 可能 throw {:activation_error, reason, s}
  defp load(store_mod, store_ctx, %__MODULE__{} = s) do
    case store_mod.read(store_ctx, s.key) do
      {:ok, nil} ->
        props = initial_props(s.module)

        unless is_map(props) do
          throw_activation(s, "initial_props must return a map, got: #{inspect(props)}")
        end

        case store_mod.create(store_ctx, s.key, props, s.module.__troupe_schema_version__()) do
          {:ok, stored} ->
            {:ok, s, stored.props, stored.revision, stored.schema_version}

          # 并发激活已建记录（共享存储多节点）：采纳现有记录
          {:error, {:conflict, :exists}} ->
            read_existing(store_mod, store_ctx, s)

          {:error, cause} ->
            store_failure(s, "create", cause)
        end

      {:ok, stored} ->
        adopt(store_mod, store_ctx, s, stored)

      {:error, cause} ->
        store_failure(s, "read", cause)
    end
  catch
    :throw, {:activation_error, reason, s2} -> {:error, reason, s2}
  end

  defp read_existing(store_mod, store_ctx, s) do
    case store_mod.read(store_ctx, s.key) do
      {:ok, stored} when stored != nil -> adopt(store_mod, store_ctx, s, stored)
      _ -> store_failure(s, "read-after-create-conflict", :record_missing)
    end
  end

  defp adopt(store_mod, store_ctx, %__MODULE__{} = s, stored) do
    target_sv = s.module.__troupe_schema_version__()

    with {:ok, props, sv} <- migrate(s, stored.props, stored.schema_version, target_sv),
         :ok <- validate(s, props) do
      if sv != stored.schema_version do
        # 迁移产物立即落盘（以当前 revision CAS）
        case store_mod.write(store_ctx, s.key, props,
               expected_revision: stored.revision,
               schema_version: sv
             ) do
          {:ok, new_stored} ->
            {:ok, s, new_stored.props, new_stored.revision, new_stored.schema_version}

          {:error, cause} ->
            store_failure(s, "write(migrated)", cause)
        end
      else
        {:ok, s, props, stored.revision, sv}
      end
    end
  end

  # schemaVersion 逐级 +1 迁移（DESIGN §7.4）；迁移失败 = 激活失败，不得以半迁移状态服务
  defp migrate(_s, props, sv, sv), do: {:ok, props, sv}

  defp migrate(s, props, from, target) when from < target do
    unless function_exported?(s.module, :migrate_props, 2) do
      throw_activation(
        s,
        "stored schema_version=#{from} < declared #{target} but migrate_props/2 is not defined"
      )
    end

    case s.module.migrate_props(props, from) do
      props2 when is_map(props2) ->
        migrate(s, props2, from + 1, target)

      other ->
        throw_activation(
          s,
          "migrate_props/2 (#{from}→#{from + 1}) returned non-map: #{inspect(other)}"
        )
    end
  end

  defp migrate(s, _props, from, target) do
    throw_activation(
      s,
      "stored schema_version=#{from} is newer than declared #{target} — deploy the newer code first"
    )
  end

  defp validate(s, props) do
    if function_exported?(s.module, :validate_props, 1) do
      case s.module.validate_props(props) do
        :ok ->
          :ok

        {:error, reason} ->
          throw_activation(s, "validate_props rejected: #{inspect(reason)}")

        other ->
          throw_activation(
            s,
            "validate_props must return :ok | {:error, reason}, got #{inspect(other)}"
          )
      end
    else
      :ok
    end
  end

  defp run_on_stage(%__MODULE__{} = s, props) do
    outcome =
      try do
        {:ok, s.module.on_stage(props, actor_ctx(s))}
      rescue
        e -> {:raise, e}
      end

    case outcome do
      {:ok, normalized} ->
        case normalize_lifecycle_return(normalized, props) do
          {:ok, props2, effects} -> {:ok, props2, effects}
          {:error, reason} -> throw_activation(s, "on_stage bad return: #{reason}")
        end

      {:raise, e} ->
        throw_activation(s, "on_stage raised: #{Exception.message(e)}")
    end
  end

  defp normalize_lifecycle_return(ret, original_props) do
    case ret do
      :ok -> {:ok, original_props, []}
      props when is_map(props) -> {:ok, props, []}
      {:ok, props, [effects: list]} when is_map(props) and is_list(list) -> {:ok, props, list}
      {:ok, props, effects} when is_map(props) -> {:ok, props, List.wrap(effects)}
      {:ok, props} when is_map(props) -> {:ok, props, []}
      other -> {:error, inspect(other)}
    end
  end

  defp initial_props(module) do
    if function_exported?(module, :initial_props, 0) do
      module.initial_props()
    else
      %{}
    end
  end

  defp throw_activation(s, reason), do: throw({:activation_error, reason, s})

  defp store_failure(s, op, cause), do: throw({:activation_error, {op, cause}, s})

  # ==========================================================================
  # Call 入口
  # ==========================================================================

  @impl true
  def handle_call(req, from, s)

  def handle_call(:troupe_state, _from, s) do
    {:reply, snapshot(s), s}
  end

  def handle_call(:troupe_off, _from, %__MODULE__{} = s) do
    {:reply, :ok, begin_drain(s, force: s.shutting_down)}
  end

  def handle_call(:troupe_shutdown, _from, %__MODULE__{} = s) do
    s = %__MODULE__{s | shutting_down: true}
    {:reply, :ok, begin_drain(s, force: true)}
  end

  def handle_call({:troupe_call, fun, args, deadline_at, caller_ctx, internal?}, from, s) do
    now = Time.now_ms()

    cond do
      not internal? and not rpc_allowed?(s, fun, args) ->
        reject(from, s, :whitelist, "method #{fun}/#{length(args) + 1} is not callable", false)

      deadline_at != nil and deadline_at <= now ->
        # 排队过期：未开始执行，零副作用，可安全重试
        _ = reply_error(from, s, :timeout_queued, fun)
        {:noreply, s}

      s.shutting_down and not internal? ->
        reject(from, s, :shutdown, "troupe is shutting down", false)

      s.lifecycle == :invalid ->
        case reactivate(s) do
          {:ok, s2} ->
            handle_call({:troupe_call, fun, args, deadline_at, caller_ctx, internal?}, from, s2)

          {:error, reason, s2} ->
            _ = reply_error(from, s2, {:activation, reason}, fun)
            {:noreply, s2}
        end

      true ->
        accept(s, from, fun, args, deadline_at, caller_ctx, internal?)
    end
  end

  # 接受一条 Call：分类 → 快路径执行 / 入队
  defp accept(%__MODULE__{} = s, from, fun, args, deadline_at, caller_ctx, internal?) do
    kind =
      if internal? do
        :strict
      else
        s.module.__troupe_kind__(fun)
      end

    item = %{
      from: from,
      module: s.module,
      fun: fun,
      args: args,
      deadline_at: deadline_at,
      caller_ctx: caller_ctx,
      internal?: internal?,
      kind: kind
    }

    cond do
      kind == :strict and idle?(s) ->
        {:noreply, run_strict(s, item)}

      kind == :strict ->
        enqueue(s, item)

      can_start_annotated?(s) ->
        # 与在飞标注 Turn 交错（组合矩阵：annotated × annotated）
        {:noreply, spawn_annotated(s, item)}

      true ->
        # strict 排队：封锁新标注 Turn（写饥饿防护），入队等待
        enqueue(s, item)
    end
  end

  defp enqueue(%__MODULE__{} = s, item) do
    limit = s.troupe.config.queue_depth

    if not item.internal? and s.pending_n >= limit do
      reject(item.from, s, :backpressure, "callboard depth #{s.pending_n} >= #{limit}", true)
    else
      send(self(), :troupe_dispatch)
      {:noreply, touch(put_pending(s, item))}
    end
  end

  defp put_pending(%__MODULE__{} = s, item) do
    strict_extra = if item.kind == :strict, do: 1, else: 0

    %__MODULE__{
      s
      | pending: :queue.in(item, s.pending),
        pending_n: s.pending_n + 1,
        pending_strict: s.pending_strict + strict_extra
    }
  end

  # ==========================================================================
  # 调度循环：队列出队（过期拒绝）→ strict 内联 / 标注派发
  # ==========================================================================

  defp dispatch(%__MODULE__{} = s) do
    cond do
      s.lifecycle not in [:running, :draining] ->
        s

      :queue.is_empty(s.pending) ->
        s

      true ->
        {{:value, item}, q} = :queue.out(s.pending)

        strict_extra = if item.kind == :strict, do: 1, else: 0

        s2 = %__MODULE__{
          s
          | pending: q,
            pending_n: s.pending_n - 1,
            pending_strict: s.pending_strict - strict_extra
        }

        if item.deadline_at != nil and item.deadline_at <= Time.now_ms() do
          reply_error(item.from, s2, :timeout_queued, item.fun)
          dispatch(s2)
        else
          case classify_run(s2, item) do
            :run_strict ->
              dispatch(run_strict(s2, item))

            :run_annotated ->
              dispatch(spawn_annotated(s2, item))

            :wait ->
              # 队首不能运行：队首阻塞，保持 FIFO（strict 启动仍按 FIFO）
              strict_extra = if item.kind == :strict, do: 1, else: 0

              %__MODULE__{
                s2
                | pending: :queue.in_r(item, s2.pending),
                  pending_n: s2.pending_n + 1,
                  pending_strict: s2.pending_strict + strict_extra
              }
          end
        end
    end
  end

  defp classify_run(s, item) do
    cond do
      # strict 是独占通道：只需无在飞 Turn（排在它后面的 pending 项在它之后调度）
      item.kind == :strict and map_size(s.in_flight) == 0 -> :run_strict
      item.kind == :strict -> :wait
      # strict 排队后不再启动新标注 Turn（写饥饿防护）；否则与在飞标注交错
      s.pending_strict == 0 -> :run_annotated
      true -> :wait
    end
  end

  # ==========================================================================
  # Turn 执行
  # ==========================================================================

  # strict：在本进程内联执行（独占通道）
  defp run_strict(%__MODULE__{} = s, item) do
    t0 = Time.now_ms()
    outcome = wrap_outcome(invoke(s, item))
    {_tag, s} = apply_outcome(s, item, outcome, t0)
    s
  end

  # 标注：派发到并发进程（快照执行，完成序提交）
  defp spawn_annotated(%__MODULE__{} = s, item) do
    ref = make_ref()
    parent = self()
    ctx = actor_ctx(s)
    props = s.props
    %{module: module, fun: fun, args: args} = item

    {:ok, task_pid} =
      Task.Supervisor.start_child(Troupe.Names.lookup!(s.troupe.name, :task_sup), fn ->
        Process.put(:troupe_ctx, ctx)

        outcome =
          try do
            {:ok, normalize_call_return(apply(module, fun, [props | args]), item)}
          rescue
            e -> {:raise, e}
          catch
            kind, value -> {:raise, {kind, value}}
          end

        send(parent, {:troupe_task_done, ref, outcome})
      end)

    Process.monitor(task_pid)

    %__MODULE__{
      s
      | in_flight: Map.put(s.in_flight, ref, %{item: item, pid: task_pid, started_at: Time.now_ms()})
    }
    |> touch()
  end

  # 统一调用用户代码并归一化结果（strict 内联路径）
  defp invoke(s, item) do
    try do
      {:ok, normalize_call_return(apply(s.module, item.fun, [s.props | item.args]), item)}
    rescue
      e -> {:raise, e}
    catch
      kind, value -> {:raise, {kind, value}}
    end
  end

  defp normalize_call_return(result, item) do
    case result do
      {:reply, r} ->
        {:ok, r, :unchanged, []}

      {:reply, r, props} when is_map(props) ->
        {:ok, r, props, []}

      {:reply, r, props, opts} when is_map(props) and is_list(opts) ->
        {:ok, r, props, Keyword.get(opts, :effects, [])}

      {:save, r, props} when is_map(props) ->
        {:ok, r, props, [:__save__]}

      {:save, r, props, opts} when is_map(props) and is_list(opts) ->
        {:ok, r, props, [:__save__ | Keyword.get(opts, :effects, [])]}

      other when item.kind == :read_only ->
        {:ok, other, :unchanged, []}

      other ->
        {:bad_return, other}
    end
  end

  # 应用执行结果：提交 Props / 落盘 / 回复 / effects / 失效
  defp apply_outcome(%__MODULE__{} = s, item, outcome, t0) do
    dur = Time.now_ms() - t0

    case outcome do
      {:ok, reply, props, tagged_effects} ->
        {save?, effects} =
          List.foldr(tagged_effects, {false, []}, fn
            :__save__, {_, acc} -> {true, acc}
            e, {sv, acc} -> {sv, [e | acc]}
          end)

        case commit(s, props, save?) do
          {:ok, s} ->
            process_effects(s, effects)
            reply_ok(item.from, reply)
            record_turn(s, item, dur, :ok)
            {:ok, finish_turn(s)}

          {:error, s, error} ->
            reply_error(item.from, s, {:business, error}, item.fun)
            record_turn(s, item, dur, :error)
            {:error, finish_turn(s)}
        end

      {:business_error, e} ->
        reply_error(item.from, s, {:business, e}, item.fun)
        record_turn(s, item, dur, :error)
        {:error, finish_turn(s)}

      {:bad_return, other} ->
        e =
          Troupe.BadReturnError.exception(module: s.module, method: item.fun, got: other)

        reply_error(item.from, s, {:business, e}, item.fun)
        record_turn(s, item, dur, :error)
        {:error, finish_turn(s)}
    end
  end

  # invoke / 任务回传的包装归一
  defp wrap_outcome({:ok, normalized}), do: normalized

  defp wrap_outcome({:raise, e}) when is_exception(e), do: {:business_error, e}

  defp wrap_outcome({:raise, {kind, value}}) do
    {:business_error, RuntimeError.exception(message: "exited (#{kind}): #{inspect(value)}")}
  end

  # 提交：内存或 CAS 落盘；失败 → 实例失效（不基于未提交状态继续服务）
  defp commit(%__MODULE__{} = s, :unchanged, _save?), do: {:ok, s}
  defp commit(%__MODULE__{} = s, props, false), do: {:ok, %__MODULE__{s | props: props}}

  defp commit(%__MODULE__{} = s, props, true) do
    {store_mod, store_ctx} = s.troupe.store

    opts = [
      expected_revision: s.revision,
      schema_version: s.module.__troupe_schema_version__()
    ]

    opts = if s.ownership_epoch, do: [fencing_token: s.ownership_epoch] ++ opts, else: opts

    case store_mod.write(store_ctx, s.key, props, opts) do
      {:ok, stored} ->
        {:ok, %__MODULE__{s | props: stored.props, revision: stored.revision}}

      {:error, {:conflict, _kind}} ->
        {:error, invalidate(s), conflict_error(s)}

      {:error, {:conflict, _kind, _details}} ->
        {:error, invalidate(s), conflict_error(s)}

      {:error, cause} ->
        {:error, invalidate(s),
         Troupe.StoreError.exception(store: inspect(store_mod), op: "write", cause: cause)}
    end
  end

  defp conflict_error(s),
    do: Troupe.ConflictError.exception(kind: :revision, role_id: s.role_id, stage_name: s.stage_name)

  defp invalidate(%__MODULE__{} = s) do
    cancel_all_timers(s)
    %__MODULE__{s | lifecycle: :invalid, invalid_reason: :save_failed, props: nil, revision: nil}
  end

  defp finish_turn(%__MODULE__{} = s) do
    s = touch(s)

    if s.lifecycle == :draining and ledger_idle?(s) do
      finish_drain(s)
    else
      send(self(), :troupe_dispatch)
      s
    end
  end

  # ==========================================================================
  # Effects：纯函数方法的副作用出口（提交后由框架执行，失败仅记录）
  # ==========================================================================

  defp process_effects(_s, []), do: :ok

  defp process_effects(s, [effect | rest]) do
    case effect do
      {:reminder, name, spec} ->
        schedule_reminder(s, name, spec)

      {:cancel_reminder, name} ->
        if s.troupe.cue_store, do: Troupe.CueStore.cancel(s.troupe.cue_store, s.key, name)

      {:publish, topic, event} ->
        if s.troupe.show, do: Troupe.Show.publish(s.troupe, topic, event)

      {:subscribe, topic} ->
        if s.troupe.show, do: Troupe.Show.subscribe(s.troupe, topic, self())

      {:unsubscribe, topic} ->
        if s.troupe.show, do: Troupe.Show.unsubscribe(s.troupe, topic, self())

      other ->
        Logger.warning("troupe: unknown effect #{inspect(other)} from #{s.role_id}/#{s.stage_name}")
    end

    process_effects(s, rest)
  end

  defp schedule_reminder(s, name, spec) do
    if s.troupe.cue_store do
      now = Time.now_ms()

      {next_due, spec2, payload} =
        case spec do
          [at: at] -> {at, {:at, at}, nil}
          [at: at, payload: p] -> {at, {:at, at}, p}
          [every: ms] -> {now + ms, {:every, ms}, nil}
          [every: ms, payload: p] -> {now + ms, {:every, ms}, p}
          other -> raise ArgumentError, "invalid reminder spec #{inspect(other)} for #{name}"
        end

      Troupe.CueStore.upsert(s.troupe.cue_store, s.key, name, payload, spec2, next_due)
    else
      Logger.warning(
        "troupe: reminder #{inspect(name)} scheduled but cues are not enabled on #{inspect(s.troupe.name)}" <>
          " — enable with Troupe.form(cues: true)"
      )
    end

    :ok
  end

  # ==========================================================================
  # 标注任务完成 / 崩溃
  # ==========================================================================

  @impl true
  def handle_info(:troupe_dispatch, s) do
    {:noreply, dispatch(s)}
  end

  @impl true
  def handle_info({:troupe_task_done, ref, outcome}, %__MODULE__{} = s) do
    case Map.pop(s.in_flight, ref) do
      {nil, _} ->
        {:noreply, s}

      {%{item: item, started_at: t0}, in_flight} ->
        s = %__MODULE__{s | in_flight: in_flight}
        {_tag, s} = apply_outcome(s, item, wrap_outcome(outcome), t0)
        {:noreply, s}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, %__MODULE__{} = s) do
    if reason == :normal do
      {:noreply, s}
    else
      case Enum.find(s.in_flight, fn {_k, v} -> v.pid == pid end) do
        nil ->
          {:noreply, s}

        {ref, %{item: item, started_at: t0}} ->
          s = %__MODULE__{s | in_flight: Map.delete(s.in_flight, ref)}

          e =
            Troupe.UnknownOutcomeError.exception(
              role_id: s.role_id,
              stage_name: s.stage_name,
              method: item.fun
            )

          reply_error(item.from, s, {:business, e}, item.fun)
          record_turn(s, item, Time.now_ms() - t0, :error)
          {:noreply, finish_turn(s)}
      end
    end
  end

  # ==========================================================================
  # Cue（内存） / Intermission / 所有权续期 / Show 事件
  # ==========================================================================

  @impl true
  def handle_info({:troupe_cue_tick, cue_name, method, every, due_at}, %__MODULE__{} = s) do
    s = %__MODULE__{s | cue_timers: Map.delete(s.cue_timers, cue_name)}

    cond do
      s.lifecycle != :running ->
        {:noreply, s}

      Time.now_ms() > due_at + div(every, 2) ->
        # 上一拍未完成（Tick 迟到）：跳拍——cueSkipped 是周期任务变慢的首要信号
        Metrics.bump(s.troupe.name, s.role_id, :cue_skipped)
        {:noreply, schedule_cue(s, cue_name, method, every)}

      idle?(s) ->
        item = internal_item(s, method, [])
        s = %__MODULE__{s | cue_running: MapSet.put(s.cue_running, cue_name)}
        s = %__MODULE__{} = run_strict(s, item)
        s = %__MODULE__{s | cue_running: MapSet.delete(s.cue_running, cue_name)}
        {:noreply, schedule_cue(s, cue_name, method, every)}

      true ->
        # 忙碌：作为内部 Call 排队（受同一调度约束，不旁路串行规则）
        {:noreply, put_pending(s, internal_item(s, method, []))}
    end
  end

  @impl true
  def handle_info(:intermission_tick, %__MODULE__{} = s) do
    s = %__MODULE__{s | intermission_ref: nil}

    cond do
      s.lifecycle != :running ->
        {:noreply, s}

      idle?(s) and Time.now_ms() - s.last_activity >= s.intermission_ms ->
        {:noreply, begin_drain(s)}

      true ->
        {:noreply, schedule_intermission(s)}
    end
  end

  @impl true
  def handle_info(:troupe_ownership_renew, %__MODULE__{} = s) do
    s = %__MODULE__{s | renew_ref: nil}
    own = s.troupe.ownership

    if own != nil and s.ownership_epoch != nil do
      case own.module.renew(own.ctx, s.key, self(), s.ownership_epoch, own.lease_ms) do
        :ok -> {:noreply, schedule_ownership_renewal(s)}
        {:error, _lost} -> {:noreply, begin_drain(s, lost_ownership: true)}
      end
    else
      {:noreply, s}
    end
  end

  @impl true
  def handle_info({:troupe_show, _topic, event}, %__MODULE__{} = s) do
    if s.lifecycle == :running do
      send(self(), :troupe_dispatch)
      {:noreply, put_pending(s, internal_item(s, :on_event, [event])) |> touch()}
    else
      {:noreply, s}
    end
  end

  @impl true
  def handle_info(:troupe_stop, s) do
    {:stop, :normal, s}
  end

  defp internal_item(s, fun, args) do
    %{
      from: nil,
      module: s.module,
      fun: fun,
      args: args,
      deadline_at: nil,
      caller_ctx: :internal,
      internal?: true,
      kind: :strict
    }
  end

  # ==========================================================================
  # 排空（Intermission 到期 / 管理下场 / 失去所有权 / 停机）
  # ==========================================================================

  defp begin_drain(%__MODULE__{} = s, opts \\ []) do
    cancel_all_timers(s)
    s = %__MODULE__{s | lifecycle: :draining}

    cond do
      not ledger_idle?(s) ->
        # 等待在执行 Turn settle（quiesce 语义：不是只等"当前 turn"）
        s

      Keyword.get(opts, :force, false) and not :queue.is_empty(s.pending) ->
        drain_reject_pending(s)

      true ->
        finish_drain(s, opts)
    end
  end

  defp drain_reject_pending(%__MODULE__{} = s) do
    s = %__MODULE__{} = reject_pending(s, :shutdown, "draining")
    finish_drain(%__MODULE__{s | pending: :queue.new(), pending_n: 0, pending_strict: 0})
  end

  defp reject_pending(%__MODULE__{} = s, reason, detail) do
    :queue.fold(
      fn item, acc ->
        if item.from, do: reject(item.from, acc, reason, detail, false)

        acc
      end,
      s,
      s.pending
    )
  end

  # off_stage 钩子 → 落盘 → （有停泊请求则重激活，否则停止）
  defp finish_drain(%__MODULE__{} = s, opts \\ []) do
    {save?, props2} =
      if s.props == nil or Keyword.get(opts, :lost_ownership, false) do
        {false, nil}
      else
        off_stage_props(s)
      end

    s =
      if save? do
        {store_mod, store_ctx} = s.troupe.store

        opts2 = [expected_revision: s.revision, schema_version: s.module.__troupe_schema_version__()]
        opts2 = if s.ownership_epoch, do: [fencing_token: s.ownership_epoch] ++ opts2, else: opts2

        case store_mod.write(store_ctx, s.key, props2, opts2) do
          {:ok, stored} -> %__MODULE__{s | props: stored.props, revision: stored.revision}
          _ -> s
        end
      else
        s
      end

    release_ownership(s)

    cond do
      s.shutting_down ->
        s

      not :queue.is_empty(s.pending) and s.troupe.config.drain_policy == :wait ->
        # 排空期间到达的请求：排空完成后重新激活并服务
        case reactivate(s) do
          {:ok, s2} ->
            send(self(), :troupe_dispatch)
            s2

          {:error, reason, %__MODULE__{} = s3} ->
            reply_pending_activation_error(s3, reason)

            %__MODULE__{s3 | pending: :queue.new(), pending_n: 0}
            |> then(fn s4 ->
              send(self(), :troupe_stop)
              s4
            end)
        end

      true ->
        send(self(), :troupe_stop)
        s
    end
  end

  defp off_stage_props(s) do
    try do
      ret = s.module.off_stage(s.props, actor_ctx(s))

      case normalize_lifecycle_return(ret, s.props) do
        {:ok, p, _effects} -> {true, p}
        {:error, _reason} -> {false, nil}
      end
    rescue
      _e -> {false, nil}
    end
  end

  defp reply_pending_activation_error(s, reason) do
    :queue.fold(
      fn item, acc ->
        if item.from, do: reply_error(item.from, acc, {:activation, reason}, item.fun)

        acc
      end,
      s,
      s.pending
    )

    :ok
  end

  defp reactivate(%__MODULE__{} = s) do
    case activate(s) do
      {:ok, s2} ->
        {:ok, s2}

      {:error, reason, %__MODULE__{} = s2} ->
        {:error, reason, %__MODULE__{s2 | lifecycle: :invalid, invalid_reason: reason}}
    end
  end

  # ==========================================================================
  # 工具
  # ==========================================================================

  defp idle?(%__MODULE__{} = s),
    do: map_size(s.in_flight) == 0 and :queue.is_empty(s.pending)

  defp ledger_idle?(%__MODULE__{} = s), do: idle?(s)

  defp can_start_annotated?(%__MODULE__{} = s), do: s.pending_strict == 0

  defp touch(%__MODULE__{} = s), do: %__MODULE__{s | last_activity: Time.now_ms()}

  defp rpc_allowed?(s, fun, args) do
    entry = s.troupe.repertoire.by_module[s.module]
    entry != nil and MapSet.member?(entry.rpc, {fun, length(args) + 1})
  end

  defp actor_ctx(s),
    do: %{troupe_name: s.troupe.name, role_id: s.role_id, stage_name: s.stage_name}

  defp reply_ok(from, reply), do: if(from, do: GenServer.reply(from, {:ok, reply}))

  defp reply_error(from, s, error_spec, fun) do
    err =
      case error_spec do
        {:business, e} ->
          e

        {:activation, reason} ->
          Troupe.ActivationError.exception(role_id: s.role_id, stage_name: s.stage_name, cause: reason)

        :timeout_queued ->
          Troupe.CallTimeoutError.exception(
            phase: :queued,
            role_id: s.role_id,
            stage_name: s.stage_name,
            method: fun
          )
      end

    metrics_kind =
      case error_spec do
        {:business, _} -> :errors
        {:activation, _} -> :errors
        _ -> :rejections
      end

    Metrics.bump(s.troupe.name, s.role_id, metrics_kind)
    if from, do: GenServer.reply(from, {:error, err})
    :ok
  end

  # 请求拒绝：可作为 handle_call 结果直接返回（{:noreply, s}，回复已发出）
  defp reject(from, s, reason, detail, retryable) do
    Metrics.bump(s.troupe.name, s.role_id, :rejections)

    if from do
      err =
        Troupe.CallRejectedError.exception(
          reason: reason,
          role_id: s.role_id,
          stage_name: s.stage_name,
          detail: detail,
          retryable: retryable
        )

      GenServer.reply(from, {:error, err})
    end

    {:noreply, s}
  end

  defp record_turn(s, item, dur_ms, outcome) do
    Metrics.bump(s.troupe.name, s.role_id, :calls)
    Metrics.latency(s.troupe.name, s.role_id, dur_ms)

    Trace.call(s.troupe.name, %{
      caller: item.caller_ctx,
      role_id: s.role_id,
      stage_name: s.stage_name,
      method: item.fun,
      duration_ms: dur_ms,
      outcome: outcome
    })
  end

  # -- 定时器 --------------------------------------------------------------

  defp schedule_intermission(%__MODULE__{} = s) do
    ref = Process.send_after(self(), :intermission_tick, s.intermission_ms)
    %__MODULE__{s | intermission_ref: ref}
  end

  defp schedule_cues(%__MODULE__{} = s) do
    Enum.reduce(s.cues, s, fn {cue_name, method, every}, acc ->
      schedule_cue(acc, cue_name, method, every)
    end)
  end

  defp schedule_cue(%__MODULE__{} = s, cue_name, method, every) do
    due_at = Time.now_ms() + every
    ref = Process.send_after(self(), {:troupe_cue_tick, cue_name, method, every, due_at}, every)

    %__MODULE__{s | cue_timers: Map.put(s.cue_timers, cue_name, ref)}
  end

  defp cancel_all_timers(%__MODULE__{} = s) do
    if s.intermission_ref, do: Process.cancel_timer(s.intermission_ref)
    if s.renew_ref, do: Process.cancel_timer(s.renew_ref)

    for {_name, ref} <- s.cue_timers, do: Process.cancel_timer(ref)

    %__MODULE__{s | intermission_ref: nil, renew_ref: nil, cue_timers: %{}}
  end

  # -- 所有权（启用时） ----------------------------------------------------

  defp acquire_ownership(%__MODULE__{} = s) do
    case s.troupe.ownership do
      nil ->
        {:ok, %__MODULE__{s | ownership_epoch: nil}}

      own ->
        case own.module.acquire(own.ctx, s.key, self(), own.lease_ms) do
          {:ok, epoch} -> {:ok, %__MODULE__{s | ownership_epoch: epoch}}
          {:error, :busy} -> throw_activation(s, "ownership is held by another stage")
        end
    end
  end

  defp schedule_ownership_renewal(%__MODULE__{} = s) do
    if s.ownership_epoch do
      lease = s.troupe.ownership.lease_ms
      ref = Process.send_after(self(), :troupe_ownership_renew, div(lease, 3))
      %__MODULE__{s | renew_ref: ref}
    else
      s
    end
  end

  defp release_ownership(%__MODULE__{} = s) do
    if s.troupe.ownership != nil and s.ownership_epoch != nil do
      own = s.troupe.ownership
      own.module.release(own.ctx, s.key, self(), s.ownership_epoch)
    end

    %__MODULE__{s | ownership_epoch: nil}
  end

  # -- 内省 ----------------------------------------------------------------

  defp snapshot(s) do
    %{
      role_id: s.role_id,
      stage_name: s.stage_name,
      module: s.module,
      lifecycle: s.lifecycle,
      invalid_reason: s.invalid_reason,
      revision: s.revision,
      schema_version: s.schema_version,
      props: s.props,
      pending: s.pending_n,
      pending_strict: s.pending_strict,
      in_flight: map_size(s.in_flight),
      last_activity_ms: s.last_activity && Time.now_ms() - s.last_activity,
      ownership_epoch: s.ownership_epoch,
      node: node()
    }
  end

  @impl true
  def terminate(_reason, s) do
    cancel_all_timers(s)
    :ok
  end
end

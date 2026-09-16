defmodule Troupe.CueStore do
  @moduledoc """
  CueStore（提词存档）：持久提醒（Reminder）的存储。记录 = 稳定名称 + 可序列化负载 +
  计划（`:at` / `:every`）+ nextDue + statusVersion——不持久化闭包。

  投递语义为**至少一次 + 业务幂等**（Troupe.js DESIGN §7.5）：
  `claim/4` 以租约原子认领；执行中宕机 → 租约到期被重新认领、重复投递；
  注销竞争以 statusVersion CAS 裁决。当前实现为**节点级**（与默认 PropStore 的
  持久级别一致）；多节点共享 CueStore（Postgres 等）按同一接口接入后即可跨节点分派。
  """

  use GenServer

  @enforce_keys [:id, :key, :name, :payload, :spec, :next_due, :sv]
  defstruct [:id, :key, :name, :payload, :spec, :next_due, :sv, :owner, :lease_until, created_at: 0]

  @type t :: %__MODULE__{}
  @type ctx :: %{server: pid()}
  @type key :: Troupe.PropStore.key()

  # -- 启动 ---------------------------------------------------------------

  def child_spec(opts) do
    troupe_name = Keyword.fetch!(opts, :troupe_name)
    mode = Keyword.get(opts, :mode, :memory)

    %{
      id: {__MODULE__, troupe_name},
      start: {__MODULE__, :start_link, [[troupe_name: troupe_name, mode: mode]]}
    }
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :troupe_name)
    GenServer.start_link(__MODULE__, opts, name: Troupe.Names.via(name, :cue_store))
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    tid =
      if Keyword.get(opts, :mode, :memory) == :dets do
        dir = Keyword.get(opts, :dir, ".troupe")
        File.mkdir_p!(dir)
        path = :filename.join(String.to_charlist(dir), ~c"cues.dat")
        tid = :"troupe_dets_cues_#{System.unique_integer([:positive])}"
        {:ok, ^tid} = :dets.open_file(tid, path: path, repair: true, auto_save: 1_000)
        tid
      end

    {:ok, %{tid: tid, records: load_initial(tid)}}
  end

  defp load_initial(nil), do: %{}

  defp load_initial(tid) do
    :dets.foldl(fn {id, rec}, acc -> Map.put(acc, id, rec) end, %{}, tid)
  end

  @impl true
  def terminate(_reason, %{tid: nil}), do: :ok
  def terminate(_reason, %{tid: tid}), do: :dets.close(tid)

  # -- API ------------------------------------------------------------------

  @doc "安排/重排提醒（幂等 upsert）。spec: `{:at, ts}` | `{:every, ms}`；payload 须可序列化。"
  @spec upsert(ctx, key, String.t(), term(), {:at, integer()} | {:every, non_neg_integer()}, integer()) ::
          :ok
  def upsert(ctx, key, name, payload, spec, next_due),
    do: GenServer.call(ctx.server, {:upsert, key, name, payload, spec, next_due})

  @doc "到期查询（limit 截断）。"
  @spec due(ctx, integer(), non_neg_integer()) :: [t()]
  def due(ctx, now, limit), do: GenServer.call(ctx.server, {:due, now, limit})

  @doc "原子认领（租约）。返回认领后的 statusVersion（complete/4 需携带）；已被他人持有时 `{:error, :stolen}`。"
  @spec claim(ctx, String.t(), term(), non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, :stolen}
  def claim(ctx, id, owner, lease_ms), do: GenServer.call(ctx.server, {:claim, id, owner, lease_ms})

  @doc "完成确认：`next_due` 为下次到期，或 `:done`（移除）。CAS 以 statusVersion 裁决。"
  @spec complete(ctx, String.t(), non_neg_integer(), integer() | :done) :: :ok | {:error, :stale}
  def complete(ctx, id, sv, next_due), do: GenServer.call(ctx.server, {:complete, id, sv, next_due})

  @doc "注销提醒（下场/显式取消）。"
  @spec cancel(ctx, key, String.t()) :: :ok
  def cancel(ctx, key, name), do: GenServer.call(ctx.server, {:cancel, key, name})

  @doc "全量列表（Director / CLI）。"
  @spec list(ctx) :: [t()]
  def list(ctx), do: GenServer.call(ctx.server, :list)

  # -- GenServer -------------------------------------------------------------

  @impl true
  def handle_call({:upsert, key, name, payload, spec, next_due}, _from, st) do
    id = id(key, name)
    prev = Map.get(st.records, id)

    rec = %__MODULE__{
      id: id,
      key: key,
      name: name,
      payload: payload,
      spec: spec,
      next_due: next_due,
      sv: (prev && prev.sv + 1) || 1,
      created_at: System.system_time(:millisecond)
    }

    {:reply, :ok, put(st, rec)}
  end

  def handle_call({:due, now, limit}, _from, st) do
    due =
      st.records
      |> Map.values()
      |> Enum.filter(&(&1.next_due <= now))
      |> Enum.sort_by(& &1.next_due)
      |> Enum.take(limit)

    {:reply, due, st}
  end

  def handle_call({:claim, id, owner, lease_ms}, _from, st) do
    case Map.get(st.records, id) do
      nil ->
        {:reply, {:error, :stolen}, st}

      rec ->
        now = System.system_time(:millisecond)
        leased = rec.lease_until != nil and rec.lease_until > now

        cond do
          leased and rec.owner != owner ->
            {:reply, {:error, :stolen}, st}

          true ->
            rec2 = %{
              rec
              | owner: owner,
                lease_until: now + lease_ms,
                sv: rec.sv + 1
            }

            {:reply, {:ok, rec2.sv}, put(st, rec2)}
        end
    end
  end

  def handle_call({:complete, id, sv, next_due}, _from, st) do
    case Map.get(st.records, id) do
      %{sv: ^sv} = rec ->
        now = System.system_time(:millisecond)

        case next_due do
          :done ->
            {:reply, :ok, drop(st, rec)}

          due_ts when is_integer(due_ts) ->
            # 错过多个周期：默认跳过并立即补发一次（nextDue 追赶到未来）
            due_ts = max(due_ts, now)
            {:reply, :ok, put(st, %{rec | next_due: due_ts, sv: rec.sv + 1, owner: nil, lease_until: nil})}
        end

      %{sv: other} ->
        {:reply, {:error, {:stale, other}}, st}

      nil ->
        {:reply, {:error, :stale}, st}
    end
  end

  def handle_call({:cancel, key, name}, _from, st) do
    id = id(key, name)

    case Map.get(st.records, id) do
      nil -> {:reply, :ok, st}
      rec -> {:reply, :ok, drop(st, rec)}
    end
  end

  def handle_call(:list, _from, st) do
    {:reply, st.records |> Map.values() |> Enum.sort_by(& &1.next_due), st}
  end

  # -- 存储 -----------------------------------------------------------------

  defp id({ns, role_id, stage_name}, name), do: "#{ns}:#{role_id}:#{stage_name}:#{name}"

  defp put(st, rec) do
    if st.tid, do: :dets.insert(st.tid, {rec.id, rec}) && :dets.sync(st.tid)
    %{st | records: Map.put(st.records, rec.id, rec)}
  end

  defp drop(st, rec) do
    if st.tid, do: :dets.delete(st.tid, rec.id) && :dets.sync(st.tid)
    %{st | records: Map.delete(st.records, rec.id)}
  end
end

defmodule Troupe.CueRunner do
  @moduledoc """
  提醒扫描器：每 tick（默认 1s）查询 CueStore 到期记录 → 原子认领（租约）→
  将 `on_cue(name, payload)` 作为一条 Call 经路由投递（不在场则触发重新登台，
  受同一调度约束）→ 完成确认（nextDue 推进 / 移除）。

  * **至少一次**：投递失败不确认，租约到期后被重新认领、重复投递——业务幂等是前提
  * **错过多个周期**：默认跳过并立即补发一次（nextDue 追赶到当前时刻之后）
  """

  use GenServer
  alias Troupe.{Agent, Time}

  def child_spec(opts) do
    troupe_name = Keyword.fetch!(opts, :troupe_name)

    %{
      id: {__MODULE__, troupe_name},
      start:
        {__MODULE__, :start_link, [[troupe_name: troupe_name, tick_ms: Keyword.get(opts, :tick_ms, 1_000)]]}
    }
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :troupe_name)
    GenServer.start_link(__MODULE__, opts, name: Troupe.Names.via(name, :cue_runner))
  end

  @impl true
  def init(opts) do
    troupe_name = Keyword.fetch!(opts, :troupe_name)
    tick = Keyword.fetch!(opts, :tick_ms)
    Process.send_after(self(), :troupe_cue_scan, tick)
    {:ok, %{troupe_name: troupe_name, tick_ms: tick}}
  end

  @impl true
  def handle_info(:troupe_cue_scan, s) do
    Process.send_after(self(), :troupe_cue_scan, s.tick_ms)
    troupe = Troupe.by_name(s.troupe_name)
    scan(troupe)
    {:noreply, s}
  end

  defp scan(nil), do: :ok

  defp scan(troupe) do
    ctx = troupe.cue_store
    now = Time.now_ms()

    for rec <- Troupe.CueStore.due(ctx, now, 100) do
      with {:ok, claimed_sv} <-
             Troupe.CueStore.claim(ctx, rec.id, {node(), self()}, troupe.config.reminder_lease_ms) do
        deliver(troupe, rec, claimed_sv)
      end
    end

    :ok
  end

  defp deliver(troupe, rec, claimed_sv) do
    agent = Agent.build(troupe.name, elem(rec.key, 1), elem(rec.key, 2))

    result =
      try do
        Troupe.internal_call(troupe, agent, :on_cue, [rec.name, rec.payload])
      rescue
        _e -> :error
      catch
        :exit, _ -> :error
      end

    next_due =
      case result do
        {:ok, _} ->
          case rec.spec do
            {:at, _ts} -> :done
            {:every, ms} -> rec.next_due + ms
          end

        _ ->
          # 不确认：租约到期后重新认领、重复投递（至少一次）
          nil
      end

    if next_due do
      Troupe.CueStore.complete(troupe.cue_store, rec.id, claimed_sv, next_due)
    end
  end
end

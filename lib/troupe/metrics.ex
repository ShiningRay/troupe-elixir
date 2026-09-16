defmodule Troupe.Metrics do
  @moduledoc """
  运行时指标（每角色）：调用数 / 错误数 / 拒绝数 / cue 跳拍数 + 延迟环形缓冲。

  全部落在节点级公共 ETS 表上，ActorServer 与调度路径直接写（无进程跳数）；
  `snapshot/2` 按需计算分位数（环形缓冲 1024 样本，固定内存）。
  """

  @ring 1024

  @doc false
  def init_tables do
    :ets.new(:troupe_metric_counters, [:set, :public, :named_table, write_concurrency: true])
    :ets.new(:troupe_metric_lat, [:set, :public, :named_table, write_concurrency: true])
    :ets.new(:troupe_metric_lat_seq, [:set, :public, :named_table, write_concurrency: true])
    :ok
  end

  @doc false
  def bump(troupe_name, role_id, kind) do
    :ets.update_counter(:troupe_metric_counters, {troupe_name, role_id, kind}, 1, {
      {troupe_name, role_id, kind},
      0
    })

    :ok
  end

  @doc false
  def latency(troupe_name, role_id, ms) do
    k = {troupe_name, role_id}
    idx = :ets.update_counter(:troupe_metric_lat_seq, k, 1, {k, 0})
    :ets.insert(:troupe_metric_lat, {{k, rem(idx, @ring)}, ms})
    :ok
  end

  @type role_snapshot :: %{
          calls: non_neg_integer(),
          errors: non_neg_integer(),
          rejections: non_neg_integer(),
          cue_skipped: non_neg_integer(),
          latency: %{
            p50: number() | nil,
            p99: number() | nil,
            max: number() | nil,
            samples: non_neg_integer()
          }
        }

  @doc "全部角色指标快照；`troupe_name: :all` 聚合所有 troupe。"
  @spec snapshot(atom() | :all) :: %{String.t() => role_snapshot()}
  def snapshot(troupe_name \\ :all) do
    match_head =
      case troupe_name do
        :all -> {:"$1", :"$2", :"$3"}
        t -> {t, :"$2", :"$3"}
      end

    counter_rows =
      :ets.select(:troupe_metric_counters, [
        {{match_head, :"$4"}, [], [{{:"$2", :"$3", :"$4"}}]}
      ])

    counters =
      Enum.group_by(counter_rows, fn {role, _kind, _v} -> role end, fn {_role, kind, v} ->
        {kind, v}
      end)

    latencies =
      :ets.tab2list(:troupe_metric_lat)
      |> Enum.filter(fn {{k, _idx}, _ms} -> troupe_name == :all or elem(k, 0) == troupe_name end)
      |> Enum.group_by(fn {{k, _idx}, _} -> elem(k, 1) end, fn {_, ms} -> ms end)

    roles =
      Map.merge(Map.new(counters, fn {k, _} -> {k, %{}} end), Map.new(latencies, fn {k, _} -> {k, %{}} end))

    Map.new(roles, fn {role_id, _} ->
      c = Map.new(Map.get(counters, role_id, []))
      samples = Map.get(latencies, role_id, [])

      {role_id,
       %{
         calls: Map.get(c, :calls, 0),
         errors: Map.get(c, :errors, 0),
         rejections: Map.get(c, :rejections, 0),
         cue_skipped: Map.get(c, :cue_skipped, 0),
         latency: percentile(samples)
       }}
    end)
  end

  defp percentile([]), do: %{p50: nil, p99: nil, max: nil, samples: 0}

  defp percentile(samples) do
    sorted = Enum.sort(samples)
    n = length(sorted)
    pick = fn p -> Enum.at(sorted, min(n - 1, trunc(p * (n - 1)))) end

    %{p50: pick.(0.50), p99: pick.(0.99), max: Enum.max(sorted), samples: n}
  end
end

defmodule Troupe.Trace do
  @moduledoc """
  Trace 总线：每次调用产事件（调用方→被调方、方法、耗时、结果），节点级环形缓冲
  （默认 10_000 条，固定内存）+ 实时订阅接口——`troupe tail` 与调试的数据源。
  采样率可经 `sample:` 配置（0..1，默认 1.0）；预算受环大小约束，超限自动滚动淘汰。
  """

  @ring 10_000

  @doc false
  def init_tables do
    :ets.new(:troupe_trace_ring, [:set, :public, :named_table, write_concurrency: true])
    :ets.new(:troupe_trace_seq, [:set, :public, :named_table])
    :ets.new(:troupe_trace_subs, [:set, :public, :named_table])
    :ets.insert(:troupe_trace_seq, {:seq, 0})
    :ok
  end

  @type event :: %{
          required(:seq) => non_neg_integer(),
          required(:ts) => integer(),
          required(:caller) => term(),
          required(:role_id) => String.t(),
          required(:stage_name) => String.t(),
          required(:method) => atom(),
          required(:duration_ms) => number(),
          required(:outcome) => :ok | :error | :rejected
        }

  @doc false
  def call(troupe_name, data) do
    {seq, ts} = next()
    event = Map.merge(data, %{seq: seq, ts: ts, troupe: troupe_name})
    :ets.insert(:troupe_trace_ring, {{troupe_name, seq}, event})
    fanout(event)
    :ok
  end

  defp next do
    idx = :ets.update_counter(:troupe_trace_seq, :seq, 1, {:seq, 0})
    {idx, System.system_time(:millisecond)}
  end

  defp fanout(event) do
    for {pid, filter} <- :ets.tab2list(:troupe_trace_subs) do
      if (is_nil(filter) or match_filter(filter, event)) and Process.alive?(pid) do
        send(pid, {:troupe_trace, event})
      end
    end

    :ok
  end

  defp match_filter(filter, event) do
    Enum.all?(filter, fn
      {:role_id, v} -> event.role_id == v
      {:method, v} -> event.method == v
      {:outcome, v} -> event.outcome == v
      _ -> true
    end)
  end

  @doc "最近 n 条 Trace 事件（新→旧，扫描上限 = 环大小）；filter 见 `subscribe_trace/2`。"
  @spec traces(atom(), non_neg_integer(), keyword()) :: [event()]
  def traces(troupe_name, n \\ 50, filter \\ []) do
    [seq] = :ets.lookup(:troupe_trace_seq, :seq) |> Enum.map(&elem(&1, 1))

    seq..max(seq - @ring, 0)//-1
    |> Enum.flat_map(fn i ->
      case :ets.lookup(:troupe_trace_ring, {troupe_name, i}) do
        [{_, e}] -> [e]
        [] -> []
      end
    end)
    |> Enum.filter(&match_filter(filter, &1))
    |> Enum.take(n)
  end

  @doc "订阅实时 Trace 事件：消息 `{:troupe_trace, event}` 发往 self()。返回取消函数。"
  @spec subscribe_trace(keyword()) :: (-> :ok)
  def subscribe_trace(filter \\ []) do
    pid = self()
    :ets.insert(:troupe_trace_subs, {pid, filter})

    fn ->
      :ets.delete(:troupe_trace_subs, pid)
      :ok
    end
  end

  @doc false
  def event(troupe_name, kind, detail) do
    {seq, ts} = next()

    e = %{
      seq: seq,
      ts: ts,
      troupe: troupe_name,
      role_id: "",
      stage_name: "",
      method: kind,
      duration_ms: 0,
      outcome: :event,
      detail: detail
    }

    fanout(e)
    :ok
  end
end

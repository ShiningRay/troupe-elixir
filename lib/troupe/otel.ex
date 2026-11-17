defmodule Troupe.OTel do
  @moduledoc """
  OTLP JSON v1 导出适配（对齐 trouperb `otel.rb` 与 troupe.cr `otel.cr`：环形缓冲/总线 + sink 回调）。

  **框架不做网络 IO**：spans/metrics 转换为 OTLP JSON（Protobuf JSON 映射）后经
  `sink.(kind, json)` 回调交付——推送至 collector 的传输（HTTP/文件/管道）由宿主实现。

  映射（与兄弟实现对齐）：

  * Trace 事件 → span：`traceparent`（事件可选字段，W3C 传播将来接入）解析出
    traceId/spanId 十六进制；无该字段用全零 id（OTLP 约定全零为无效 id，由 collector
    侧裁决）；outcome → status code（ok=1，其余=2 带 message）；
  * 角色指标 → monotonic Sum（aggregationTemporality=2 累计）+ 延迟分位 gauge；
  * service.name = `troupe.<troupe 名>`。

  JSON 编码零依赖：优先 OTP 27+ 内置 `:json`，无则回退用户侧 `Jason`。

  用法：

      stop = Troupe.OTel.attach_exporter(troupe, fn
        :trace, json -> File.write!("spans.json", json, [:append])
        :metrics, json -> File.write!("metrics.json", json, [:append])
      end)
      ...
      stop.()
  """

  require Logger

  @span_kind_internal 1
  @status_ok 1
  @status_error 2

  @scope_name "troupe-elixir"

  # -- Trace 事件 → OTLP spans -------------------------------------------------

  @doc "Trace 事件列表 → OTLP JSON 字符串（单 resource 单 scope 的 spans）。"
  @spec spans_to_otlp_json(String.t(), [map()]) :: String.t()
  def spans_to_otlp_json(service_name, events) when is_list(events) do
    spans =
      for ev <- events do
        {trace_id, span_id} = parse_traceparent(Map.get(ev, :traceparent))
        ts_ms = Map.get(ev, :ts) || System.system_time(:millisecond)
        dur_ms = Map.get(ev, :duration_ms) || 0
        end_ns = ts_ms * 1_000_000
        start_ns = max(end_ns - dur_ms * 1_000_000, 0)
        outcome = to_string(Map.get(ev, :outcome, :ok))
        role = Map.get(ev, :role_id) || ""
        method = Map.get(ev, :method)

        %{
          "traceId" => trace_id,
          "spanId" => span_id,
          "parentSpanId" => "",
          "name" => "#{role}.#{method}",
          "kind" => @span_kind_internal,
          "startTimeUnixNano" => Integer.to_string(start_ns),
          "endTimeUnixNano" => Integer.to_string(end_ns),
          "attributes" =>
            Enum.reject(
              [
                attr("troupe.stage_name", Map.get(ev, :stage_name)),
                attr("troupe.from", inspect(Map.get(ev, :caller, :client), limit: 10)),
                attr("troupe.seq", Map.get(ev, :seq))
              ],
              &is_nil/1
            ),
          "status" =>
            if(outcome == "ok",
              do: %{"code" => @status_ok},
              else: %{"code" => @status_error, "message" => outcome}
            )
        }
      end

    json_encode(%{
      "resourceSpans" => [
        %{
          "resource" => %{"attributes" => [attr("service.name", service_name)]},
          "scopeSpans" => [%{"scope" => %{"name" => @scope_name}, "spans" => spans}]
        }
      ]
    })
  end

  # traceparent（W3C）→ {trace_id, span_id}；非法/缺失 → 全零（OTLP 全零为无效 id 约定）
  defp parse_traceparent(tp) when is_binary(tp) do
    case Regex.run(~r/\A00-([0-9a-f]{32})-([0-9a-f]{16})-01\z/, tp, capture: :all_but_first) do
      [trace_id, span_id] -> {trace_id, span_id}
      _ -> zero_ids()
    end
  end

  defp parse_traceparent(_), do: zero_ids()

  defp zero_ids, do: {String.duplicate("0", 32), String.duplicate("0", 16)}

  # -- 指标快照 → OTLP metrics --------------------------------------------------

  @doc "角色指标快照 → OTLP JSON 字符串（单 resource 单 scope；counter → monotonic Sum，延迟 → gauge）。"
  @spec metrics_to_otlp_json(String.t(), map()) :: String.t()
  def metrics_to_otlp_json(service_name, snapshot) when is_map(snapshot) do
    time_ns = Integer.to_string(System.system_time(:millisecond) * 1_000_000)

    metrics =
      for {role, m} <- snapshot, is_map(m), role_metric = role_metrics(role, m, time_ns) do
        role_metric
      end
      |> List.flatten()

    json_encode(%{
      "resourceMetrics" => [
        %{
          "resource" => %{"attributes" => [attr("service.name", service_name)]},
          "scopeMetrics" => [%{"scope" => %{"name" => @scope_name}, "metrics" => metrics}]
        }
      ]
    })
  end

  defp role_metrics(role, m, time_ns) do
    counters = [
      counter_metric("troupe.#{role}.calls", Map.get(m, :calls, 0), time_ns),
      counter_metric("troupe.#{role}.errors", Map.get(m, :errors, 0), time_ns),
      counter_metric("troupe.#{role}.rejections", Map.get(m, :rejections, 0), time_ns),
      counter_metric("troupe.#{role}.cue_skipped", Map.get(m, :cue_skipped, 0), time_ns)
    ]

    lat = Map.get(m, :latency) || %{}

    gauges =
      for {key, name} <- [p50: "p50", p99: "p99", max: "max"],
          v = Map.get(lat, key),
          is_number(v) do
        gauge_metric("troupe.#{role}.latency.#{name}", v, time_ns)
      end

    counters ++ gauges
  end

  defp counter_metric(name, value, time_ns) do
    %{
      "name" => name,
      "sum" => %{
        "dataPoints" => [%{"asInt" => value, "timeUnixNano" => time_ns}],
        "aggregationTemporality" => 2,
        "isMonotonic" => true
      }
    }
  end

  defp gauge_metric(name, value, time_ns) do
    %{
      "name" => name,
      "gauge" => %{
        "dataPoints" => [%{"asDouble" => value * 1.0, "timeUnixNano" => time_ns}]
      }
    }
  end

  defp attr(key, value) when is_binary(value) and value != "",
    do: %{"key" => key, "value" => %{"stringValue" => value}}

  defp attr(key, value) when is_integer(value),
    do: %{"key" => key, "value" => %{"intValue" => value}}

  defp attr(key, value) when is_binary(value),
    do: %{"key" => key, "value" => %{"stringValue" => value}}

  defp attr(_key, nil), do: nil

  # -- sink 接线 ---------------------------------------------------------------

  @doc """
  sink 接线：订阅节点 trace 总线（每事件 → :trace OTLP JSON）+ 周期推送指标快照
  （:metrics OTLP JSON）。传输由 sink 宿主实现（框架不做网络 IO）。

  返回 stop 函数（停机时调用；未处理完的 trace 事件丢弃——尽力语义与 Show 一致）。
  """
  @spec attach_exporter(Troupe.t() | atom(), (atom(), String.t() -> term()),
          every_ms: non_neg_integer()
        ) :: (-> :ok)
  def attach_exporter(troupe_or_name, sink, opts \\ []) do
    name =
      case troupe_or_name do
        %Troupe{name: n} when is_atom(n) -> n
        n when is_atom(n) -> n
      end

    every_ms = Keyword.get(opts, :every_ms, 10_000)
    service = "troupe.#{name}"
    pid = spawn(fn -> loop(service, name, sink, every_ms) end)

    fn ->
      # 置停标志：exporter 取到消息后先查标志再交付——stop 后积压事件静默丢弃
      :persistent_term.put({__MODULE__, :stop, name}, true)
      send(pid, :troupe_otel_stop)
      :ok
    end
  end

  defp stopped?(name), do: :persistent_term.get({__MODULE__, :stop, name}, false)

  defp loop(service, name, sink, every_ms) do
    unsubscribe = Troupe.subscribe_trace()
    loop_(service, name, sink, every_ms, unsubscribe)
  end

  defp loop_(service, name, sink, every_ms, unsubscribe) do
    cond do
      stopped?(name) ->
        unsubscribe.()
        :ok

      true ->
        receive do
          :troupe_otel_stop ->
            unsubscribe.()
            :ok

          {:troupe_trace, event} ->
            # 只导出本 troupe 的事件（节点总线跨 troupe 共享；service.name 不得混标）
            if not stopped?(name) and event.troupe == name do
              deliver(sink, :trace, spans_to_otlp_json(service, [Map.delete(event, :troupe)]))
            end

            loop_(service, name, sink, every_ms, unsubscribe)
        after
          every_ms ->
            deliver(sink, :metrics, metrics_to_otlp_json(service, Troupe.Metrics.snapshot(name)))
            loop_(service, name, sink, every_ms, unsubscribe)
        end
    end
  end

  defp deliver(sink, kind, json) do
    sink.(kind, json)
  rescue
    e ->
      Logger.warning("OTel sink #{kind} 交付失败：#{Exception.format(:error, e)}")
  end

  # -- JSON 编码（零依赖：OTP 27+ :json 优先，回退用户侧 Jason） -----------------

  defp json_encode(term) do
    cond do
      Code.ensure_loaded?(:json) and function_exported?(:json, :encode, 1) ->
        :json.encode(term)

      Code.ensure_loaded?(Jason) ->
        Jason.encode!(term)

      true ->
        raise ArgumentError,
              "Troupe.OTel 需要 JSON 编码器：OTP 27+ 内置 :json，或用户应用提供 :jason " <>
                "（add {:jason, \"~> 1.4\"}）"
    end
  end
end

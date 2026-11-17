# OTel 导出适配验收（对齐 troupe.cr M6 otel_spec / trouperb otel_test）：
# Trace 事件 → OTLP span JSON 结构与状态映射；指标 → monotonic Sum + latency gauge；
# attach_exporter sink 闭环（trace 实时交付 + metrics 周期推送 + stop 干净退出）。
defmodule Troupe.OTelTest do
  use ExUnit.Case, async: true
  use Troupe.Case, actors: [Troupe.TestActors.Cart]

  defmodule Counter do
    use Troupe.Actor

    def initial_props, do: %{n: 0}
    def bump(props, by \\ 1), do: {:save, props.n + by, Map.update!(props, :n, &(&1 + by))}
  end

  defp decode!(json), do: Jason.decode!(json)

  # -- spans 映射 ---------------------------------------------------------------

  test "trace event → valid OTLP span (zero ids without traceparent, status mapping)" do
    ev = %{
      seq: 7,
      ts: 1_700_000_000_000,
      caller: :client,
      role_id: "Cart",
      stage_name: "ot-1",
      method: :add,
      duration_ms: 12,
      outcome: :ok
    }

    doc = decode!(Troupe.OTel.spans_to_otlp_json("troupe.default", [ev]))
    assert [rs] = doc["resourceSpans"]
    assert %{"value" => %{"stringValue" => "troupe.default"}} = rs["resource"]["attributes"] |> hd()
    assert [ss] = rs["scopeSpans"]
    assert ss["scope"]["name"] == "troupe-elixir"
    assert [span] = ss["spans"]

    assert span["name"] == "Cart.add"
    assert span["kind"] == 1
    assert span["traceId"] == String.duplicate("0", 32), "无 traceparent → 全零 traceId"
    assert span["spanId"] == String.duplicate("0", 16)
    assert span["startTimeUnixNano"] == "1699999999988000000", "start = end - duration"
    assert span["endTimeUnixNano"] == "1700000000000000000"
    assert span["status"] == %{"code" => 1}, "outcome ok → STATUS_OK"

    assert Enum.any?(span["attributes"], &(&1["key"] == "troupe.stage_name"))
    assert Enum.any?(span["attributes"], &(&1["key"] == "troupe.seq"))
  end

  test "traceparent parsed; error outcome maps to STATUS_ERROR with message" do
    ev = %{
      seq: 8,
      ts: 1_700_000_000_000,
      role_id: "Cart",
      method: :boom,
      duration_ms: 5,
      outcome: :error,
      traceparent: "00-abcdefabcdefabcdefabcdefabcdefab-0123456789abcdef-01"
    }

    doc = decode!(Troupe.OTel.spans_to_otlp_json("svc", [ev]))
    [span] = (hd(doc["resourceSpans"])["scopeSpans"] |> hd())["spans"]
    assert span["traceId"] == "abcdefabcdefabcdefabcdefabcdefab"
    assert span["spanId"] == "0123456789abcdef"
    assert span["status"] == %{"code" => 2, "message" => "error"}
  end

  # -- metrics 映射 -------------------------------------------------------------

  test "role metrics → monotonic sums + latency gauges" do
    snapshot = %{
      "Cart" => %{
        calls: 10,
        errors: 2,
        rejections: 1,
        cue_skipped: 0,
        latency: %{p50: 3.5, p99: 42.0, max: 88, samples: 10}
      }
    }

    doc = decode!(Troupe.OTel.metrics_to_otlp_json("troupe.default", snapshot))
    assert [rm] = doc["resourceMetrics"]
    assert [sm] = rm["scopeMetrics"]
    metrics = sm["metrics"] |> Enum.map(&{&1["name"], &1}) |> Map.new()

    for name <- ~w(troupe.Cart.calls troupe.Cart.errors troupe.Cart.rejections troupe.Cart.cue_skipped) do
      m = Map.fetch!(metrics, name)
      assert m["sum"]["isMonotonic"] == true
      assert m["sum"]["aggregationTemporality"] == 2
      assert [dp] = m["sum"]["dataPoints"]
      assert is_integer(dp["asInt"])
    end

    assert metrics["troupe.Cart.calls"]["sum"]["dataPoints"] |> hd() |> Map.get("asInt") == 10

    for name <- ~w(troupe.Cart.latency.p50 troupe.Cart.latency.p99 troupe.Cart.latency.max) do
      m = Map.fetch!(metrics, name)
      assert m["gauge"]["dataPoints"] |> hd() |> Map.get("asDouble") >= 0
    end
  end

  # -- attach_exporter sink 闭环 -------------------------------------------------

  test "attach_exporter delivers spans via sink and stops cleanly", %{troupe: troupe} do
    sink_parent = self()

    sink = fn kind, json -> send(sink_parent, {:otel_sink, kind, json}) end

    stop = Troupe.OTel.attach_exporter(troupe, sink, every_ms: 200)

    cart = Troupe.cast(troupe, Troupe.TestActors.Cart, "otel-1")
    assert {:ok, :ok} = Troupe.call(cart, :add, ["x"])

    # 实时 trace 交付（等待至多 2s；尽力语义）
    assert_receive {:otel_sink, :trace, json}, 2_000
    doc = decode!(json)
    [span] = (hd(doc["resourceSpans"])["scopeSpans"] |> hd())["spans"]
    assert span["name"] == "Cart.add"

    # 周期指标推送
    assert_receive {:otel_sink, :metrics, mjson}, 2_000
    mdoc = decode!(mjson)
    names =
      ((hd(mdoc["resourceMetrics"])["scopeMetrics"] |> hd())["metrics"])
      |> Enum.map(& &1["name"])
    assert "troupe.Cart.calls" in names

    # stop 后 sink 不再收到交付
    :ok = stop.()
    refute_receive {:otel_sink, _kind, _json}, 600
  end
end

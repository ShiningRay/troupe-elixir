defmodule Troupe.CuesShowTest do
  @moduledoc """
  Cue 与 Show 验收（Troupe.js PLAN M4/M7）：持久提醒下场后触发、至少一次投递、
  注销竞争、内存 Cue 跳拍、Show pub/sub 全链路。
  """

  use ExUnit.Case, async: true

  use Troupe.Case,
    actors: [
      Troupe.TestActors.Alarm,
      Troupe.TestActors.Metronome,
      Troupe.TestActors.LazyMetronome,
      Troupe.TestActors.Room
    ]

  test "持久提醒：Actor 下场后按时触发并重新登台（onCue 分派）", %{troupe: troupe} do
    alarm = Troupe.cast(troupe, Troupe.TestActors.Alarm, "rem-1")
    Troupe.TestSink.register("rem-1", self())

    assert :ok = Troupe.call!(alarm, :arm, [100, "ring"])
    assert :ok = troupe |> Troupe.drain_all()

    # 到期 →（不在场则）重新登台 → on_cue 作为一条 Call 执行
    eventually(
      fn ->
        match?({:ok, 1}, Troupe.call(alarm, :rings, []))
      end,
      5_000
    )

    # at 型一次性：完成后移除，不再重复触发
    eventually(
      fn ->
        {:ok, cues} = {:ok, Troupe.Director.cues(troupe.name)}
        cues == []
      end,
      3_000
    )

    Process.sleep(300)
    assert {:ok, 1} = Troupe.call(alarm, :rings, [])
  end

  test "注销竞争：cancel 在到期前生效则不投递", %{troupe: troupe} do
    alarm = Troupe.cast(troupe, Troupe.TestActors.Alarm, "rem-cancel")
    Troupe.TestSink.register("rem-cancel", self())

    assert :ok = Troupe.call!(alarm, :arm, [150, "boom"])
    assert :ok = Troupe.call!(alarm, :disarm, ["boom"])

    eventually(fn -> Troupe.Director.cues(troupe.name) == [] end, 3_000)

    Process.sleep(400)
    assert {:ok, 0} = Troupe.call(alarm, :rings, [])
    refute_receive {:troupe_test_event, "rem-cancel", :ring}
  end

  test "内存 Cue：周期执行", %{troupe: troupe} do
    m = Troupe.cast(troupe, Troupe.TestActors.Metronome, "metro-1")
    Troupe.TestSink.register("metro-1", self())

    # 触发激活（激活即挂 Cue）
    assert {:ok, 0} = Troupe.call(m, :ticks, [])

    eventually(
      fn ->
        match?({:ok, n} when n >= 3, Troupe.call(m, :ticks, []))
      end,
      5_000
    )
  end

  test "cueSkipped：上一拍未完成而跳拍（周期任务变慢的首要信号）" do
    {:ok, troupe} = Troupe.Rehearsal.form(actors: [Troupe.TestActors.LazyMetronome])
    m = Troupe.cast(troupe, Troupe.TestActors.LazyMetronome, "lazy-1")

    # 激活：Cue 每 40ms，slow 执行 120ms → 必然跳拍
    assert {:ok, :ok} = Troupe.call(m, :slow, [])
    # slow 阻塞期间已经历若干拍；等它完成后观察跳拍计数
    eventually(
      fn ->
        metrics = Troupe.metrics(troupe)["LazyMetronome"]
        metrics != nil and metrics.cue_skipped >= 1
      end,
      5_000
    )

    Troupe.shutdown(troupe, grace: 1_000)
  end

  test "Show：Actor 订阅 on_stage effects；发布经内部 Call 投递（严格串行）" do
    {:ok, troupe} = Troupe.Rehearsal.form(actors: [Troupe.TestActors.Room], show: true)
    Troupe.TestSink.register("room-1", self())
    room = Troupe.cast(troupe, Troupe.TestActors.Room, "room-1")

    # 激活即订阅 "room:1"
    assert {:ok, []} = Troupe.call(room, :received, [])

    # 发布
    assert 1 = Troupe.publish(troupe, "room:1", "hello")

    eventually(
      fn ->
        match?({:ok, ["hello"]}, Troupe.call(room, :received, []))
      end,
      5_000
    )

    assert_receive {:troupe_test_event, "room-1", {:show, "hello"}}, 3_000

    # Actor 内发布（effect）
    assert :ok = Troupe.call!(room, :say, ["hi"])

    eventually(
      fn ->
        match?({:ok, ["hello", "hi"]}, Troupe.call(room, :received, []))
      end,
      5_000
    )

    Troupe.shutdown(troupe, grace: 1_000)
  end

  test "Show：外部 Audience（{m, f, args} 代理）实时收到事件" do
    {:ok, troupe} = Troupe.Rehearsal.form(actors: [Troupe.TestActors.Room], show: true)

    un = Troupe.subscribe_mf(troupe, "room:1", {Troupe.TestTap, :relay, [self(), :audience_got]})

    assert 1 = Troupe.publish(troupe, "room:1", "evt")

    assert_receive {:audience_got, "evt"}, 3_000
    assert :ok = un.()
    assert 0 = Troupe.publish(troupe, "room:1", "gone")
    Troupe.shutdown(troupe, grace: 1_000)
  end

  test "Show 未启用时 publish 显式报错" do
    {:ok, troupe} = Troupe.Rehearsal.form(actors: [Troupe.TestActors.Room])

    assert_raise RuntimeError, ~r/show is not enabled/, fn ->
      Troupe.publish(troupe, "room:1", "x")
    end

    Troupe.shutdown(troupe, grace: 1_000)
  end
end

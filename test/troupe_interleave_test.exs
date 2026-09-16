defmodule Troupe.InterleaveTest do
  @moduledoc """
  交错执行验收（Troupe.js PLAN M5，DESIGN §5.4）：
  组合矩阵（RO×RO 并行 / improv×annotated 并行 / strict 独占）、写饥饿防护。
  """

  use ExUnit.Case, async: true
  use Troupe.Case, actors: [Troupe.TestActors.ImprovStage]

  test "read_only × read_only 并行执行（不占串行通道）", %{troupe: troupe} do
    stage = Troupe.cast(troupe, Troupe.TestActors.ImprovStage, "ro-ro")

    t0 = System.monotonic_time(:millisecond)

    tasks =
      for _ <- 1..3 do
        Task.async(fn -> Troupe.call(stage, :ro_wait, [150]) end)
      end

    results = Task.await_many(tasks, 10_000)
    elapsed = System.monotonic_time(:millisecond) - t0

    # 串行需 ~450ms；并行应远小于
    assert elapsed < 350
    assert Enum.all?(results, &match?({:ok, {:ro, 0}}, &1))
  end

  test "组合矩阵：improv 与 annotated 交错；strict 仅在飞为 0 时启动", %{troupe: troupe} do
    Troupe.TestSink.register("matrix-1", self())
    stage = Troupe.cast(troupe, Troupe.TestActors.ImprovStage, "matrix-1")

    t_improv = Task.async(fn -> Troupe.call(stage, :improv_wait, [150]) end)
    Process.sleep(20)
    t_ro = Task.async(fn -> Troupe.call(stage, :ro_wait, [80]) end)

    # 与在飞 improv 交错：80ms 的 RO 不必等 150ms 的 improv 结束
    assert {:ok, {:ro, 0}} = Task.await(t_ro, 10_000)
    assert {:ok, :ok} = Task.await(t_improv, 10_000)

    # RO 在 improv 仍在执行期间启动
    events = collect_events("matrix-1", [])
    assert events |> Enum.map(&elem(&1, 1)) == [:improv_start, :ro_start, :improv_done]
  end

  test "写饥饿防护：strict 入队后，不再启动新 annotated Turn（strict FIFO）", %{troupe: troupe} do
    Troupe.TestSink.register("starve-1", self())
    stage = Troupe.cast(troupe, Troupe.TestActors.ImprovStage, "starve-1")

    # 1) improv 在飞（300ms）
    t1 = Task.async(fn -> Troupe.call(stage, :improv_wait, [300]) end)
    Process.sleep(50)

    # 2) strict 入队——在 improv settle 前不被放行，但入队后封锁新 annotated
    t2 = Task.async(fn -> Troupe.call(stage, :strict_mark, []) end)
    wait_pending_strict(troupe, "ImprovStage", "starve-1", 1)

    # 3) RO 排在 strict 之后——必须等 strict 执行完
    t3 = Task.async(fn -> Troupe.call(stage, :ro_wait, [10]) end)

    assert {:ok, :ok} = Task.await(t1, 10_000)
    assert {:ok, :ok} = Task.await(t2, 10_000)
    assert {:ok, {:ro, _}} = Task.await(t3, 10_000)

    order = collect_events("starve-1", []) |> Enum.map(&elem(&1, 1))
    assert order == [:improv_start, :improv_done, :strict_start, :ro_start]
  end

  test "improv 提交按完成序应用（整 Props 覆盖语义，跨步骤一致性归 strict）", %{troupe: troupe} do
    stage = Troupe.cast(troupe, Troupe.TestActors.ImprovStage, "commit-1")

    t1 = Task.async(fn -> Troupe.call(stage, :improv_wait, [60]) end)
    Process.sleep(5)
    t2 = Task.async(fn -> Troupe.call(stage, :improv_wait, [60]) end)

    assert {:ok, :ok} = Task.await(t1, 10_000)
    assert {:ok, :ok} = Task.await(t2, 10_000)

    # 两个 improv 并发提交，last-commit-wins：props_state 至少 +1，不承诺 +2（文档语义）
    assert {:ok, n} = Troupe.call(stage, :state, [])
    assert n >= 1
  end

  # 收集本 stage 的事件（清空已收的无关事件）
  defp collect_events(stage, acc) do
    receive do
      {:troupe_test_event, ^stage, tag} ->
        if is_tuple(tag) or is_atom(tag) do
          collect_events(stage, acc ++ [{stage, tag}])
        else
          collect_events(stage, acc)
        end

      {:troupe_test_event, _other, _} ->
        collect_events(stage, acc)
    after
      0 -> acc
    end
  end

  defp wait_pending_strict(troupe, role, stage, want) do
    Troupe.Case.eventually(
      fn ->
        case Troupe.Names.find_actor(troupe.name, role, stage) do
          nil ->
            false

          pid ->
            st = GenServer.call(pid, :troupe_state, 2_000)
            st.pending_strict == want
        end
      end,
      3_000
    )
  end
end

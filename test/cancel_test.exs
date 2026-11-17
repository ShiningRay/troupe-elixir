# 协作取消验收（对齐 trouperb cancel_test / troupe.cr CancelSignal，Issue #7 语义）：
# advisory 置位 → Turn 内 cancel_requested? 观察 → 方法自行提前收束（部分提交）；
# 不可强行终止（无标志时行为不变）；settle 后标志自动清除。
defmodule Troupe.CancelTest do
  use ExUnit.Case, async: true
  use Troupe.Case, actors: [Troupe.TestActors.CancellableWorker]

  test "advisory cancel observed inside Turn → early settle with partial commit", %{troupe: troupe} do
    worker = Troupe.cast(troupe, Troupe.TestActors.CancellableWorker, "c-1")

    # 在飞长 Turn（每步观察取消标志，置位即提前收束）
    task = Task.async(fn -> Troupe.call(worker, :churn, [100]) end)
    Process.sleep(150)

    assert :ok = Troupe.cancel(worker)
    assert {:ok, steps} = Task.await(task, 10_000)

    # 提前收束：远少于 100 步；部分进度已随 {:save} 提交
    assert steps in 1..99
    assert {:ok, ^steps} = Troupe.call(worker, :progress, [])
  end

  test "no cancel flag → full run, behavior unchanged", %{troupe: troupe} do
    worker = Troupe.cast(troupe, Troupe.TestActors.CancellableWorker, "c-2")
    assert {:ok, 20} = Troupe.call(worker, :churn, [20])
    assert {:ok, false} = Troupe.call(worker, :cancel_probe, [])
  end

  test "flag auto-clears after Turn settle", %{troupe: troupe} do
    worker = Troupe.cast(troupe, Troupe.TestActors.CancellableWorker, "c-3")

    task = Task.async(fn -> Troupe.call(worker, :churn, [100]) end)
    Process.sleep(150)
    assert :ok = Troupe.cancel(worker)
    assert {:ok, _steps} = Task.await(task, 10_000)

    # settle 后标志已清（finish_turn 清除）：下一 Turn 观察为 false
    assert {:ok, false} = Troupe.call(worker, :cancel_probe, [])
  end

  test "cancel before any Turn still effective for next Turn", %{troupe: troupe} do
    worker = Troupe.cast(troupe, Troupe.TestActors.CancellableWorker, "c-4")
    assert :ok = Troupe.cancel(worker)
    assert {:ok, 1} = Troupe.call(worker, :churn_once, [])
  end
end

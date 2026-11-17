defmodule Troupe.CoreTest do
  use ExUnit.Case, async: true

  use Troupe.Case,
    actors: [
      Troupe.TestActors.Cart,
      Troupe.TestActors.Sequencer,
      Troupe.TestActors.Butterfly,
      Troupe.TestActors.Fragile,
      Troupe.TestActors.Narrow
    ]

  @moduledoc """
  核心验收（Troupe.js PLAN M1/M2 横切场景）：DESIGN §3.1 最小示例、激活单飞、
  严格串行、执行占用、排队过期、业务异常、白名单、背压、Director 内省。
  """

  alias Troupe.{CallRejectedError, CallTimeoutError, UnknownOutcomeError}

  test "DESIGN §3.1 Cart 最小示例真实运行", %{troupe: troupe} do
    cart = Troupe.cast(troupe, Troupe.TestActors.Cart, "user-123")
    assert {:ok, :ok} = Troupe.call(cart, :add, ["apple"])
    assert {:ok, 1} = Troupe.call(cart, :peek, [])
    assert {:ok, :ok} = Troupe.call(cart, :add, ["pear"])
    assert {:ok, 2} = Troupe.call(cart, :peek, [])
  end

  test "cast 未注册角色 fail-fast 并列出 Repertoire", %{troupe: troupe} do
    assert_raise Troupe.UnknownRoleError, ~r/repertoire/i, fn ->
      Troupe.cast(troupe, NoSuchActor, "x")
    end
  end

  test "并发 100 个 add 无交错（串行性验证）", %{troupe: troupe} do
    seq = Troupe.cast(troupe, Troupe.TestActors.Sequencer, "serial-1")

    tasks =
      for i <- 1..100 do
        Task.async(fn -> Troupe.call(seq, :push, [i]) end)
      end

    Task.await_many(tasks, 20_000)
    assert {:ok, 100} = Troupe.call(seq, :count, [])
  end

  test "并发首调只激活一次（single-flight）", %{troupe: troupe} do
    Troupe.TestSink.register("sf-1", self())
    b = Troupe.cast(troupe, Troupe.TestActors.Butterfly, "sf-1")

    tasks = for _ <- 1..20, do: Task.async(fn -> Troupe.call(b, :passes, []) end)
    results = Task.await_many(tasks, 15_000)

    assert Enum.all?(results, &(&1 == {:ok, 1}))
    # 只登台一次
    assert_receive {:troupe_test_event, "sf-1", :on_stage}, 2_000
    refute_receive {:troupe_test_event, "sf-1", :on_stage}
  end

  test "执行占用：调用方超时不放行旧 Turn（A 超时后 B 不与 A 交错）", %{troupe: troupe} do
    Troupe.TestSink.register("occ-1", self())
    seq = Troupe.cast(troupe, Troupe.TestActors.Sequencer, "occ-1")

    # A：1s 慢调用，调用方 50ms 就放弃（+500ms 宽限）→ 结果未知
    t_a = Task.async(fn -> Troupe.call(seq, :slow, [1_000], deadline: 50) end)
    Process.sleep(20)

    # B：紧随其后——必须等 A settle 完才开始
    t_b = Task.async(fn -> Troupe.call(seq, :mark, [:b]) end)

    assert {:error, %UnknownOutcomeError{}} = Task.await(t_a, 15_000)
    assert {:ok, :ok} = Task.await(t_b, 10_000)

    assert_receive {:troupe_test_event, "occ-1", :slow_start}, 1_000
    assert_receive {:troupe_test_event, "occ-1", :slow_done}, 1_000
    assert_receive {:troupe_test_event, "occ-1", {:mark, :b}}, 1_000
  end

  test "排队过期不执行：deadline 已过的排队 Call 零副作用拒绝", %{troupe: troupe} do
    Troupe.TestSink.register("qd-1", self())
    seq = Troupe.cast(troupe, Troupe.TestActors.Sequencer, "qd-1")

    t_slow = Task.async(fn -> Troupe.call(seq, :slow, [400]) end)
    Process.sleep(20)

    assert {:error, %CallTimeoutError{phase: :queued}} = Troupe.call(seq, :mark, [:expired], deadline: 50)

    assert {:ok, :ok} = Task.await(t_slow, 10_000)
    refute_receive {:troupe_test_event, "qd-1", {:mark, :expired}}
  end

  test "业务异常原样回传，不影响后续调用", %{troupe: troupe} do
    fragile = Troupe.cast(troupe, Troupe.TestActors.Fragile, "f-1")

    assert {:error, %RuntimeError{message: "kapow"}} = Troupe.call(fragile, :boom, ["kapow"])
    assert {:ok, :ok} = Troupe.call(fragile, :safe, [])
    assert :ok = Troupe.call!(fragile, :bump, [])
  end

  # 轮询 ActorServer 状态直到 pending_strict 达到期望值（消除任务调度时序竞态）
  defp wait_pending_strict(troupe, role, stage, want) do
    eventually(
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

  test "白名单：生命周期钩子不可经 RPC 调用；rpc: 收窄生效", %{troupe: troupe} do
    narrow = Troupe.cast(troupe, Troupe.TestActors.Narrow, "n-1")

    assert {:error, %CallRejectedError{reason: :whitelist}} = Troupe.call(narrow, :hidden, [])
    assert {:error, %CallRejectedError{reason: :whitelist}} = Troupe.call(narrow, :on_stage, [])
    assert {:ok, :yes} = Troupe.call(narrow, :visible, [])
  end

  test "白名单列出未知方法在 form() 阶段 fail-fast" do
    defmodule BadWhitelist do
      use Troupe.Actor, rpc: [:nope]
      def real(props), do: {:reply, :ok, props}
    end

    assert_raise ArgumentError, ~r/unknown method/, fn ->
      Troupe.Rehearsal.form(actors: [BadWhitelist])
    end
  end

  test "角色名冲突在 form() 阶段 fail-fast" do
    defmodule TwinNarrow do
      use Troupe.Actor, role_id: "Narrow"
      def other(props), do: {:reply, :ok, props}
    end

    # TwinNarrow 显式声明 role_id "Narrow"，与 TestActors.Narrow 冲突
    assert_raise Troupe.RoleConflictError, fn ->
      Troupe.Rehearsal.form(actors: [Troupe.TestActors.Narrow, TwinNarrow])
    end
  end

  test "背压：CallBoard 深度超限显式拒绝（retryable）" do
    # 场景：improv 在飞（异步 Turn）→ strict 排队 → pending 达到深度上限
    # → 后续调用被显式拒绝。
    # 说明：strict 在线执行期间，后续消息滞留在 BEAM 信箱（信箱即 CallBoard），
    # 深度上限作用于"已受理待调度"的显式队列——这是当前实现的边界，文档已注明。
    {:ok, small} = Troupe.Rehearsal.form(actors: [Troupe.TestActors.ImprovStage], queue_depth: 1)
    stage = Troupe.cast(small, Troupe.TestActors.ImprovStage, "bp-1")

    t_improv = Task.async(fn -> Troupe.call(stage, :improv_wait, [400]) end)
    Process.sleep(50)

    # strict 入队：写饥饿防护下排队（pending_n = 1 = 深度上限）
    t_strict = Task.async(fn -> Troupe.call(stage, :strict_mark, []) end)
    wait_pending_strict(small, "ImprovStage", "bp-1", 1)

    # 后续调用：pending 已满 → 显式拒绝
    assert {:error, %CallRejectedError{reason: :backpressure, retryable: true}} =
             Troupe.call(stage, :ro_wait, [5])

    assert {:ok, :ok} = Task.await(t_improv, 10_000)
    assert {:ok, :ok} = Task.await(t_strict, 10_000)
    Troupe.shutdown(small, grace: 1_000)
  end

  test "Trace 总线记录调用事件（含调用方与耗时）", %{troupe: troupe} do
    cart = Troupe.cast(troupe, Troupe.TestActors.Cart, "trace-1")
    Troupe.call!(cart, :add, ["x"])
    Troupe.call!(cart, :peek, [])

    # Trace 经消息驱动落环（异步），慢 runner 上立即读取可能缺尾——等待到齐（返回事件列表）
    wait_trace_events = fn want ->
      deadline = System.monotonic_time(:millisecond) + 3_000

      repeat = fn repeat ->
        events = troupe |> Troupe.traces(10, role_id: "Cart")

        cond do
          length(events) >= want ->
            events

          System.monotonic_time(:millisecond) >= deadline ->
            events

          true ->
            Process.sleep(50)
            repeat.(repeat)
        end
      end

      repeat.(repeat)
    end

    events = wait_trace_events.(2)
    assert length(events) >= 2, "trace events: #{length(events)} for troupe #{troupe.name}"
    # 最新在前
    assert %{method: :peek, outcome: :ok} = hd(events)
    assert Enum.any?(events, &(&1.method == :add and &1.outcome == :ok))
    assert is_number(hd(events).duration_ms)
  end

  test "Director 内省：ps / inspect / metrics", %{troupe: troupe} do
    cart = Troupe.cast(troupe, Troupe.TestActors.Cart, "dir-1")
    Troupe.call!(cart, :add, ["a"])

    actors = Troupe.Director.actors(troupe.name, [])
    assert Enum.any?(actors, &(&1.stage_name == "dir-1" and &1.role_id == "Cart"))

    detail = Troupe.Director.inspect(troupe.name, "Cart", "dir-1")
    assert detail.lifecycle == :running
    assert detail.props.items == ["a"]

    metrics = Troupe.metrics(troupe)
    assert metrics["Cart"].calls >= 1
  end

  test "off 强制下场 + Props 保留，再次调用重新登台", %{troupe: troupe} do
    cart = Troupe.cast(troupe, Troupe.TestActors.Cart, "off-1")
    Troupe.call!(cart, :add, ["keep"])

    assert :ok = Troupe.Director.off(troupe.name, "Cart", "off-1")
    eventually(fn -> Troupe.Names.find_actor(troupe.name, "Cart", "off-1") == nil end)

    assert {:ok, 1} = Troupe.call(cart, :peek, [])
  end
end

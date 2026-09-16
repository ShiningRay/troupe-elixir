defmodule Troupe.PersistenceTest do
  @moduledoc """
  持久化验收（Troupe.js PLAN M1/M2）：持久成功语义、保存失败即失效、
  CAS 冲突、schemaVersion 迁移、DETS 进程级持久、故障即重激活（destroy）。
  """

  use ExUnit.Case, async: true

  import Troupe.Case, only: [eventually: 1, eventually: 2]

  alias Troupe.TestActors

  # -- 可注入故障的测试存储（自定义 PropStore 契约演示） ------------------------

  defmodule FlakyStore do
    @behaviour Troupe.PropStore

    def init(_opts) do
      table =
        :ets.new(:"flaky_#{System.unique_integer([:positive])}", [:set, :public, :named_table])

      :ets.insert(table, {:fail_next_write, false})
      table
    end

    def arm_failure(table), do: :ets.insert(table, {:fail_next_write, true})

    @impl true
    def read(table, key) do
      case :ets.lookup(table, key) do
        [{^key, props, rev, sv}] -> {:ok, %{props: props, revision: rev, schema_version: sv}}
        [] -> {:ok, nil}
      end
    end

    @impl true
    def create(table, key, props, sv) do
      if :ets.insert_new(table, {key, props, 1, sv}) do
        {:ok, %{props: props, revision: 1, schema_version: sv}}
      else
        {:error, {:conflict, :exists}}
      end
    end

    @impl true
    def write(table, key, props, opts) do
      expected = Keyword.fetch!(opts, :expected_revision)

      if :ets.lookup_element(table, :fail_next_write, 2) do
        :ets.insert(table, {:fail_next_write, false})
        {:error, :injected_io_failure}
      else
        case :ets.lookup(table, key) do
          [{^key, _, ^expected, sv}] ->
            :ets.insert(table, {key, props, expected + 1, sv})
            {:ok, %{props: props, revision: expected + 1, schema_version: sv}}

          [{^key, _, other, _}] ->
            {:error, {:conflict, :revision, current: other}}

          [] ->
            {:error, {:conflict, :revision, current: nil}}
        end
      end
    end

    @impl true
    def delete(table, key) do
      :ets.delete(table, key)
      :ok
    end
  end

  test "持久成功：{:save, ..} 提交成功后才回复；最后已提交快照可恢复" do
    {:ok, troupe} = Troupe.Rehearsal.form(actors: [TestActors.Cart])
    cart = Troupe.cast(troupe, TestActors.Cart, "persist-1")

    assert {:ok, :ok} = Troupe.call(cart, :add, ["a"])

    # 模拟故障：直接击杀在场 Actor（kill -9 等价），下一条 Call 从 PropStore 重登台
    Troupe.destroy(troupe)
    eventually(fn -> Troupe.Names.find_actor(troupe.name, "Cart", "persist-1") == nil end)

    assert {:ok, 1} = Troupe.call(cart, :peek, [])
    Troupe.shutdown(troupe, grace: 1_000)
  end

  test "保存失败 → 实例失效 → 下次调用从最后已提交快照恢复（不基于未提交状态服务）" do
    {:ok, troupe} =
      Troupe.Rehearsal.form(actors: [TestActors.Cart], store: {FlakyStore, %{}})

    cart = Troupe.cast(troupe, TestActors.Cart, "flaky-1")
    assert {:ok, :ok} = Troupe.call(cart, :add, ["committed"])

    # 注入下一次写入失败（ctx 即 ETS 表）
    FlakyStore.arm_failure(elem(troupe.store, 1))
    assert {:error, %Troupe.StoreError{}} = Troupe.call(cart, :add, ["will-fail"])

    # 实例已失效：重新激活只看到最后已提交快照（"committed"，不含 "will-fail"）
    assert {:ok, 1} = Troupe.call(cart, :peek, [])
    Troupe.shutdown(troupe, grace: 1_000)
  end

  test "CAS 冲突（外部并发写入）→ ConflictError + 实例失效" do
    {:ok, troupe} = Troupe.Rehearsal.form(actors: [TestActors.Cart])
    cart = Troupe.cast(troupe, TestActors.Cart, "cas-1")
    assert {:ok, :ok} = Troupe.call(cart, :add, ["mine"])

    # 外部直接推进 revision（模拟另一 Stage 持有者写入）
    {store_mod, store_ctx} = troupe.store
    {:ok, stored} = store_mod.read(store_ctx, {"default", "Cart", "cas-1"})

    {:ok, _} =
      store_mod.write(store_ctx, {"default", "Cart", "cas-1"}, %{items: ["external"]},
        expected_revision: stored.revision,
        schema_version: 1
      )

    assert {:error, %Troupe.ConflictError{}} = Troupe.call(cart, :add, ["stale"])

    # 失效后从外部写入的新快照恢复
    assert {:ok, 1} = Troupe.call(cart, :peek, [])
    Troupe.shutdown(troupe, grace: 1_000)
  end

  test "schemaVersion 逐级迁移；迁移失败 = 激活失败" do
    {:ok, troupe} = Troupe.Rehearsal.form(actors: [TestActors.Migrator])

    # 直接写入 v1 状态
    {store_mod, store_ctx} = troupe.store
    {:ok, _} = store_mod.create(store_ctx, {"default", "Migrator", "mig-1"}, %{names: ["a", "b"]}, 1)

    Troupe.TestSink.register("mig-1", self())
    mig = Troupe.cast(troupe, TestActors.Migrator, "mig-1")

    assert {:ok, %{names: ["a", "b"], items: ["a", "b"]}} = Troupe.call(mig, :get, [])
    assert_receive {:troupe_test_event, "mig-1", :migrated}, 2_000

    # 迁移产物已落盘（schema_version = 2）
    {:ok, stored} = store_mod.read(store_ctx, {"default", "Migrator", "mig-1"})
    assert stored.schema_version == 2
    Troupe.shutdown(troupe, grace: 1_000)
  end

  test "File 存储：进程级持久（跨 troupe 重启恢复，同 props_dir）" do
    dir = Path.join(System.tmp_dir!(), "troupe-file-#{System.unique_integer([:positive])}")

    {:ok, t1} = Troupe.form(actors: [TestActors.Cart], store: :file, props_dir: dir, name: :file_a)
    cart = Troupe.cast(t1, TestActors.Cart, "dets-1")
    assert {:ok, :ok} = Troupe.call(cart, :add, ["durable"])
    Troupe.shutdown(t1, grace: 2_000)

    {:ok, t2} = Troupe.form(actors: [TestActors.Cart], store: :file, props_dir: dir, name: :file_b)
    assert {:ok, 1} = Troupe.call(Troupe.cast(t2, TestActors.Cart, "dets-1"), :peek, [])
    Troupe.shutdown(t2, grace: 2_000)

    File.rm_rf!(dir)
  end

  test "Intermission：空闲自动下场 + off_stage 钩子 + Props 归还 PropStore" do
    Troupe.TestSink.register("bf-1", self())
    {:ok, troupe} = Troupe.Rehearsal.form(actors: [TestActors.Butterfly])
    b = Troupe.cast(troupe, TestActors.Butterfly, "bf-1")

    assert :ok = Troupe.call!(b, :remember, ["x"])

    # 120ms 后空闲下场
    eventually(fn -> Troupe.Names.find_actor(troupe.name, "Butterfly", "bf-1") == nil end, 3_000)
    assert_receive {:troupe_test_event, "bf-1", :off_stage}, 3_000

    # 重新登台：恢复已提交 Props，on_stage 再次执行
    assert {:ok, ["x"]} = Troupe.call(b, :recall, [])
    assert {:ok, 2} = Troupe.call(b, :passes, [])
    Troupe.shutdown(troupe, grace: 1_000)
  end

  test "排空期间新请求：等待排空完成后重新激活（drain_policy: :wait）" do
    Troupe.TestSink.register("bf-2", self())
    {:ok, troupe} = Troupe.Rehearsal.form(actors: [TestActors.Butterfly])
    b = Troupe.cast(troupe, TestActors.Butterfly, "bf-2")
    assert :ok = Troupe.call!(b, :remember, ["y"])

    # 等 idle 边缘：等它下场，同时立刻发起调用（停泊 → 重新激活）
    eventually(fn -> Troupe.Names.find_actor(troupe.name, "Butterfly", "bf-2") == nil end, 3_000)
    t = Task.async(fn -> Troupe.call(b, :recall, []) end)
    assert {:ok, ["y"]} = Task.await(t, 5_000)
    assert_receive {:troupe_test_event, "bf-2", :on_stage}, 3_000
    Troupe.shutdown(troupe, grace: 1_000)
  end

  test "幂等：同键同指纹重放；同键异指纹拒绝" do
    {:ok, troupe} = Troupe.Rehearsal.form(actors: [TestActors.Ticket])
    t = Troupe.cast(troupe, TestActors.Ticket, "idem-1")

    assert {:ok, {:executed, {:confirmed, 1}}} = Troupe.call(t, :reserve, ["r1", 1])
    # 同键同参数：重放原结果（不再扣减）
    assert {:ok, {:replayed, {:confirmed, 1}}} = Troupe.call(t, :reserve, ["r1", 1])
    # 同键不同参数：拒绝
    assert {:error, %Troupe.IdempotencyConflictError{}} = Troupe.call(t, :reserve, ["r1", 2])
    # 新键正常
    assert {:ok, {:executed, {:confirmed, 0}}} = Troupe.call(t, :reserve, ["r2", 1])
    # 库存不足
    assert {:ok, {:executed, {:denied, 0}}} = Troupe.call(t, :reserve, ["r3", 1])
    Troupe.shutdown(troupe, grace: 1_000)
  end
end

# PostgreSQL Provider 验收（Issue #3 / ROADMAP Phase 1；仅 TROUPE_TEST_PG 时运行）。
#
# 验收链：CAS / fencing 拒绝 / epoch 单调 + 墓碑 / 旧持有者恢复被拒 /
# Props 跨"崩溃"恢复 / Cue claim-complete-重投（DB 时钟）。
# 跨节点 kill -9 级验收在 Troupe.DistributedTest（TROUPE_DIST_TEST leg）。
if System.get_env("TROUPE_TEST_PG") do
  defmodule Troupe.PgProviderTest do
    use ExUnit.Case, async: false

    @url System.get_env("TROUPE_TEST_PG")
    @lease_ms 2_000

    defmodule Counter do
      use Troupe.Actor

      def initial_props, do: %{"n" => 0}
      def bump(props, by \\ 1), do: {:save, props["n"] + by, Map.update!(props, "n", &(&1 + by))}
      def peek(props), do: {:reply, props["n"], props}
    end

    defmodule Migrator do
      use Troupe.Actor, schema_version: 2

      def initial_props, do: %{"names" => []}

      def migrate_props(props, 1), do: Map.update!(props, "names", &Enum.reverse/1)
      def peek(props), do: {:reply, props["names"], props}
    end

    defmodule Reminder do
      use Troupe.Actor, cues: [tick: [every: 200, name: "tick"]]

      def initial_props, do: %{"ticks" => 0}
      def on_cue(props, "tick", _payload), do: {:save, :ok, Map.update!(props, "ticks", &(&1 + 1))}
      def ticks(props), do: {:reply, props["ticks"], props}
    end

    setup do
      {:ok, conn} = Postgrex.start_link(hostname: "127.0.0.1", port: 55444, username: "troupe", password: "troupe", database: "troupe_rb_test")

      store = %{conn: conn, opts: []}
      Troupe.PropStore.Postgres.ensure_schema(store)

      # 本套件 namespace 前缀清理（跨运行残留；actor_key 为 JSON 数组文本）
      Postgrex.query!(conn, "DELETE FROM troupe_ex_props WHERE actor_key LIKE '[\"pgt%'", [])
      Postgrex.query!(conn, "DELETE FROM troupe_ex_ownership WHERE actor_key LIKE '[\"pgt%'", [])
      Postgrex.query!(conn, "DELETE FROM troupe_ex_cues WHERE namespace LIKE 'pgt%'", [])

      on_exit(fn -> Process.exit(conn, :kill) end)

      %{store: store, conn: conn}
    end

    # -- CAS 与 fencing 基础合同 -------------------------------------------------

    test "create conflict + CAS write + stale revision rejected", %{store: store} do
      key = {"pgt", "Counter", "cas-1"}

      assert {:ok, %{revision: 1}} = Troupe.PropStore.Postgres.create(store, key, %{"n" => 0}, 1, fencing_token: 1)
      assert {:error, {:conflict, :exists}} = Troupe.PropStore.Postgres.create(store, key, %{"n" => 9}, 1, [])

      # 带合法 fencing（ownership 行由 Ownership.Postgres 建立）
      own = %{conn: store.conn, lease_ms: @lease_ms}
      assert {:ok, epoch} = Troupe.Ownership.Postgres.acquire(own, key, "owner-a", @lease_ms)
      assert {:ok, %{revision: 2}} = Troupe.PropStore.Postgres.write(store, key, %{"n" => 1}, expected_revision: 1, schema_version: 1, fencing_token: epoch)

      # 旧 revision 拒绝
      assert {:error, {:conflict, :revision}} =
               Troupe.PropStore.Postgres.write(store, key, %{"n" => 99}, expected_revision: 1, schema_version: 1, fencing_token: epoch)

      assert {:ok, %{props: %{"n" => 1}}} = Troupe.PropStore.Postgres.read(store, key)
    end

    test "stale fencing token rejected even with matching revision", %{store: store} do
      key = {"pgt", "Counter", "fence-1"}
      own = %{conn: store.conn, lease_ms: @lease_ms}

      assert {:ok, e1} = Troupe.Ownership.Postgres.acquire(own, key, "owner-a", @lease_ms)
      assert {:ok, %{revision: 1}} = Troupe.PropStore.Postgres.create(store, key, %{"n" => 0}, 1, fencing_token: e1)

      # 接管：owner-b 取得 epoch 2
      Troupe.PropStore.Postgres.expire_ownership!(store, key)
      assert {:ok, e2} = Troupe.Ownership.Postgres.acquire(own, key, "owner-b", @lease_ms)
      assert e2 == e1 + 1, "epoch 单调递增"
      assert {:ok, %{revision: 2}} = Troupe.PropStore.Postgres.write(store, key, %{"n" => 5}, expected_revision: 1, schema_version: 1, fencing_token: e2)

      # 旧持有者恢复，以 epoch 1 + 暂存 revision 写 → fencing 拒绝（fencing 优先于 CAS 诊断）
      assert {:error, {:conflict, :fencing, token: ^e1}} =
               Troupe.PropStore.Postgres.write(store, key, %{"n" => 999_999}, expected_revision: 2, schema_version: 1, fencing_token: e1)

      # 现行 revision 未被旧 epoch 覆盖
      assert {:ok, %{props: %{"n" => 5}, revision: rev}} = Troupe.PropStore.Postgres.read(store, key)
      assert rev >= 2
    end

    test "ownership: busy while lease valid, re-entry keeps epoch, release leaves tombstone", %{store: store} do
      key = {"pgt", "Counter", "own-1"}
      own = %{conn: store.conn, lease_ms: @lease_ms}

      assert {:ok, e1} = Troupe.Ownership.Postgres.acquire(own, key, "owner-a", @lease_ms)
      assert {:error, :busy} = Troupe.Ownership.Postgres.acquire(own, key, "owner-b", @lease_ms)
      assert {:ok, ^e1} = Troupe.Ownership.Postgres.acquire(own, key, "owner-a", @lease_ms), "同 owner 重入不涨 epoch"

      assert :ok = Troupe.Ownership.Postgres.release(own, key, "owner-a", e1)

      # 墓碑：release 后行仍在，epoch 不回退
      assert {epoch, _owner, _lease} = Troupe.PropStore.Postgres.ownership_row(store, key)
      assert epoch == e1

      assert {:ok, e2} = Troupe.Ownership.Postgres.acquire(own, key, "owner-b", @lease_ms)
      assert e2 == e1 + 1, "release 后接管 epoch 单调 +1"
    end

    # -- 集成：troupe.form production 门禁放行 + 崩溃恢复 --------------------------

    test "production cluster form with PG store passes capability gate; actor survives crash", %{conn: conn} do
      ns = "pgt-form-#{System.unique_integer([:positive])}"

      {:ok, troupe} =
        Troupe.form(
          actors: [Counter],
          name: String.to_atom("pgt_#{System.unique_integer([:positive])}"),
          namespace: ns,
          store: {Troupe.PropStore.Postgres, [url: pg_url(), pool_size: 2]},
          ownership: [lease_ms: 1_000],
          cluster: [seeds: [], mode: :production],
          intermission: "1h"
        )

      cart = Troupe.cast(troupe, Counter, "survivor-1")
      assert {:ok, 3} = Troupe.call(cart, :bump, [3])

      # 模拟 Stage 崩溃（kill ActorServer，非优雅下场）→ 重激活从共享存储恢复
      {_role, stage} = {"Counter", "survivor-1"}

      pid =
        Enum.find_value(Troupe.Activator.which_children(troupe.name), fn pid ->
          state = :sys.get_state(pid)

          if state.stage_name == stage, do: pid
        end)

      assert pid, "actor process found"
      Process.exit(pid, :kill)

      # 等租约过期（防双活正确语义：崩溃后重激活需等 grace）
      Process.sleep(1_200)
      assert {:ok, 3} = Troupe.call(cart, :peek, []), "崩溃后从 PG 恢复 props"
      assert {:ok, 4} = Troupe.call(cart, :bump, [1])

      Troupe.shutdown(troupe)
    end

    test "fencing write on migrated adopt (schema upgrade under ownership)", %{store: store} do
      # 旧 schema 记录 + 旧 epoch 墓碑 → 新代码（schema_version 2）激活即迁移，携 fencing 写
      key = {"pgt", "Migrator", "mig-1"}
      own = %{conn: store.conn, lease_ms: @lease_ms}
      assert {:ok, e1} = Troupe.Ownership.Postgres.acquire(own, key, "old-owner", @lease_ms)
      assert {:ok, %{revision: 1}} = Troupe.PropStore.Postgres.create(store, key, %{"names" => ["a", "b"]}, 1, fencing_token: e1)
      Troupe.PropStore.Postgres.expire_ownership!(store, key)

      {:ok, troupe} =
        Troupe.form(
          actors: [Migrator],
          name: String.to_atom("pgt_mig_#{System.unique_integer([:positive])}"),
          namespace: "pgt",
          store: {Troupe.PropStore.Postgres, [url: pg_url(), pool_size: 2]},
          ownership: true,
          intermission: "1h"
        )

      m = Troupe.cast(troupe, Migrator, "mig-1")
      # adopt（v1 记录）→ migrate_props v1→v2（reverse names）→ 携 fencing 写回 PG
      assert {:ok, ["b", "a"]} = Troupe.call(m, :peek, []), "迁移后的 props 生效"

      stored = Troupe.PropStore.Postgres.read(store, key)
      assert match?({:ok, %{schema_version: 2}}, stored), "迁移产物已落盘（schema_version 2）"

      Troupe.shutdown(troupe)
    end

    # -- Cue：claim / complete / 租约过期重投（DB 时钟） ---------------------------

    test "cue claim/complete + lease expiry redelivery", %{store: store} do
      key = {"pgt", "Reminder", "cue-1"}
      ctx = Troupe.PropStore.Postgres.cue_ctx(store)

      assert :ok = Troupe.CueStore.upsert(ctx, key, "tick", %{"k" => 1}, {:every, 500}, 1_000)

      assert [rec] = Troupe.CueStore.due(ctx, 0, 10)
      assert rec.name == "tick"

      assert {:ok, sv} = Troupe.CueStore.claim(ctx, rec.id, "runner-a", @lease_ms)

      # 租约期内他人认领 → stolen
      assert {:error, :stolen} = Troupe.CueStore.claim(ctx, rec.id, "runner-b", @lease_ms)

      # 模拟执行中宕机：claim 后未 complete（next_due 不动），租约过期 → 重投（DB 时钟判定）
      expire_cue_lease(store, rec.id)
      Process.sleep(50)

      assert [rec2] = Troupe.CueStore.due(ctx, 0, 10), "running + 租约过期 → 重投（至少一次）"
      assert {:ok, sv2} = Troupe.CueStore.claim(ctx, rec2.id, "runner-c", @lease_ms)

      # complete 以 statusVersion CAS：推进到未来 → 不再到期
      future = System.system_time(:millisecond) + 60_000
      assert :ok = Troupe.CueStore.complete(ctx, rec2.id, sv2, future)
      assert [] = Troupe.CueStore.due(ctx, 0, 10), "未到期不再投递"

      # :done 移除：active 记录直接认领完成（claim 不受 next_due 拦截）
      assert {:ok, sv3} = Troupe.CueStore.claim(ctx, rec2.id, "runner-d", @lease_ms)
      assert :ok = Troupe.CueStore.complete(ctx, rec2.id, sv3, :done)
      assert Troupe.CueStore.Postgres.list(ctx) == [], "记录已移除"
    end

    defp expire_cue_lease(store, id) do
      Postgrex.query!(store.conn, "UPDATE troupe_ex_cues SET lease_until = to_timestamp(0) WHERE id = $1", [id])
      :ok
    end

    defp pg_url, do: @url
  end
end

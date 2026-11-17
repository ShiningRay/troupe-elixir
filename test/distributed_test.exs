# 真实双 BEAM 节点分布式验收（Issue #2 / ROADMAP Phase 0）。
#
# 仅在 TROUPE_DIST_TEST=1 时运行（CI 独立 leg / 本地 `TROUPE_DIST_TEST=1 mix test`）：
# 主测试节点 Node.start 后经 Port 启动子节点 worker（test/support/dist_worker.exs），
# 验收：透明调用跨节点路由、production 角色不兼容 fail-fast 断连、
# 节点死亡后 Actor 重激活与环自愈。
if System.get_env("TROUPE_DIST_TEST") do
  defmodule Troupe.DistributedTest do
    use ExUnit.Case, async: false

    @moduletag :distributed

    @cookie :troupe_dist_test
    @ready_timeout 60_000

    # 节点名用本机短主机名（`name@host` 显式 IP 形式在部分 Windows 组合上连 epmd
    # 会被拒；sname 缺省补 hostname 在双平台均验证可用）
    @hostname :inet.gethostname() |> elem(1) |> List.to_string()

    defp hostname, do: @hostname

    # -- 节点/进程管理 ---------------------------------------------------------

    defp ensure_distributed do
      unless Node.alive?() do
        ensure_epmd()

        case Node.start(String.to_atom("primary@#{@hostname}")) do
          {:ok, _} -> :ok
          {:error, {:already_started, _}} -> :ok
          {:error, reason} -> flunk("cannot start distributed node: #{inspect(reason)}")
        end
      end

      # 断开前次运行残留的孤儿 worker（自监护会在 nodedown 后停机）；等它们真正退出
      Enum.each(Node.list(), &Node.disconnect/1)

      drain_orphans(System.monotonic_time(:millisecond) + 8_000)

      :erlang.set_cookie(node(), @cookie)
      node()
    end

    defp drain_orphans(deadline) do
      if Node.list() == [] or System.monotonic_time(:millisecond) >= deadline do
        :ok
      else
        Enum.each(Node.list(), &Node.disconnect/1)
        Process.sleep(200)
        drain_orphans(deadline)
      end
    end

    defp ensure_epmd do
      # epmd 可能未运行（干净 CI runner）；OTP 安装目录内定位（Windows PATH 通常无 epmd）
      case Path.wildcard(Path.join([:code.root_dir(), "erts-*", "bin", "epmd*"])) do
        [exe | _] ->
          System.cmd(exe, ["-daemon"])
          Process.sleep(300)

        _ ->
          :ok
      end
    end

    defp spawn_worker(sname, env \\ %{}) do
      port_cmd =
        "elixir --sname #{sname} --cookie #{Atom.to_string(@cookie)} " <>
          "-S mix run test/support/dist_worker.exs"

      env_list =
        Enum.map(
          Map.merge(
            %{
              "MIX_ENV" => "test",
              "DIST_PRIMARY" => "primary@#{hostname()}",
              "DIST_TROUPE" => "dist_troupe",
              # worker 的 mix run 需与主进程同一 deps 解析（postgrex 由该变量启用）
              "TROUPE_TEST_PG" => System.get_env("TROUPE_TEST_PG") || ""
            },
            env
          ),
          fn {k, v} -> {String.to_charlist(k), String.to_charlist(v)} end
        )

      Port.open(
        {:spawn, String.to_charlist(port_cmd)},
        [:stream, :binary, :exit_status, :stderr_to_stdout, env: env_list]
      )
    end

    defp wait_ready(port, acc \\ "") do
      receive do
        {^port, {:data, data}} ->
          acc = acc <> data
          first_line = acc |> String.split("\n") |> Enum.find(&(&1 != "" and &1 != "\r"))

          cond do
            first_line == nil ->
              wait_ready(port, acc)

            String.contains?(first_line, "DIST_WORKER_READY") ->
              :ok

            String.contains?(first_line, "DIST_WORKER_ERROR") ->
              flunk("worker failed: #{first_line}")

            true ->
              wait_ready(port, acc)
          end

        {^port, {:exit_status, status}} ->
          flunk("worker exited (#{status}) before READY: #{inspect(String.slice(acc, -500, 500))}")
      after
        @ready_timeout ->
          flunk("worker not READY within #{@ready_timeout}ms: #{inspect(String.slice(acc, -500, 500))}")
      end
    end

    defp wait_connected(worker, timeout \\ 15_000), do: wait_until(timeout, fn -> node_connected?(worker) end)

    defp wait_disconnected(worker, timeout \\ 10_000),
      do: wait_until(timeout, fn -> not node_connected?(worker) end)

    defp node_connected?(worker), do: worker in Node.list()

    defp wait_until(timeout, pred, poll \\ 100) do
      deadline = now() + timeout
      do_wait_until(deadline, pred, poll)
    end

    defp do_wait_until(deadline, pred, poll) do
      if pred.() do
        :ok
      else
        if now() >= deadline, do: flunk("condition not met within deadline")
        Process.sleep(poll)
        do_wait_until(deadline, pred, poll)
      end
    end

    defp now, do: System.monotonic_time(:millisecond)

    defp stop_worker(worker) do
      if node_connected?(worker) do
        try do
          :erpc.call(worker, System, :stop, [])
        catch
          _, _ -> :ok
        end
      end

      wait_disconnected(worker, 5_000)
    end

    # 主节点上的 troupe（与 worker 同角色集；唯一名避免失败级联撞名）
    defp form_primary(opts \\ []) do
      default = [
        actors: [Troupe.DistWorker.Counter],
        name: String.to_atom("dist_troupe_#{System.unique_integer([:positive])}"),
        namespace: "default",
        intermission: "1h",
        store: :memory,
        ownership: true
      ]

      kw = Keyword.merge(default, opts)

      on_exit(fn ->
        case Troupe.by_name(kw[:name]) do
          nil -> :ok
          t -> Troupe.shutdown(t)
        end
      end)

      {:ok, troupe} = Troupe.form(kw)
      troupe
    end

    # -- 场景 1：透明调用跨节点路由 + 节点死亡重激活 ----------------------------

    @tag :distributed
    test "transparent call routes to worker node and survives node death via re-activation" do
      ensure_distributed()
      suffix = System.unique_integer([:positive])
      worker = :"worker1_#{suffix}@#{hostname()}"
      sname = "worker1_#{suffix}"

      # primary 先 form（seeds 空：worker 未起）；worker 随后以 primary 为 seed 入团，
      # 同名 troupe 是跨节点路由前提（Router.remote 在目标端 by_name!）
      troupe = form_primary(cluster: [seeds: [], mode: :dev])

      port =
        spawn_worker(sname, %{
          "DIST_ACTORS" => "cart",
          "DIST_SEEDS" => "primary@#{hostname()}",
          "DIST_MODE" => "dev",
          "DIST_TROUPE" => Atom.to_string(troupe.name)
        })

      wait_ready(port)
      wait_connected(worker)
      wait_ring_contains(troupe, worker)

      # 选一个环归属为 worker 的 stage_name（2 节点环上哈希两侧各半；池 20 个必覆盖）
      stage =
        Enum.find(Enum.map(1..20, &"c-#{&1}"), fn st ->
          Troupe.Playbill.owner(troupe.playbill, {troupe.config.namespace, "Counter", st}) == worker
        end)

      assert stage, "no stage_name hashes to worker (ring: #{inspect(Troupe.Playbill.ring_info(troupe.playbill)[:members])})"

      cart = Troupe.cast(troupe, Troupe.DistWorker.Counter, stage)
      assert {:ok, 1} = Troupe.call(cart, :bump, [])
      assert {:ok, 1} = Troupe.call(cart, :peek, [])

      # 调用确实在 worker 节点执行（环放置 + erpc 转发）
      assert actor_on_node?(troupe, "Counter", stage, worker)

      # 节点死亡：nodedown → 环重建 → 本节点重激活（memory store：无跨节点 props，视为全新激活）
      stop_worker(worker)

      # 等环收缩（环漂移期路由到死亡节点 = 结果未知，重试语义；收缩后再验重激活）
      wait_ring_excludes(troupe, worker)

      # memory store：无跨节点 props → 重激活即全新（n 0 + bump 10 = 10）
      assert {:ok, 10} = Troupe.call(cart, :bump, [10])
      assert {:ok, 10} = Troupe.call(cart, :peek, [])

      Troupe.shutdown(troupe)
    end

    # -- 场景 2：production 角色不兼容 → 协商拒绝 + fail-fast --------------------

    @tag :distributed
    test "production mode rejects role-incompatible node and disconnects it" do
      ensure_distributed()
      suffix = System.unique_integer([:positive])
      worker = :"worker2_#{suffix}@#{hostname()}"
      sname = "worker2_#{suffix}"

      # production 门禁要求共享存储（真实 PG）
      pg = System.fetch_env!("TROUPE_TEST_PG")
      troupe =
        form_primary(
          store: {Troupe.PropStore.Postgres, [url: pg, pool_size: 2]},
          ownership: [lease_ms: 1_000],
          cluster: [seeds: [], mode: :production]
        )

      # worker 跑不同角色集合（Unrelated）：协商被拒 → production fail-fast（进程退出）
      port =
        spawn_worker(sname, %{
          "DIST_ACTORS" => "other",
          "DIST_SEEDS" => "primary@#{hostname()}",
          "DIST_MODE" => "production",
          "DIST_STORE" => "postgres",
          "DIST_PG_URL" => pg,
          "DIST_TROUPE" => Atom.to_string(troupe.name)
        })

      # 读 worker 输出直到 READY（form 完成，随后 Roster 异步 join）或退出（fail-fast）
      worker_out = drain_worker_output(port)
      assert String.contains?(worker_out, "DIST_WORKER_READY"), "worker form completed: #{worker_out}"

      # 协商被拒 → production fail-fast：worker 进程退出（join_seeds raise）
      assert worker_rejected_or_exited(port, worker), "incompatible worker must fail fast"

      # 本端协商机制工作：rejected 异步落账（nodeup/negotiate 为异步消息），等待记录
      roster = Troupe.Names.lookup!(troupe.name, :roster)

      assert wait_until?(10_000, fn ->
               GenServer.call(roster, :negotiation_state).rejected != []
             end), "negotiation rejections not recorded at all"

      refute node_connected?(worker), "incompatible node must be disconnected in production mode"

      # 调用不跨该节点（本节点激活即可用；唯一 stage 避开 PG 跨轮残留）
      stage = "prod-#{System.unique_integer([:positive])}"
      cart = Troupe.cast(troupe, Troupe.DistWorker.Counter, stage)
      assert {:ok, 5} = Troupe.call(cart, :bump, [5])

      Troupe.shutdown(troupe)
    end

    # -- 场景 3：dev 模式角色不兼容 → 记录拒绝但保持实验性连接 -------------------

    @tag :distributed
    test "dev mode records rejection but keeps experimental connectivity" do
      ensure_distributed()
      suffix = System.unique_integer([:positive])
      worker = :"worker3_#{suffix}@#{hostname()}"
      sname = "worker3_#{suffix}"

      troupe = form_primary(cluster: [seeds: [], mode: :dev])

      port =
        spawn_worker(sname, %{
          "DIST_ACTORS" => "other",
          "DIST_SEEDS" => "primary@#{hostname()}",
          "DIST_MODE" => "dev",
          "DIST_TROUPE" => Atom.to_string(troupe.name)
        })

      wait_ready(port)
      wait_connected(worker)

      # 协商互检后本端 roster 记录不兼容原因（incompatible 元组；nodeup 反向协商）
      ok =
        wait_until?(10_000, fn ->
          roster = Troupe.Names.lookup!(troupe.name, :roster)
          state = GenServer.call(roster, :negotiation_state)

          case Keyword.get(state.rejected, worker) do
            nil -> false
            reasons -> Enum.any?(reasons, &match?({:incompatible, _}, &1))
          end
        end)

      unless ok do
        manifest = %{protocol: Troupe.Roster.protocol(), namespace: "default", roles: ["Counter"]}

        hs =
          try do
            :erpc.call(worker, Troupe.Roster, :handshake, [troupe.name, manifest])
          catch
            kind, reason -> {:erpc_failure, kind, reason}
          end

        IO.inspect(hs, label: "manual handshake")
        IO.inspect(GenServer.call(Troupe.Names.lookup!(troupe.name, :roster), :negotiation_state), label: "state")
        flunk("dev rejection not recorded")
      end

      # dev 不断连（实验性集群语义）
      assert node_connected?(worker), "dev mode keeps incompatible nodes connected"

      Troupe.shutdown(troupe)
      stop_worker(worker)
    end

    defp wait_ring_contains(troupe, node, timeout \\ 15_000) do
      wait_until(timeout, fn ->
        ring = Troupe.Playbill.ring_info(troupe.playbill)
        node in (ring[:members] || [])
      end)
    end

    defp wait_ring_excludes(troupe, node, timeout \\ 15_000) do
      wait_until(timeout, fn ->
        ring = Troupe.Playbill.ring_info(troupe.playbill)
        node not in (ring[:members] || [node])
      end)
    end

    # production 协商拒绝的验收：worker 端 fail-fast 退出（PORT exit）或被本端断连
    defp worker_rejected_or_exited(port, worker, timeout \\ 15_000) do
      deadline = now() + timeout
      check(deadline, port, worker)
    end

    defp check(deadline, port, worker) do
      exited? =
        receive do
          {^port, {:exit_status, _}} -> true
        after
          0 -> false
        end

      cond do
        exited? or not node_connected?(worker) ->
          true

        now() >= deadline ->
          false

        true ->
          Process.sleep(100)
          check(deadline, port, worker)
      end
    end

    # 收集 worker stdout 至 READY/ERROR/退出（诊断与就绪判定）
    defp drain_worker_output(port, acc \\ "") do
      receive do
        {^port, {:data, data}} ->
          acc = acc <> data

          cond do
            String.contains?(acc, "DIST_WORKER_READY") or String.contains?(acc, "DIST_WORKER_ERROR") ->
              acc

            true ->
              drain_worker_output(port, acc)
          end

        {^port, {:exit_status, _}} ->
          acc <> "\n[WORKER EXITED]"

        {^port, _} ->
          drain_worker_output(port, acc)
      after
        60_000 ->
          acc <> "\n[DRAIN TIMEOUT]"
      end
    end

    defp actor_on_node?(troupe, role_id, stage_name, expected_node) do
      ok =
        wait_until?(5_000, fn ->
          Troupe.Director.cluster_actors(troupe.name)
          |> Enum.any?(fn a ->
            a.role_id == role_id and a.stage_name == stage_name and a.node == expected_node
          end)
        end)

      unless ok do
        IO.inspect(Troupe.Director.cluster_actors(troupe.name), label: "cluster_actors")
        IO.inspect(Node.list(), label: "nodes")
        flunk("actor #{role_id}/#{stage_name} not on #{expected_node}")
      end

      :ok
    end

    # 可返回 false 的等待（供诊断分支）
    defp wait_until?(timeout, pred, poll \\ 100) do
      deadline = now() + timeout

      repeat_until(deadline, pred, poll)
    end

    defp repeat_until(deadline, pred, poll) do
      if pred.() do
        true
      else
        if now() >= deadline do
          false
        else
          Process.sleep(poll)
          repeat_until(deadline, pred, poll)
        end
      end
    end
  end
end

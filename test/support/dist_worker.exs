# 分布式测试子节点 worker（Troupe.DistributedTest 经 Port 启动）。
#
#   MIX_ENV=test elixir --sname <name>@127.0.0.1 --cookie <cookie> -S mix run \
#     test/support/dist_worker.exs
#
# 环境变量：
#   DIST_TROUPE  — troupe 逻辑名（默认 dist_troupe）
#   DIST_ACTORS  — cart | other（角色集合；cart = Counter，other = Unrelated）
#   DIST_SEEDS   — 逗号分隔 seeds（空 = 不组团，仅 standby）
#   DIST_MODE    — dev | production（默认 dev）
#   DIST_NS      — namespace（默认 default）
#
# 就绪后打印 DIST_WORKER_READY 并驻留；stdout 用于握手协议，其余日志走 stderr。
# Actor 模块定义在 test/support/dist_actors.ex（MIX_ENV=test build，主/子节点共享）。

actors =
  case System.get_env("DIST_ACTORS", "cart") do
    "cart" -> [Troupe.DistWorker.Counter]
    "other" -> [Troupe.DistWorker.Unrelated]
  end

opts = [
  actors: actors,
  name: String.to_atom(System.get_env("DIST_TROUPE", "dist_troupe")),
  namespace: System.get_env("DIST_NS", "default"),
  intermission: "1h",
  store: :memory
]

# production 门禁要求共享存储（Issue #2/#3）；分布式 PG leg 提供真实 PostgreSQL
opts =
  case System.get_env("DIST_STORE") do
    "postgres" ->
      opts
      |> Keyword.put(:store, {Troupe.PropStore.Postgres, [url: System.fetch_env!("DIST_PG_URL"), pool_size: 2]})
      |> Keyword.put(:ownership, [lease_ms: 1_000])

    _ ->
      opts
  end

opts =
  case System.get_env("DIST_SEEDS") do
    nil -> opts
    "" -> opts
    seeds when is_binary(seeds) ->
      Keyword.put(opts, :cluster, [
        seeds: seeds |> String.split(",") |> Enum.map(&String.to_atom/1),
        mode: String.to_atom(System.get_env("DIST_MODE", "dev"))
      ])
  end

case Troupe.form(opts) do
  {:ok, _troupe} ->
    IO.puts("DIST_WORKER_READY #{node()}")

    # 自监护：primary 测试节点退出/断连后自动停机（不留孤儿 VM）
    primary = String.to_atom(System.get_env("DIST_PRIMARY", "primary@127.0.0.1"))

    spawn(fn ->
      :net_kernel.monitor_nodes(true)
      :erlang.monitor_node(primary, true)

      # nodedown → 停机；停机期间 seed reconnect 可能重连——循环确认（VM 死透为止）
      loop = fn loop ->
        receive do
          {:nodedown, ^primary, _} ->
            System.stop(0)
            Process.sleep(5_000)
            loop.(loop)

          :node_down ->
            System.stop(0)
            Process.sleep(5_000)
            loop.(loop)
        end
      end

      loop.(loop)
    end)

    Process.sleep(:infinity)

  {:error, reason} ->
    IO.puts("DIST_WORKER_ERROR #{inspect(reason)}")
    System.stop(1)
end

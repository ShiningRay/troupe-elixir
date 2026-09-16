# 基准：创建性能对比（Process vs Actor 的"诞生成本"）
#
# 场景：
#   * 原生 Process · spawn（即退）     —— 裸进程创建 + 立即退出
#   * 原生 Process · spawn（常驻）     —— 裸进程创建并存活（基准结束后批量回收）
#   * [参考] GenServer · start（常驻） —— OTP 服务器创建
#   * Troupe Actor · 激活（memory）    —— cast + 首调冷启动（Registry 未命中 → Casting
#                                         → 存储建初始 Props → on_stage → Turn 执行）
#   * Troupe Actor · 激活（file 持久） —— 同上，初始 Props 同步落盘（持久成功级别）
#   * Troupe Actor · 激活（跨节点）    —— 可选（--peer）：按 HashRing 路由到远端 Stage 激活
#
# 度量：
#   * 顺序创建：创建/s + 单次 p50/p99（µs）
#   * 并发创建（8 客户端）：创建/s
#   * 常驻场景附 每实例进程内存（KB）
#
# 用法：
#   mix run bench/creation.exs                         # 本机场景
#   mix run bench/creation.exs --peer bench-worker@127.0.0.1   # 加跨节点激活
#     （worker 终端：elixir --name bench-worker@127.0.0.1 --cookie benchcookie \
#        -S mix run bench/benchmark.exs --role worker）

argv = Enum.reject(System.argv(), &(&1 == "--"))

{opts, _rest, _invalid} =
  OptionParser.parse(argv,
    strict: [peer: :string, node: :string, cookie: :string, keep_peer: :boolean]
  )

peer_name = if kw = Keyword.get(opts, :peer), do: String.to_atom(kw)
cookie = String.to_atom(Keyword.get(opts, :cookie, "benchcookie"))

defmodule Bench.Create do
  @moduledoc false

  @tab :bench_create_pids

  def init_table do
    if :ets.info(@tab) == :undefined do
      :ets.new(@tab, [:bag, :public, :named_table])
    end

    :ok
  end

  def collect(pid), do: :ets.insert(@tab, {pid})

  def cleanup do
    for {pid} <- :ets.tab2list(@tab), Process.alive?(pid) do
      Process.exit(pid, :kill)
    end

    :ets.delete_all_objects(@tab)
    :ok
  end

  # ---- 度量 ---------------------------------------------------------------

  @doc """
  顺序执行 n 次 fun（预热不计入）。
  resident?=常驻场景时同时给出每实例进程内存增量（KB）。返回 {n, 总µs, p50µs, p99µs, KB}。
  """
  def measure_seq(fun, n, resident?) do
    for _ <- 1..(div(n, 10) + 5), do: fun.()

    before = if resident?, do: :erlang.memory(:processes)

    rtts =
      for _ <- 1..n do
        t0 = System.monotonic_time(:nanosecond)
        fun.()
        System.monotonic_time(:nanosecond) - t0
      end

    after_mem = if resident?, do: :erlang.memory(:processes)
    total_us = Enum.sum(rtts) / 1_000
    sorted = Enum.sort(rtts)
    p50 = Enum.at(sorted, trunc(0.50 * (n - 1))) / 1_000
    p99 = Enum.at(sorted, trunc(0.99 * (n - 1))) / 1_000

    kb = if resident?, do: Float.round((after_mem - before) / n / 1024, 1)
    {n, total_us, p50, p99, kb}
  end

  @doc "clients 个并发任务各执行 per 次：返回 {total, 总耗时µs}。"
  def measure_par(fun, clients, per) do
    t0 = System.monotonic_time(:microsecond)

    tasks =
      for _ <- 1..clients do
        Task.async(fn ->
          for _ <- 1..per,
              do:
                (
                  fun.()
                  :ok
                )
        end)
      end

    Task.await_many(tasks, 300_000)
    {clients * per, System.monotonic_time(:microsecond) - t0}
  end

  # ---- 报告（CJK 按 2 列宽对齐） -------------------------------------------

  def pad(s, w) when is_binary(s) do
    dw =
      s
      |> String.graphemes()
      |> Enum.reduce(0, fn g, acc -> acc + cjk_width(:unicode.characters_to_list(g)) end)

    s <> String.duplicate(" ", max(w - dw, 1))
  end

  defp cjk_width([c]) when c in 0x1100..0x115F, do: 2
  defp cjk_width([c]) when c in 0x2E80..0xA4CF, do: 2
  defp cjk_width([c]) when c in 0xAC00..0xD7A3, do: 2
  defp cjk_width([c]) when c in 0xF900..0xFAFF, do: 2
  defp cjk_width([c]) when c in 0xFF00..0xFF60, do: 2
  defp cjk_width([c]) when c in 0xFFE0..0xFFE6, do: 2
  defp cjk_width([c]) when c in 0x3000..0x303E, do: 2
  defp cjk_width(_), do: 1

  def fmt(v, w), do: :io_lib.format("~#{w}.1f", [v]) |> List.to_string()
end

# ======================================================================
# troupe 组建
# ======================================================================

# 跨节点场景需先起 distribution：环在组建时固化 node()，后起的节点名不会自愈
if peer_name,
  do:
    Bench.Support.ensure_distribution(:"bench-create-#{System.unique_integer([:positive])}@127.0.0.1", cookie)

Bench.Create.init_table()

{:ok, t_mem} = Troupe.form(actors: [Bench.PingActor], name: :bc_mem, store: :memory)

dir = Path.join(System.tmp_dir!(), "troupe-create-#{System.unique_integer([:positive])}")

{:ok, t_file} =
  Troupe.form(actors: [Bench.PingActor], name: :bc_file, store: :file, props_dir: dir)

# 可选：跨节点激活（需要 worker 节点）
peer =
  if peer_name do
    Bench.Support.wait_peer(peer_name, 60_000)
    :erpc.call(peer_name, Bench.Support, :ensure_troupe, [:bc_clu])

    {:ok, t_clu} =
      Troupe.form(
        actors: [Bench.PingActor],
        name: :bc_clu,
        store: :memory,
        cluster: [seeds: [peer_name]]
      )

    IO.puts("[ok] worker #{peer_name} 就绪，跨节点激活场景启用\n")
    {peer_name, t_clu}
  else
    nil
  end

counter = :atomics.new(1, signed: false)
next = fn prefix -> prefix <> "-#{:atomics.add_get(counter, 1, 1)}" end

# ---- 场景目标 -------------------------------------------------------------

spawn_exit = fn ->
  spawn(fn -> :ok end)
  :ok
end

spawn_alive = fn ->
  Bench.Create.collect(spawn(Bench.Support, :sleep_loop, []))
  :ok
end

gs_start = fn ->
  {:ok, pid} = GenServer.start(Bench.GSEcho, :ok)
  Bench.Create.collect(pid)
  :ok
end

actor_mem = fn ->
  agent = Troupe.cast(t_mem, Bench.PingActor, next.("m"))

  try do
    Troupe.call!(agent, :ping, [])
  rescue
    e in [Troupe.UnknownOutcomeError, Troupe.ActivationError] ->
      IO.puts("!!! 挂起抓栈：#{agent.role_id}/#{agent.stage_name}")

      dump = fn pid, label ->
        if pid && Process.alive?(pid) do
          info = Process.info(pid, [:current_stacktrace, :status, :message_queue_len])

          IO.puts(
            "--- #{label} #{inspect(pid)} status=#{inspect(info[:status])} mq=#{info[:message_queue_len]}"
          )

          Enum.each(info[:current_stacktrace] || [], fn f -> IO.puts("      " <> inspect(f)) end)
        else
          IO.puts("--- #{label}: nil/dead")
        end
      end

      dump.(Troupe.Names.find_actor(t_mem.name, agent.role_id, agent.stage_name), "actor")
      key = {t_mem.config.namespace, agent.role_id, agent.stage_name}
      IO.puts("--- owner(:bc_mem)=#{inspect(Troupe.Playbill.owner(t_mem.playbill, key))}")

      IO.puts(
        "--- registry mem=#{inspect(Registry.lookup(Troupe.Actors, {:bc_mem, agent.role_id, agent.stage_name}))}"
      )

      IO.puts(
        "--- registry clu=#{inspect(Registry.lookup(Troupe.Actors, {:bc_clu, agent.role_id, agent.stage_name}))}"
      )

      IO.puts(
        "--- registry worker side=#{inspect(:erpc.call(peer_name, Registry, :lookup, [Troupe.Actors, {:bc_clu, agent.role_id, agent.stage_name}]))}"
      )

      dump.(Troupe.Names.lookup(t_mem.name, :casting), "casting")
      dump.(Troupe.Names.lookup(t_mem.name, :playbill), "playbill")
      dump.(Troupe.Names.lookup(t_mem.name, :prop_store), "prop holder")
      dump.(Troupe.Names.lookup(t_mem.name, :activator), "activator")
      reraise e, __STACKTRACE__
  end
end

actor_file = fn ->
  Troupe.call!(Troupe.cast(t_file, Bench.PingActor, next.("f")), :ping, [])
end

actor_remote =
  if peer do
    {peer_node, t_clu} = peer

    base =
      Enum.find(500_000..800_000, fn i ->
        Troupe.Playbill.owner(t_clu.playbill, {"default", "Ping", "c-#{i}"}) == peer_node
      end)

    counter2 = :atomics.new(2, signed: false)

    fn ->
      i = base + :atomics.add_get(counter2, 1, 1)
      agent = Troupe.cast(t_clu, Bench.PingActor, "c-#{i}")

      # 瞬断容忍（仅基准）：一次结果未知 → 等 Roster 自愈后重试一次
      try do
        Troupe.call!(agent, :ping, [])
      rescue
        e in [Troupe.UnknownOutcomeError, Troupe.ActivationError] ->
          Process.sleep(1_500)
          Troupe.call!(agent, :ping, [])
      end
    end
  end

seq_specs = [
  {"原生 Process · spawn（即退）", spawn_exit, 30_000, false},
  {"原生 Process · spawn（常驻）", spawn_alive, 30_000, true},
  {"[参考] GenServer · start（常驻）", gs_start, 10_000, true},
  {"Troupe Actor · 激活（memory）", actor_mem, 5_000, true},
  {"Troupe Actor · 激活（file 持久）", actor_file, 1_000, true}
]

seq_specs =
  if peer do
    seq_specs ++ [{"Troupe Actor · 激活（跨节点）", actor_remote, 1_000, true}]
  else
    seq_specs
  end

par_specs = [
  {"原生 Process · spawn（即退）", spawn_exit, 6_000},
  {"原生 Process · spawn（常驻）", spawn_alive, 6_000},
  {"[参考] GenServer · start（常驻）", gs_start, 1_500},
  {"Troupe Actor · 激活（memory）", actor_mem, 1_000},
  {"Troupe Actor · 激活（file 持久）", actor_file, 200}
]

par_specs =
  if peer do
    par_specs ++ [{"Troupe Actor · 激活（跨节点）", actor_remote, 200}]
  else
    par_specs
  end

versions =
  "Elixir #{System.version()} / OTP #{:erlang.system_info(:otp_release)} / #{:erlang.system_info(:schedulers_online)} schedulers"

IO.puts("""
════════════════════════════════════════════════════════════════════════
 创建性能基准：Process vs Actor（每次 = 创建一个全新实例）
 #{versions}
════════════════════════════════════════════════════════════════════════

""")

# ---- 顺序（单客户端，含分位数与内存） ---------------------------------------

caller = self()

rows =
  for {name, fun, n, resident?} <- seq_specs do
    IO.puts("[..] #{name} (n=#{n})")

    # 看门狗：单次调用卡住 15s 时抓调用者栈（诊断用）
    watchdog =
      spawn(fn ->
        receive do
          :done -> :ok
        after
          15_000 ->
            IO.puts("!!! 看门狗：#{name} 卡住 15s")
            info = Process.info(caller, [:current_stacktrace, :status, :message_queue_len])

            IO.puts("caller status=#{inspect(info[:status])} mq=#{info[:message_queue_len]}")
            Enum.each(info[:current_stacktrace] || [], fn f -> IO.puts("      " <> inspect(f)) end)
        end
      end)

    t0 = System.monotonic_time(:millisecond)

    {done, total_us, p50, p99, kb} = Bench.Create.measure_seq(fun, n, resident?)
    send(watchdog, :done)
    IO.puts("[ok] #{name} #{System.monotonic_time(:millisecond) - t0}ms")
    {name, done / (total_us / 1_000_000), p50, p99, kb}
  end

# ---- 并发（8 客户端） ------------------------------------------------------

par_rates =
  Map.new(par_specs, fn {name, fun, per} ->
    {total, us} = Bench.Create.measure_par(fun, 8, per)
    {name, total / (us / 1_000_000)}
  end)

# ---- 报告 ----------------------------------------------------------------

header =
  Bench.Create.pad("场景", 40) <>
    Bench.Create.pad("顺序 创建/s", 13) <>
    Bench.Create.pad("p50 µs", 10) <>
    Bench.Create.pad("p99 µs", 10) <>
    Bench.Create.pad("并发8 创建/s", 14) <> "KB/实例"

IO.puts(header)
IO.puts(String.duplicate("-", 106))

Enum.each(rows, fn {name, rate, p50, p99, kb} ->
  par = Map.fetch!(par_rates, name)

  last = if kb, do: Bench.Create.fmt(kb, 7), else: "-"

  IO.puts(
    Bench.Create.pad(name, 40) <>
      Bench.Create.pad(Bench.Create.fmt(rate, 10), 13) <>
      Bench.Create.pad(Bench.Create.fmt(p50, 7), 10) <>
      Bench.Create.pad(Bench.Create.fmt(p99, 7), 10) <>
      Bench.Create.pad(Bench.Create.fmt(par, 11), 14) <>
      last
  )
end)

IO.puts("""

说明：
 * spawn（即退）包含进程退出与调度器回收；spawn（常驻）只含创建。两者是原生下限。
 * Actor 激活 = cast + 首调冷启动完整路径：寻址未命中 → Casting 单飞 →
   DynamicSupervisor 启动 → 读/建初始 Props（file 场景含同步落盘）→ on_stage → Turn。
 * KB/实例 为进程内存增量（含 ActorServer 进程与 Props；不含存储文件）。
 * 并发激活在同一节点经单一 Casting 进程串行启动——多 Stage（节点）才能水平扩展激活。
""")

# ---- 清理 ----------------------------------------------------------------

Troupe.destroy(t_mem)
Troupe.destroy(t_file)

if peer do
  Troupe.destroy(elem(peer, 1))
end

File.rm_rf!(dir)
Bench.Create.cleanup()

if peer && !Keyword.get(opts, :keep_peer, false) do
  peer_node = elem(peer, 0)

  try do
    :erpc.call(peer_node, :erlang, :halt, [0])
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end
end

:ok

# 基准：消息处理速度对比（同步往返 = 一条消息请求 + 一条回复）
#
# 场景矩阵：
#   原生 Process（裸 send/receive）   ×  同节点 / 跨节点
#   GenServer（参考基线）             ×  同节点 / 跨节点
#   Troupe Actor（cast + call）       ×  同节点 / 跨节点
#
# 度量：
#   * 吞吐：并发客户端同时压测，msg/s（每 msg = 一次完整往返）
#   * 延迟：单客户端顺序往返的 p50 / p99 / 平均（µs）
#
# 用法（跨节点场景需要两个终端；cookie 必须一致）：
#
#   # 终端 1：worker 节点（承载跨节点目标）
#   elixir --name bench-worker@127.0.0.1 --cookie benchcookie -S mix run bench/benchmark.exs --role worker
#
#   # 终端 2：主节点（运行全部基准）
#   mix bench
#   # 或显式指定：mix run bench/benchmark.exs --role main --node bench-main@127.0.0.1 --peer bench-worker@127.0.0.1
#
#   # 只跑同节点场景（无 worker 也可）：
#   mix bench --no-cluster
#
# worker 节点由主节点在结束时停止；也可 --keep-peer 让其继续运行。

argv = Enum.reject(System.argv(), &(&1 == "--"))

{opts, _rest, _invalid} =
  OptionParser.parse(argv,
    strict: [
      role: :string,
      node: :string,
      peer: :string,
      cookie: :string,
      clients: :integer,
      target_ms: :integer,
      no_cluster: :boolean,
      keep_peer: :boolean
    ]
  )

role = Keyword.get(opts, :role, "main")
cookie = String.to_atom(Keyword.get(opts, :cookie, "benchcookie"))
clients = Keyword.get(opts, :clients, 8)
target_ms = Keyword.get(opts, :target_ms, 1_500)
no_cluster? = Keyword.get(opts, :no_cluster, false)

defmodule Bench.Runner do
  @moduledoc false

  ## ------------------------------------------------------------------
  #  分布式启动
  ## ------------------------------------------------------------------

  def ensure_distribution(name, cookie) do
    if node() == :nonode@nohost do
      case :net_kernel.start(name, %{name_domain: :longnames}) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
        {:error, reason} -> raise "cannot start distribution #{inspect(name)}: #{inspect(reason)}"
      end
    end

    :erlang.set_cookie(node(), cookie)
    node()
  end

  def wait_peer(peer, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_peer(peer, deadline)
  end

  defp do_wait_peer(peer, deadline) do
    if Node.connect(peer) == true do
      case :erpc.call(peer, Bench.Support, :ready?, []) do
        true -> :ok
        other -> retry_or_give_up(peer, deadline, {:bad_probe, other})
      end
    else
      retry_or_give_up(peer, deadline, :noconnection)
    end
  end

  defp retry_or_give_up(peer, deadline, reason) do
    if System.monotonic_time(:millisecond) >= deadline do
      raise "cannot reach peer #{inspect(peer)} (#{inspect(reason)})" <>
              " — start it first: elixir --name #{peer} --cookie <cookie> -S mix run bench/benchmark.exs --role worker"
    end

    Process.sleep(500)
    do_wait_peer(peer, deadline)
  end

  ## ------------------------------------------------------------------
  #  场景
  ## ------------------------------------------------------------------

  def throughput(fun, clients, target_ms) do
    # 预热
    run_clients(clients, 50, fun)
    run_clients(clients, 100, fun)

    # 自适应规模：让测量窗口 ≈ target_ms
    {probe_total, probe_us} = run_clients(clients, 50, fun)
    per_op_us = probe_us / probe_total
    per_client = trunc(target_ms * 1_000 / (per_op_us * clients))
    per_client = per_client |> max(200) |> min(500_000)

    {total, us} = run_clients(clients, per_client, fun)
    mps = total / (us / 1_000_000)
    %{throughput: mps, rtt_avg_us: us / per_client, total: total, clients: clients}
  end

  def latency(fun, n) do
    run_clients(1, 100, fun)

    rtts =
      for _ <- 1..n do
        t0 = System.monotonic_time(:nanosecond)
        fun.()
        System.monotonic_time(:nanosecond) - t0
      end

    sorted = Enum.sort(rtts)

    %{
      p50: pct(sorted, 0.50) / 1_000,
      p99: pct(sorted, 0.99) / 1_000,
      avg: Enum.sum(sorted) / n / 1_000
    }
  end

  defp run_clients(clients, per_client, fun) do
    t0 = System.monotonic_time(:microsecond)

    tasks =
      for _ <- 1..clients do
        Task.async(fn ->
          for _ <- 1..per_client do
            fun.()
            :ok
          end
        end)
      end

    Task.await_many(tasks, 300_000)
    {clients * per_client, System.monotonic_time(:microsecond) - t0}
  end

  defp pct(sorted, q) do
    idx = trunc(q * (length(sorted) - 1))
    Enum.at(sorted, idx)
  end

  ## ------------------------------------------------------------------
  #  报告
  ## ------------------------------------------------------------------

  def print_row(name, thr, lat) do
    fmt = fn w, v -> :io_lib.format("~#{w}.1f", [v]) |> List.to_string() end

    IO.puts(
      pad(name, 46) <>
        pad(fmt.(11, thr.throughput), 13) <>
        pad(fmt.(11, thr.rtt_avg_us), 13) <>
        pad(fmt.(8, lat.p50), 10) <>
        pad(fmt.(8, lat.p99), 10) <>
        fmt.(8, lat.avg) <> "
"
    )
  end

  def print_header do
    IO.puts(
      pad("场景", 46) <>
        pad("吞吐 msg/s", 13) <>
        pad("RTT avg µs", 13) <>
        pad("p50 µs", 10) <>
        pad("p99 µs", 10) <> "avg µs"
    )

    IO.puts(String.duplicate("-", 105))
  end

  # CJK 字符按 2 列宽对齐
  def pad(s, w) when is_list(s), do: pad(List.to_string(s), w)

  def pad(s, w) when is_binary(s) do
    dw =
      s
      |> String.graphemes()
      |> Enum.reduce(0, fn g, acc -> acc + cjk_width(g) end)

    s <> String.duplicate(" ", max(w - dw, 1))
  end

  defp cjk_width(g) do
    case :unicode.characters_to_list(g) do
      [c] when c >= 0x1100 and c <= 0x115F -> 2
      [c] when c >= 0x2E80 and c <= 0xA4CF -> 2
      [c] when c >= 0xAC00 and c <= 0xD7A3 -> 2
      [c] when c >= 0xF900 and c <= 0xFAFF -> 2
      [c] when c >= 0xFE30 and c <= 0xFE4F -> 2
      [c] when c >= 0xFF00 and c <= 0xFF60 -> 2
      [c] when c >= 0xFFE0 and c <= 0xFFE6 -> 2
      [c] when c >= 0x3000 and c <= 0x303E -> 2
      _ -> 1
    end
  end
end

# ======================================================================
# 角色分发
# ======================================================================

worker_node =
  Keyword.get(opts, :node, "bench-worker@127.0.0.1") |> String.to_atom()

main_node = "bench-main-#{System.unique_integer([:positive])}@127.0.0.1" |> String.to_atom()
peer_name = Keyword.get(opts, :peer, "bench-worker@127.0.0.1") |> String.to_atom()

if role == "worker" do
  # --------------------------------------------------------------------
  # worker：组建 troupe 后待命（等主节点压测跨节点场景）
  # --------------------------------------------------------------------
  Bench.Runner.ensure_distribution(worker_node, cookie)
  {:ok, _} = Bench.Support.ensure_troupe(:bench)
  IO.puts("WORKER_UP #{node()} (troupe :bench ready)")
  Process.sleep(:infinity)
else
  # --------------------------------------------------------------------
  # main：跑全部场景
  # --------------------------------------------------------------------
  Bench.Runner.ensure_distribution(main_node, cookie)

  versions =
    "Elixir #{System.version()} / OTP #{:erlang.system_info(:otp_release)} / #{:erlang.system_info(:schedulers_online)} schedulers"

  IO.puts("""
  ════════════════════════════════════════════════════════════════════
   Troupe 消息处理速度基准（同步往返：请求 + 回复 各算一条消息）
   #{versions}
   main node: #{node()}   clients: #{clients}   window: #{target_ms}ms
  ════════════════════════════════════════════════════════════════════
  """)

  peer = if no_cluster?, do: nil, else: peer_name

  {peer, troupe} =
    if peer do
      IO.puts("[..] 等待 worker #{peer} …")
      Bench.Runner.wait_peer(peer, 60_000)

      # worker 节点组建同名 troupe（Router 按名解析）
      {:ok, _} = :erpc.call(peer, Bench.Support, :ensure_troupe, [:bench])

      troupe_elem =
        Troupe.form(
          actors: [Bench.PingActor],
          name: :bench,
          store: :memory,
          cluster: [seeds: [peer]]
        )

      {:ok, troupe} = troupe_elem
      IO.puts("[ok] worker 就绪， troupe :bench 已在双节点组团")
      {peer, troupe}
    else
      {:ok, troupe} = Troupe.form(actors: [Bench.PingActor], name: :bench, store: :memory)
      {nil, troupe}
    end

  # ---- 场景目标 ----------------------------------------------------------

  {:ok, native_local} = Task.start(Bench.Support, :echo_loop, [])

  native_remote =
    if peer do
      :erpc.call(peer, :erlang, :spawn, [Bench.Support, :echo_loop, []])
    end

  {:ok, gs_local} = Bench.GSEcho.start_link()

  gs_remote =
    if peer do
      {:ok, pid} = :erpc.call(peer, GenServer, :start, [Bench.GSEcho, :ok])
      pid
    else
      :skip
    end

  # Actor 寻址：找一个哈希到本节点的名字（同节点场景）、一个哈希到 worker 的名字（跨节点场景）
  find_name = fn want_node ->
    Enum.find(0..200_000, fn i ->
      Troupe.Playbill.owner(troupe.playbill, {"default", "Ping", "bench-#{i}"}) == want_node
    end)
  end

  local_i = find_name.(node())

  agent_local =
    Troupe.cast(troupe, Bench.PingActor, "bench-#{local_i}")

  agent_remote =
    if peer do
      i = find_name.(peer)
      Troupe.cast(troupe, Bench.PingActor, "bench-#{i}")
    end

  scenarios = [
    {"原生 Process · 同节点（裸 send/receive）",
     fn ->
       ref = make_ref()
       send(native_local, {:ping, self(), ref})

       receive do
         {:pong, ^ref} -> :ok
       end
     end}
  ]

  scenarios =
    if peer do
      scenarios ++
        [
          {"原生 Process · 跨节点（distribution）",
           fn ->
             ref = make_ref()
             send(native_remote, {:ping, self(), ref})

             receive do
               {:pong, ^ref} -> :ok
             end
           end}
        ]
    else
      scenarios
    end

  scenarios =
    scenarios ++
      [
        {"[参考] GenServer · 同节点", fn -> GenServer.call(gs_local, :ping) end}
      ]

  scenarios =
    if peer,
      do: scenarios ++ [{"[参考] GenServer · 跨节点", fn -> GenServer.call(gs_remote, :ping) end}],
      else: scenarios

  scenarios =
    scenarios ++
      [
        {"Troupe Actor · 同节点（strict Turn）", fn -> Troupe.call!(agent_local, :ping, []) end}
      ]

  scenarios =
    if peer,
      do:
        scenarios ++
          [
            {"Troupe Actor · 跨节点（erpc 路由）", fn -> Troupe.call!(agent_remote, :ping, []) end}
          ],
      else: scenarios

  IO.puts("")
  Bench.Runner.print_header()

  results =
    for {name, fun} <- scenarios do
      thr = Bench.Runner.throughput(fun, clients, target_ms)
      lat = Bench.Runner.latency(fun, 1_000)
      Bench.Runner.print_row(name, thr, lat)
      {name, thr, lat}
    end

  IO.puts(String.duplicate("-", 100))
  local_native = elem(Enum.at(results, 0), 1).throughput

  IO.puts("")
  IO.puts("  相对「原生 Process · 同节点」的吞吐比：")

  Enum.each(results, fn {name, thr, _lat} ->
    pct_line = :io_lib.format("~6.1f%", [thr.throughput / local_native * 100]) |> List.to_string()
    IO.puts("    " <> Bench.Runner.pad(name, 46) <> pct_line)
  end)

  IO.puts("""

  说明：
   * 原生 Process = 裸 spawn + send/receive，无任何框架；
     GenServer = OTP 服务器（monitor + 回复路由）参考线；
     Troupe Actor = 虚拟寻址（HashRing 放置判定）+ 调度账本 + 严格串行 Turn + 提交。
   * 同节点 Actor 的每次 Troupe.call 含放置判定与 Registry 查址；跨节点再经
     erpc（distribution）转发到归属 Stage 的 ActorServer。
   * 单 Actor 即单串行通道（容量事实）：并发客户端压同一 Actor 时，吞吐受串行
     Turn 约束——扩展方式是更多 Actor（不同 stageName）/ 更多 Stage。
  """)

  # 收尾：停止 worker
  if peer && !Keyword.get(opts, :keep_peer, false) do
    try do
      :erpc.call(peer, :erlang, :halt, [0])
    rescue
      _ -> :ok
    catch
      :exit, _ -> :ok
    end
  end

  :ok
end

defmodule Bench.Support do
  @moduledoc """
  基准测试支持模块（编译进应用，主/工作两个节点都可用）。

  * `echo_loop/0` —— 原生 Process 的 echo 循环（裸 send/receive，零框架）
  * `bootstrap_troupe/1` —— 在本节点组建 troupe（worker 节点引导用）
  """

  ## 原生 Process echo 循环：收 {ping, 回复方, 引用} 回 {:pong, 引用}
  def echo_loop do
    receive do
      {:ping, reply_to, ref} ->
        send(reply_to, {:pong, ref})
        echo_loop()

      :stop ->
        :ok
    end
  end

  ## GenServer echo（参考基线）
  def gs_init, do: :ok

  ## 常驻原生进程（创建基准：存活但不通信）
  def sleep_loop, do: Process.sleep(:infinity)

  ## 分布式启动（脚本用）
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
    do_wait(peer, deadline)
  end

  defp do_wait(peer, deadline) do
    if Node.connect(peer) == true and :erpc.call(peer, Bench.Support, :ready?, []) == true do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        raise "cannot reach peer #{inspect(peer)} — start: elixir --name #{peer} --cookie <c> -S mix run bench/benchmark.exs --role worker"
      end

      Process.sleep(500)
      do_wait(peer, deadline)
    end
  end

  ## worker 节点引导：确保同名 troupe 就绪（幂等，Router 按名解析）
  def ensure_troupe(name) do
    {:ok, _} = Application.ensure_all_started(:troupe)

    case Troupe.by_name(name) do
      %Troupe{} ->
        {:ok, name}

      nil ->
        {:ok, troupe} = Troupe.form(actors: [Bench.PingActor], name: name, store: :memory)
        {:ok, troupe.name}
    end
  end

  ## 简单存活探测（等待 worker 就绪）
  def ready?, do: true
end

defmodule Bench.PingActor do
  @moduledoc "基准用 Actor：一条 strict Call，回复 :pong（默认串行通道）"
  use Troupe.Actor

  def initial_props, do: %{pings: 0}

  def ping(props), do: {:reply, :pong, Map.update!(props, :pings, &(&1 + 1))}
end

defmodule Bench.GSEcho do
  @moduledoc "GenServer echo（参考基线）"
  use GenServer

  def start_link, do: GenServer.start_link(__MODULE__, :ok)

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call(:ping, _from, state), do: {:reply, :pong, state}
end

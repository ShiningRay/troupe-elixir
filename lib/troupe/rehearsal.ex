defmodule Troupe.Rehearsal do
  @moduledoc """
  Rehearsal（排练）：测试集群。强制全内存 PropStore（无持久副作用）、
  单节点（无集群语义），其余默认不变——`NODE_ENV=test` 自动 Rehearsal 的等价物。

  ```elixir
  {:ok, troupe} = Troupe.Rehearsal.form(actors: [MyApp.CartActor])
  ```

  常配 `use Troupe.Case`（自动 start/清理 + 断言助手）。
  """

  @spec form(keyword()) :: {:ok, Troupe.t()}
  def form(opts \\ []) do
    opts
    |> Keyword.put_new(:store, :memory)
    |> Keyword.put(:cues, Keyword.get(opts, :cues, true))
    |> Troupe.form()
  end
end

defmodule Troupe.Case do
  @moduledoc """
  ExUnit 集成：每个测试自动获得一个隔离 Rehearsal troupe。

  ```elixir
  defmodule MyApp.CartTest do
    use ExUnit.Case, async: true
    use Troupe.Case, actors: [MyApp.CartActor]

    test "cart", %{troupe: troupe} do
      cart = Troupe.cast(troupe, MyApp.CartActor, "t1")
      assert {:ok, :ok} = Troupe.call(cart, :add, ["a"])
      assert {:ok, 1} = Troupe.call(cart, :peek, [])
    end
  end
  ```

  * 每个 troupe 唯一命名 + 内存存储 → `async: true` 安全
  * 测试结束自动 shutdown
  * `destroy_troupe/0` 模拟 kill -9（直接击杀在场 Actor，验证故障重激活）
  """

  use ExUnit.CaseTemplate

  using(opts) do
    quote do
      import Troupe.Case

      setup do
        {:ok, troupe} = Troupe.Rehearsal.form(unquote(opts))
        on_exit(fn -> Troupe.shutdown(troupe, grace: 2_000) end)
        {:ok, troupe: troupe}
      end
    end
  end

  @doc "模拟 kill -9：击杀全部在场 Actor（Props 已提交部分保留）。"
  def destroy_troupe(troupe), do: Troupe.destroy(troupe)

  @doc "等待条件成立（默认 2s 超时，10ms 轮询）。"
  def eventually(cond, timeout \\ 2_000) when is_function(cond, 0) do
    deadline = System.monotonic_time(:millisecond) + timeout

    do_eventually(cond, deadline)
  end

  defp do_eventually(cond, deadline) do
    if cond.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("eventually/2 timed out")
      end

      Process.sleep(10)
      do_eventually(cond, deadline)
    end
  end
end

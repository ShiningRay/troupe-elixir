defmodule Troupe.DistWorker.Counter do
  @moduledoc "分布式测试共享 Actor：计数器（主/子节点加载同一 test build，模块一致 = 角色协商一致）。"
  use Troupe.Actor

  def initial_props, do: %{"n" => 0}

  def bump(props, by \\ 1), do: {:save, props["n"] + by, Map.update!(props, "n", &(&1 + by))}
  def peek(props), do: {:reply, props["n"], props}
end

defmodule Troupe.DistWorker.Unrelated do
  @moduledoc "分布式测试的不相干角色集合（模拟两节点部署了不同 Actor 注册表）。"
  use Troupe.Actor

  def initial_props, do: %{"x" => 1}
  def touch(props), do: {:save, :ok, Map.update!(props, "x", &(&1 + 1))}
end

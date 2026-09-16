defmodule Troupe.HashRingTest do
  use ExUnit.Case, async: true

  alias Troupe.HashRing

  @keys for i <- 1..3000, do: {"/default/Role/#{i}"}

  test "确定性：同成员环对同一键恒定归属" do
    r1 = HashRing.new([:a@n1, :b@n1, :c@n1])
    r2 = HashRing.new([:a@n1, :b@n1, :c@n1])

    for k <- @keys do
      assert HashRing.node_for(r1, k) == HashRing.node_for(r2, k)
    end
  end

  test "均衡度：3000 键在 3 节点上的分布接近均匀（±25%）" do
    ring = HashRing.new([:a, :b, :c])

    counts =
      @keys
      |> Enum.frequencies_by(&HashRing.node_for(ring, &1))
      |> Map.new()

    for {node, n} <- counts do
      assert n in 750..1250, "#{node}: #{n}"
    end
  end

  test "成员变更漂移：只迁移 ~1/N 的键（一致性哈希红利）" do
    ring3 = HashRing.new([:a, :b, :c])
    ring4 = HashRing.add(ring3, :d)

    moved =
      Enum.count(@keys, fn k -> HashRing.node_for(ring3, k) != HashRing.node_for(ring4, k) end)

    # 理想 25%，放宽到 12%..40%
    ratio = moved / length(@keys)
    assert ratio >= 0.12 and ratio <= 0.40

    ring_back = HashRing.remove(ring4, :d)
    assert Enum.all?(@keys, fn k -> HashRing.node_for(ring3, k) == HashRing.node_for(ring_back, k) end)
  end

  test "空环返回 nil；单节点环恒定" do
    assert HashRing.node_for(HashRing.new([]), "x") == nil
    ring = HashRing.new([:solo])

    assert Enum.all?(@keys, &(HashRing.node_for(ring, &1) == :solo))
  end
end

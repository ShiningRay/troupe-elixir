defmodule Troupe.HashRing do
  @moduledoc """
  纯一致性哈希环（Playbill 的放置内核）。

  * 哈希函数：`:erlang.phash2/2`（任意节点对同一基本项结果恒定，保证跨 Stage 放置一致）
  * 每成员 `@vnodes`（160）个虚拟节点，与 Troupe.js 参考实现一致
  * 环是无目录寻址：`node_for/2` 只回答"候选归谁"，是否真正拥有由所有权契约决定

  环结构为有序 `{hash, node}` 列表，查找为二分（首地址 = 第一个 `hash >= key` 的 vnode，环形回绕）。
  """

  @vnodes 160
  @range 4_294_967_296

  @opaque t :: {members :: [node()], ring :: [{hash :: non_neg_integer(), node()}]}

  @spec new([node()]) :: t()
  def new(members) do
    members = members |> List.wrap() |> Enum.uniq() |> Enum.sort()
    {members, build(members)}
  end

  @spec members(t()) :: [node()]
  def members({members, _}), do: members

  @spec add(t(), node()) :: t()
  def add({members, _}, node), do: new(Enum.uniq([node | members]))

  @spec remove(t(), node()) :: t()
  def remove({members, _}, node), do: new(List.delete(members, node))

  @doc "键的归属节点。空环返回 nil。"
  @spec node_for(t(), term()) :: node() | nil
  def node_for({_, []}, _key), do: nil

  def node_for({_, ring}, key) do
    h = :erlang.phash2(key, @range)
    {_, node} = first_at_or_after(ring, h)
    node
  end

  @doc "各成员的虚拟节点份额（CLI `ring` 视图 / 均衡度）。"
  @spec distribution(t()) :: [{node(), non_neg_integer()}]
  def distribution({members, ring}) do
    counts = Enum.frequencies(for {_, n} <- ring, do: n)
    for m <- members, do: {m, Map.get(counts, m, 0)}
  end

  # ring 已按 hash 升序；找第一个 hash >= h 的条目，找不到回绕到第一个。
  defp first_at_or_after(ring, h) do
    case Enum.find(ring, fn {hash, _} -> hash >= h end) do
      nil -> hd(ring)
      found -> found
    end
  end

  defp build([]), do: []

  defp build(members) do
    members
    |> Enum.flat_map(fn node ->
      for i <- 0..(@vnodes - 1) do
        {:erlang.phash2({node, i}, @range), node}
      end
    end)
    |> Enum.sort()
  end
end

defmodule Troupe.Playbill do
  @moduledoc """
  Playbill（节目单）：寻址路由——**纯一致性哈希，无目录**。

  `phash2((namespace, roleId, stageName))` → HashRing（每 Stage 160 虚拟节点）→ 归属 Stage。
  无注册、无目录查询、无缓存失效——这是"简单"的最大红利。

  HashRing 只回答"候选归谁"；是否真正拥有该 Actor 由所有权契约决定（启用 ownership 时
  激活前置租约）。成员变更漂移期，Router 将误投 Call 转发给正确归属（跳数上限 2，防环）。
  """

  use GenServer
  alias Troupe.HashRing

  def child_spec(opts) do
    troupe_name = Keyword.fetch!(opts, :troupe_name)

    %{
      id: {__MODULE__, troupe_name},
      start: {__MODULE__, :start_link, [[troupe_name: troupe_name, opts: opts]]}
    }
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :troupe_name)

    GenServer.start_link(__MODULE__, %{troupe_name: name, opts: opts},
      name: Troupe.Names.via(name, :playbill)
    )
  end

  @impl true
  def init(%{troupe_name: troupe_name, opts: opts}) do
    clustered? = Keyword.has_key?(opts, :cluster)
    ring = HashRing.new([node()])

    if clustered? do
      # Roster 推送成员变更；本地单机模式环恒为 [self]
      case Troupe.Names.lookup(troupe_name, :roster) do
        nil ->
          :ok

        roster ->
          Process.send_after(self(), :troupe_pull_members, 0)
          GenServer.cast(roster, {:troupe_subscribe_playbill, self()})
      end
    end

    {:ok, %{ring: ring, troupe_name: troupe_name}}
  end

  @impl true
  def handle_info(:troupe_pull_members, s) do
    case Troupe.Names.lookup(s.troupe_name, :roster) do
      nil -> {:noreply, s}
      roster -> {:noreply, %{s | ring: HashRing.new(GenServer.call(roster, :members))}}
    end
  end

  @impl true
  def handle_cast({:troupe_members, members}, s) do
    {:noreply, %{s | ring: HashRing.new(members)}}
  end

  @impl true
  def handle_cast({:troupe_subscribe_playbill, _pid}, s) do
    {:noreply, s}
  end

  @impl true
  def handle_call({:owner, key}, _from, s) do
    {:reply, HashRing.node_for(s.ring, key), s}
  end

  def handle_call(:ring_info, _from, s) do
    {:reply,
     %{
       members: HashRing.members(s.ring),
       vnodes_per_member: 160,
       distribution: HashRing.distribution(s.ring)
     }, s}
  end

  # -- API ------------------------------------------------------------------

  @doc "键的候选归属节点。"
  @spec owner(pid(), Troupe.PropStore.key()) :: node() | nil
  def owner(playbill, key), do: GenServer.call(playbill, {:owner, key})

  @doc "环视图（CLI `ring`）。"
  @spec ring_info(pid()) :: map()
  def ring_info(playbill), do: GenServer.call(playbill, :ring_info)
end

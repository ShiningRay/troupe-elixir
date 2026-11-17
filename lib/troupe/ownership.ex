defmodule Troupe.Ownership do
  @moduledoc """
  所有权契约（Troupe.js DESIGN §6.5）：`(namespace, roleId, stageName) → {owner, epoch, leaseUntil}`。

  * **激活前置**：CAS 取得所有权（新 epoch > 旧）才 OnStage；取不到则激活失败
  * **租约续期**：在场期间以 lease/3 周期续约；丢失即排空让位
  * **fencing token**：epoch 随 `write` 传给 PropStore——共享存储 Provider 拒绝旧 token，
    暂停后恢复的旧持有者写入被存储拒绝（内存/DETS 单写者实现不校验，见契约注释）

  ⚠️ 租约不保护**外部副作用**：HTTP/消息接收方需校验 epoch 或用 outbox。

  无协调模式（不启用 ownership）定位为**实验性集群**：分区/扩缩容期间同一 Actor
  可能双活，revision CAS 使冲突写入可检测（防静默丢更新），不对强一致业务做无条件保证。

  内置 `Troupe.Ownership.Memory`（同节点单写者，实验用）；生产多节点需共享存储实现
  （Postgres/Redis，同一 behaviour）。
  """

  @type ctx :: term()
  @type key :: Troupe.PropStore.key()

  @callback acquire(ctx, key, owner :: term(), lease_ms :: non_neg_integer()) ::
              {:ok, epoch :: pos_integer()} | {:error, :busy}
  @callback renew(ctx, key, owner :: term(), epoch :: pos_integer(), lease_ms :: non_neg_integer()) ::
              :ok | {:error, :lost}
  @callback release(ctx, key, owner :: term(), epoch :: pos_integer()) :: :ok

  # 可选能力声明：跨节点仲裁（production 单激活证明的前提）。
  @callback cross_node_safe :: boolean()
  @optional_callbacks cross_node_safe: 0

  @doc "Provider 是否提供跨节点单激活仲裁（默认 false）。"
  @spec cross_node_safe?(module()) :: boolean()
  def cross_node_safe?(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :cross_node_safe, 0) and module.cross_node_safe()
  end
end

defmodule Troupe.Ownership.Memory do
  @moduledoc "单节点内存实现：记录随进程保存，重启即失效（实验/单机多 troupe 用）。"

  use GenServer
  @behaviour Troupe.Ownership

  def child_spec(opts) do
    troupe_name = Keyword.fetch!(opts, :troupe_name)

    %{id: {__MODULE__, troupe_name}, start: {__MODULE__, :start_link, [[troupe_name: troupe_name]]}}
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :troupe_name)
    GenServer.start_link(__MODULE__, :ok, name: Troupe.Names.via(name, :ownership))
  end

  @impl true
  def init(:ok), do: {:ok, %{}}

  @impl true
  def acquire(server, key, owner, lease_ms),
    do: GenServer.call(server, {:acquire, key, owner, lease_ms})

  @impl true
  def renew(server, key, owner, epoch, lease_ms),
    do: GenServer.call(server, {:renew, key, owner, epoch, lease_ms})

  @impl true
  def release(server, key, owner, epoch),
    do: GenServer.call(server, {:release, key, owner, epoch})

  @impl true
  def handle_call({:acquire, key, owner, lease_ms}, _from, st) do
    now = System.system_time(:millisecond)

    case Map.get(st, key) do
      %{owner: ^owner} = rec ->
        {:reply, {:ok, rec.epoch}, Map.put(st, key, %{rec | lease_until: now + lease_ms})}

      %{lease_until: until} when until > now ->
        {:reply, {:error, :busy}, st}

      %{epoch: epoch} ->
        {:reply, {:ok, epoch + 1},
         Map.put(st, key, %{owner: owner, epoch: epoch + 1, lease_until: now + lease_ms})}

      nil ->
        {:reply, {:ok, 1}, Map.put(st, key, %{owner: owner, epoch: 1, lease_until: now + lease_ms})}
    end
  end

  def handle_call({:renew, key, owner, epoch, lease_ms}, _from, st) do
    now = System.system_time(:millisecond)

    case Map.get(st, key) do
      %{owner: ^owner, epoch: ^epoch} = rec ->
        {:reply, :ok, Map.put(st, key, %{rec | lease_until: now + lease_ms})}

      _ ->
        {:reply, {:error, :lost}, st}
    end
  end

  def handle_call({:release, key, owner, epoch}, _from, st) do
    case Map.get(st, key) do
      %{owner: ^owner, epoch: ^epoch} -> {:reply, :ok, Map.delete(st, key)}
      _ -> {:reply, :ok, st}
    end
  end

  @impl true
  def cross_node_safe, do: false
end

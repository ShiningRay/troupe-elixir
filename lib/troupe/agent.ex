defmodule Troupe.Agent do
  @moduledoc """
  Agent（经纪人）：Actor 的代理引用，调用方持有它做位置透明的调用。

  只携带逻辑身份 `(troupe_name, role_id, stage_name)`，不含进程或节点信息——
  因此可以安全地跨节点传输、存入 Props/数据库（不含 pid，进程无关）。
  首条 `Troupe.call/4` 到达时目标 Actor 才被激活（OnStage）。

  注意与 OTP `GenServer.cast/2` 无关：`Troupe.cast/3` 是"选角"——返回 Agent。

  在 Actor 方法体内等价 JS `this.stageName` 的助手是 `Troupe.Actor.ctx/0`
  （含 `troupe_name / role_id / stage_name`）。
  """

  @enforce_keys [:troupe_name, :role_id, :stage_name]
  defstruct [:troupe_name, :role_id, :stage_name]

  @type t :: %__MODULE__{troupe_name: atom(), role_id: String.t(), stage_name: String.t()}

  @doc false
  @spec build(atom(), String.t(), term()) :: t()
  def build(troupe_name, role_id, stage_name) when is_atom(troupe_name) do
    %__MODULE__{troupe_name: troupe_name, role_id: role_id, stage_name: to_string(stage_name)}
  end

  @doc "寻址键：`(namespace, roleId, stageName)`，存储与放置共用。"
  @spec key(t(), String.t()) :: Troupe.PropStore.key()
  def key(%__MODULE__{} = agent, namespace), do: {namespace, agent.role_id, agent.stage_name}
end

defimpl Inspect, for: Troupe.Agent do
  def inspect(agent, _opts) do
    "#Agent<#{agent.troupe_name} #{agent.role_id}::#{agent.stage_name}>"
  end
end

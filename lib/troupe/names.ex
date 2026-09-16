defmodule Troupe.Names do
  @moduledoc """
  Troupe 进程命名：经全局 `Registry` 以 `{troupe_name, component}` 寻址，零 atom 泄漏。

  两个应用级 Registry（由 `Troupe.Application` 启动）：

  * `Troupe.Names` —— 每个 Troupe 的基础设施进程（playbill / casting / activator / …）
  * `Troupe.Actors` —— 在场 Actor 进程，键 `{troupe_name, role_id, stage_name}`
  """

  @spec via(atom(), term()) :: {:via, Registry, {module(), term()}}
  def via(troupe_name, component),
    do: {:via, Registry, {Troupe.Names, {troupe_name, component}}}

  @spec actor_key(atom(), String.t(), String.t()) :: {atom(), String.t(), String.t()}
  def actor_key(troupe_name, role_id, stage_name), do: {troupe_name, role_id, stage_name}

  @spec actor_via(atom(), String.t(), String.t()) :: {:via, Registry, {module(), term()}}
  def actor_via(troupe_name, role_id, stage_name),
    do: {:via, Registry, {Troupe.Actors, actor_key(troupe_name, role_id, stage_name)}}

  @doc "解析本节点上 troupe 的基础设施进程；未启动返回 nil。"
  @spec lookup(atom(), term()) :: pid() | nil
  def lookup(troupe_name, component) do
    case Registry.lookup(Troupe.Names, {troupe_name, component}) do
      [{pid, _meta} | _] -> pid
      [] -> nil
    end
  end

  @doc "解析本节点上 troupe 的基础设施进程；未启动 raise。"
  @spec lookup!(atom(), term()) :: pid()
  def lookup!(troupe_name, component) do
    lookup(troupe_name, component) ||
      raise "troupe #{inspect(troupe_name)} is not running on #{inspect(node())}" <>
              " (component: #{inspect(component)})"
  end

  @doc "查找在场 Actor 进程（未激活返回 nil）。"
  @spec find_actor(atom(), String.t(), String.t()) :: pid() | nil
  def find_actor(troupe_name, role_id, stage_name) do
    case Registry.lookup(Troupe.Actors, actor_key(troupe_name, role_id, stage_name)) do
      [{pid, _meta} | _] -> pid
      [] -> nil
    end
  end

  @doc "本节点上某 troupe 的全部在场 Actor 键与 pid。"
  @spec local_actors(atom()) :: [{{String.t(), String.t()}, pid()}]
  def local_actors(troupe_name) do
    Registry.select(Troupe.Actors, [
      {{{troupe_name, :"$1", :"$2"}, :"$3", :_}, [], [{{:"$1", :"$2", :"$3"}}]}
    ])
    |> Enum.map(fn {role_id, stage_name, pid} -> {{role_id, stage_name}, pid} end)
  end
end

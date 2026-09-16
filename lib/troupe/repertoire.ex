defmodule Troupe.Repertoire do
  @moduledoc """
  Repertoire（保留剧目表）：本 Troupe 的角色注册表。

  `Troupe.form(actors: [...])` 显式注册（推荐，零全局副作用）；每个 Troupe 一份独立快照，
  同节点多 Troupe 互不污染。注册时完成 fail-fast 校验（Troupe.js DESIGN §8.5）：

  * 角色名冲突 → 启动失败，指出冲突模块
  * `read_only` / `improv` / `timeouts` / `cues` / `rpc` 中的未知方法 → 启动失败
  * 生命周期钩子与元数据函数永不进入 RPC 白名单（信任边界）
  """

  alias Troupe.Actor

  @enforce_keys [:by_role, :by_module]
  defstruct [:by_role, :by_module]

  @type entry :: %{
          required(:module) => module(),
          required(:role_id) => String.t(),
          required(:methods) => MapSet.t({atom(), non_neg_integer()}),
          required(:rpc) => MapSet.t({atom(), non_neg_integer()})
        }

  @type t :: %__MODULE__{by_role: %{String.t() => entry()}, by_module: %{module() => entry()}}

  @doc "构建注册表并完成全部 form 期校验；任何问题直接 raise（显式报错清单）。"
  @spec build([module()]) :: t()
  def build(modules) do
    entries =
      modules
      |> List.wrap()
      |> Enum.map(fn module ->
        unless Code.ensure_loaded?(module) and function_exported?(module, :__troupe_role_id__, 0) do
          raise ArgumentError,
                "#{inspect(module)} is not a Troupe actor — `use Troupe.Actor` first"
        end

        %{
          role_id: module.__troupe_role_id__(),
          module: module,
          methods: surface(module),
          rpc: rpc_surface(module)
        }
      end)

    # 角色冲突 fail fast
    Enum.group_by(entries, & &1.role_id)
    |> Enum.each(fn {role_id, group} ->
      if length(group) > 1 do
        raise Troupe.RoleConflictError,
          role_id: role_id,
          modules: Enum.map(group, & &1.module)
      end
    end)

    by_role = Map.new(entries, &{&1.role_id, &1})
    by_module = Map.new(entries, &{&1.module, &1})
    %__MODULE__{by_role: by_role, by_module: by_module}
  end

  @doc "模块 → 角色条目；未注册角色在此 fail（错误列出当前 Repertoire）。"
  @spec fetch!(t(), module() | String.t()) :: entry()
  def fetch!(%__MODULE__{} = rep, module) when is_atom(module) do
    case Map.fetch(rep.by_module, module) do
      {:ok, entry} -> entry
      :error -> raise Troupe.UnknownRoleError, module_or_role: module, repertoire: Map.keys(rep.by_role)
    end
  end

  def fetch!(%__MODULE__{} = rep, role_id) when is_binary(role_id) do
    case Map.fetch(rep.by_role, role_id) do
      {:ok, entry} -> entry
      :error -> raise Troupe.UnknownRoleError, module_or_role: role_id, repertoire: Map.keys(rep.by_role)
    end
  end

  @doc "角色可被普通 RPC 调用的方法面：导出函数 − 保留函数 − 编译器内联产物，可被 `rpc:` 收窄。"
  @spec surface(module()) :: MapSet.t({atom(), non_neg_integer()})
  def surface(module) do
    reserved = MapSet.new(Actor.reserved())

    module.module_info(:functions)
    |> MapSet.new()
    |> MapSet.filter(fn {name, arity} ->
      arity >= 1 and not MapSet.member?(reserved, name) and
        not (is_atom(name) and String.starts_with?(to_string(name), "-"))
    end)
  end

  defp rpc_surface(module) do
    methods = surface(module)

    case module.__troupe_rpc__() do
      :all ->
        methods

      names when is_list(names) ->
        narrowed =
          MapSet.new(names, fn name ->
            Enum.find(methods, {:missing, name}, fn {n, _a} -> n == name end)
          end)

        case Enum.find(narrowed, &match?({:missing, _}, &1)) do
          nil ->
            MapSet.new(narrowed)

          {:missing, name} ->
            raise ArgumentError,
                  "rpc whitelist of #{inspect(module)} lists unknown method #{inspect(name)}" <>
                    " — exported methods: #{inspect(Enum.map(methods, &elem(&1, 0)) |> Enum.uniq())}"
        end

      other ->
        raise ArgumentError, "rpc must be :all or a list of method names, got: #{inspect(other)}"
    end
  end

  @doc "form 期校验：kind/timeouts/cues 声明的方法必须存在。"
  @spec validate!(t()) :: :ok
  def validate!(%__MODULE__{} = rep) do
    for {_role, entry} <- rep.by_role do
      m = entry.module
      exported = MapSet.new(entry.methods, fn {name, _arity} -> name end)
      missing = MapSet.difference(declared_methods(m), exported)

      unless MapSet.size(missing) == 0 do
        raise ArgumentError,
              "#{inspect(m)} declares unknown methods: #{inspect(MapSet.to_list(missing))}" <>
                " — exported: #{inspect(MapSet.to_list(exported))}"
      end
    end

    :ok
  end

  defp declared_methods(module) do
    if function_exported?(module, :__troupe_declared__, 0) do
      MapSet.new(module.__troupe_declared__(), fn {name, _tag} -> name end)
    else
      MapSet.new()
    end
  end
end

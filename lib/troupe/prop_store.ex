defmodule Troupe.PropStore do
  @moduledoc """
  PropStore（道具间）行为契约——Props 的持久化提供方。

  并发修订号（`revision`，每次成功写入 +1，CAS 用）与结构版本
  （`schema_version`，状态形状演进 +1，`migrate_props` 用）是两个维度。

  ## 合同（Provider 必须遵守）

  * `create/4` 仅当记录不存在时成功（原子）；已存在返回 `{:error, {:conflict, :exists}}`
  * `write/4` 仅当当前 `revision == expected_revision` 时写入（CAS 原子）；
    冲突返回 `{:error, {:conflict, :revision}}`，成功返回新 revision
  * 写结果不确定（超时）时，调用方以 `read/2` 校验实际 revision 后决定，不盲目重写
  * `fencing_token`：可选校验项——共享存储 Provider 应拒绝旧 token 的写入（所有权，§6.5）

  内置实现（均为 Troupe 监督树管理，零依赖）：

  | 实现 | 持久级别 | 用途 |
  |---|---|---|
  | `Memory`（ETS） | 无 | Rehearsal / 测试 |
  | `File`（快照文件 + 原子改名） | 进程级 | 默认；`./.troupe/props/`，进程崩溃/重启可恢复 |
  | `Dets`（OTP `:dets`） | 进程级 | 可选；部分 Windows/OTP 组合上 `:dets` 本身不稳定，故默认不启用 |
  | 自定义 `{module, init_opts}` | — | 节点级共享实现（Postgres/Redis）按同一契约接入 |
  """

  @type key :: {namespace :: String.t(), role_id :: String.t(), stage_name :: String.t()}

  @type stored :: %{
          required(:props) => map(),
          required(:revision) => non_neg_integer(),
          required(:schema_version) => non_neg_integer()
        }

  @type ctx :: term()
  @type kind :: :memory | :file | :dets | {module(), term()}

  @callback read(ctx, key) :: {:ok, stored() | nil} | {:error, term()}
  @callback create(ctx, key, props :: map(), schema_version :: pos_integer()) ::
              {:ok, stored()} | {:error, {:conflict, :exists}} | {:error, term()}
  @callback write(ctx, key, props :: map(), opts :: keyword()) ::
              {:ok, stored()} | {:error, {:conflict, :revision}} | {:error, term()}
  @callback delete(ctx, key) :: :ok | {:error, term()}

  @doc "为内置实现生成监督树子进程（自定义 {module, opts} 返回 nil，ctx 在 form 期由 init/1 构建）。"
  @spec child_spec(kind(), keyword(), atom()) :: Supervisor.child_spec() | nil
  def child_spec(:memory, _opts, troupe_name) do
    %{
      id: {Troupe.PropStore.Memory, troupe_name},
      start: {Troupe.PropStore.Memory, :start_link, [[troupe_name: troupe_name]]}
    }
  end

  def child_spec(:file, opts, troupe_name) do
    %{
      id: {Troupe.PropStore.File.Server, troupe_name},
      start:
        {Troupe.PropStore.File.Server, :start_link,
         [[troupe_name: troupe_name, dir: Keyword.get(opts, :props_dir, ".troupe")]]}
    }
  end

  def child_spec(:dets, opts, troupe_name) do
    %{
      id: {Troupe.PropStore.Dets.Server, troupe_name},
      start:
        {Troupe.PropStore.Dets.Server, :start_link,
         [[troupe_name: troupe_name, dir: Keyword.get(opts, :props_dir, ".troupe")]]}
    }
  end

  def child_spec({module, _opts}, _opts2, _troupe_name) when is_atom(module), do: nil

  @doc "troupe 组建完成后解析出行为实现句柄。"
  @spec resolve(kind(), atom()) :: {module(), ctx}
  def resolve(:memory, troupe_name) do
    table = :persistent_term.get({Troupe.PropStore.Memory, troupe_name}, nil)
    {Troupe.PropStore.Memory, %{table: table}}
  end

  def resolve(:file, troupe_name),
    do: {Troupe.PropStore.File, %{server: Troupe.Names.lookup!(troupe_name, :prop_store)}}

  def resolve(:dets, troupe_name),
    do: {Troupe.PropStore.Dets, %{server: Troupe.Names.lookup!(troupe_name, :prop_store)}}

  def resolve({module, opts}, _troupe_name) when is_atom(module),
    do: {module, module.init(opts)}
end

defmodule Troupe.PropStore.Memory do
  @moduledoc """
  内存 PropStore（ETS）：Rehearsal / 测试用，无持久化。表由本模块的 Holder 进程
  持有（随 Troupe 监督树生灭）。

  单键写入方在实践中是唯一（该键的 ActorServer 进程），CAS 以读-比-写实现；
  并发写同一键时非原子裁决——这是内存测试存储的已知取舍（Troupe.js 同款）。
  """

  use GenServer
  @behaviour Troupe.PropStore

  # -- Holder（监督树子进程） ---------------------------------------------

  def start_link(opts) do
    name = Keyword.fetch!(opts, :troupe_name)
    GenServer.start_link(__MODULE__, name, name: Troupe.Names.via(name, :prop_store))
  end

  @impl true
  def init(troupe_name) do
    table =
      :ets.new(:"troupe_props_#{System.unique_integer([:positive])}", [
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])

    :persistent_term.put({__MODULE__, troupe_name}, table)
    {:ok, %{table: table, troupe_name: troupe_name}}
  end

  @impl true
  def terminate(_reason, %{table: table, troupe_name: name}) do
    :persistent_term.erase({__MODULE__, name})
    if :ets.info(table) != :undefined, do: :ets.delete(table)

    :ok
  end

  # -- 行为实现 -------------------------------------------------------------

  @impl true
  def read(%{table: table}, key) do
    case :ets.lookup(table, key) do
      [{^key, props, revision, sv}] ->
        {:ok, %{props: props, revision: revision, schema_version: sv}}

      [] ->
        {:ok, nil}
    end
  end

  @impl true
  def create(%{table: table}, key, props, sv) do
    if :ets.insert_new(table, {key, props, 1, sv}) do
      {:ok, %{props: props, revision: 1, schema_version: sv}}
    else
      {:error, {:conflict, :exists}}
    end
  end

  @impl true
  def write(%{table: table}, key, props, opts) do
    expected = Keyword.fetch!(opts, :expected_revision)
    sv = Keyword.fetch!(opts, :schema_version)

    case read(%{table: table}, key) do
      {:ok, %{revision: ^expected}} ->
        new_rev = expected + 1
        :ets.insert(table, {key, props, new_rev, sv})
        {:ok, %{props: props, revision: new_rev, schema_version: sv}}

      {:ok, %{revision: other}} ->
        {:error, {:conflict, :revision, current: other}}

      {:ok, nil} ->
        {:error, {:conflict, :revision, current: nil}}
    end
  end

  @impl true
  def delete(%{table: table}, key) do
    :ets.delete(table, key)
    :ok
  end
end

defmodule Troupe.PropStore.File.Server do
  @moduledoc false
  use GenServer

  def start_link(opts) do
    name = Keyword.fetch!(opts, :troupe_name)
    GenServer.start_link(__MODULE__, opts, name: Troupe.Names.via(name, :prop_store))
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    dir = Keyword.get(opts, :dir, ".troupe")
    props_dir = :filename.join(String.to_charlist(dir), ~c"props")
    File.mkdir_p!(props_dir)
    {:ok, %{dir: props_dir}}
  end

  @impl true
  def handle_call({:read, key}, _from, st) do
    {:reply, lookup(st, key), st}
  end

  def handle_call({:create, key, props, sv}, _from, st) do
    case lookup(st, key) do
      {:ok, nil} ->
        store(st, key, props, 1, sv)
        {:reply, {:ok, %{props: props, revision: 1, schema_version: sv}}, st}

      {:ok, _existing} ->
        {:reply, {:error, {:conflict, :exists}}, st}
    end
  end

  def handle_call({:write, key, props, opts}, _from, st) do
    expected = Keyword.fetch!(opts, :expected_revision)
    sv = Keyword.fetch!(opts, :schema_version)

    case lookup(st, key) do
      {:ok, %{revision: ^expected}} ->
        store(st, key, props, expected + 1, sv)
        {:reply, {:ok, %{props: props, revision: expected + 1, schema_version: sv}}, st}

      {:ok, %{revision: other}} ->
        {:reply, {:error, {:conflict, :revision, current: other}}, st}

      {:ok, nil} ->
        {:reply, {:error, {:conflict, :revision, current: nil}}, st}
    end
  end

  def handle_call({:delete, key}, _from, st) do
    _ = :file.delete(path(st, key))
    {:reply, :ok, st}
  end

  # -- 存储：每键一个 term 快照文件，tmp + 原子改名 --------------------------

  defp lookup(st, key) do
    case :file.read_file(path(st, key)) do
      {:ok, bin} ->
        {props, rev, sv} = :erlang.binary_to_term(bin)
        {:ok, %{props: props, revision: rev, schema_version: sv}}

      {:error, :enoent} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp store(st, key, props, rev, sv) do
    p = path(st, key)
    tmp = :filename.join(st.dir, String.to_charlist(".tmp-#{System.unique_integer([:positive])}"))
    :ok = :file.write_file(tmp, :erlang.term_to_binary({props, rev, sv}), [:sync])
    :ok = :file.rename(tmp, p)
    :ok
  end

  defp path(st, key) do
    fname =
      key
      |> :erlang.term_to_binary()
      |> Base.url_encode64(padding: false)

    :filename.join(st.dir, String.to_charlist(fname <> ".props"))
  end
end

defmodule Troupe.PropStore.File do
  @moduledoc """
  本地持久 PropStore（默认）：每键一个 term 快照文件（`./.troupe/props/<key>.props`），
  写入 = tmp 文件 + 原子改名（同卷原子，Windows/Linux 均验证）。零依赖。

  **进程级持久**——担保进程崩溃/重启后恢复；**不担保**节点丢失与扩缩容再平衡
  （数据在原 Stage 磁盘上）。多节点生产必须切换到共享存储（节点级 PropStore），
  这是持久级别的界限。

  所有操作经单个 GenServer 串行化，CAS 因此是原子的。
  `fencing_token` 参数被接受以遵循契约；本实现单写者模型下不校验，
  共享存储 Provider（Postgres 等）应实现 token 拒绝。
  """

  @behaviour Troupe.PropStore

  @impl true
  def read(%{server: pid}, key), do: GenServer.call(pid, {:read, key})

  @impl true
  def create(%{server: pid}, key, props, sv), do: GenServer.call(pid, {:create, key, props, sv})

  @impl true
  def write(%{server: pid}, key, props, opts), do: GenServer.call(pid, {:write, key, props, opts})

  @impl true
  def delete(%{server: pid}, key), do: GenServer.call(pid, {:delete, key})
end

defmodule Troupe.PropStore.Dets.Server do
  @moduledoc false
  use GenServer

  def start_link(opts) do
    name = Keyword.fetch!(opts, :troupe_name)
    GenServer.start_link(__MODULE__, opts, name: Troupe.Names.via(name, :prop_store))
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    dir = Keyword.get(opts, :dir, ".troupe")
    File.mkdir_p!(dir)

    path =
      Keyword.get(opts, :path) || :filename.join(String.to_charlist(dir), ~c"props.dat")

    tid = :"troupe_dets_props_#{System.unique_integer([:positive])}"

    case :dets.open_file(tid, path: path, repair: true, auto_save: 1_000) do
      {:ok, ^tid} ->
        {:ok, %{tid: tid}}

      {:error, reason} ->
        raise Troupe.StoreError, store: "DetsPropStore", op: "open", cause: reason
    end
  end

  @impl true
  def handle_call({:read, key}, _from, st) do
    {:reply, lookup(st.tid, key), st}
  end

  def handle_call({:create, key, props, sv}, _from, st) do
    if :dets.insert_new(st.tid, {key, props, 1, sv}) do
      :dets.sync(st.tid)
      {:reply, {:ok, %{props: props, revision: 1, schema_version: sv}}, st}
    else
      {:reply, {:error, {:conflict, :exists}}, st}
    end
  end

  def handle_call({:write, key, props, opts}, _from, st) do
    expected = Keyword.fetch!(opts, :expected_revision)
    sv = Keyword.fetch!(opts, :schema_version)

    case lookup(st.tid, key) do
      {:ok, %{revision: ^expected}} ->
        :dets.insert(st.tid, {key, props, expected + 1, sv})
        :dets.sync(st.tid)
        {:reply, {:ok, %{props: props, revision: expected + 1, schema_version: sv}}, st}

      {:ok, %{revision: other}} ->
        {:reply, {:error, {:conflict, :revision, current: other}}, st}

      {:ok, nil} ->
        {:reply, {:error, {:conflict, :revision, current: nil}}, st}
    end
  end

  def handle_call({:delete, key}, _from, st) do
    :dets.delete(st.tid, key)
    :dets.sync(st.tid)
    {:reply, :ok, st}
  end

  @impl true
  def terminate(_reason, st) do
    :dets.close(st.tid)
    :ok
  end

  defp lookup(tid, key) do
    case :dets.lookup(tid, key) do
      [{^key, props, rev, sv}] -> {:ok, %{props: props, revision: rev, schema_version: sv}}
      [] -> {:ok, nil}
    end
  end
end

defmodule Troupe.PropStore.Dets do
  @moduledoc """
  `:dets` 本地持久 PropStore（可选）。

  ⚠️ 部分 Windows + OTP 组合上 `:dets.open_file/2` 本身不稳定（ArgumentError），
  因此默认本地持久实现为 `Troupe.PropStore.File`；本实现保留给 Linux 服务器等
  `:dets` 可用的环境（`store: :dets`）。
  """

  @behaviour Troupe.PropStore

  @impl true
  def read(%{server: pid}, key), do: GenServer.call(pid, {:read, key})

  @impl true
  def create(%{server: pid}, key, props, sv), do: GenServer.call(pid, {:create, key, props, sv})

  @impl true
  def write(%{server: pid}, key, props, opts), do: GenServer.call(pid, {:write, key, props, opts})

  @impl true
  def delete(%{server: pid}, key), do: GenServer.call(pid, {:delete, key})
end

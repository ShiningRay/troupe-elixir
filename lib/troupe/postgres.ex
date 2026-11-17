defmodule Troupe.PropStore.Postgres do
  @moduledoc """
  PostgreSQL 生产 PropStore（对齐 trouperb `PostgresPropStore` 与 troupe.cr 的单语句
  CAS 模式——Issue #3 的共享存储闭环核心）：

  * **cross_node_safe**：Props / 所有权 / Cue 三表共库；CAS 与 fencing 在单条
    `UPDATE … WHERE` 内原子完成；租约与到期判定全部用**数据库服务器时钟**（now()），
    客户端时钟漂移不参与；
  * **epoch 单调**：接管（他人取得）epoch + 1；同 owner 重入不涨；release 保留
    epoch 墓碑（租约拨到过去，fencing 不回退，DESIGN §6.5）；
  * **旧 epoch 写一律拒绝**：epoch 匹配 + 租约有效都在 UPDATE 的 WHERE 中（fencing
    权威是 ownership 表——props 行的 fence 只随写入前移，不回读作 WHERE，否则接管者
    首次写会被旧行挡住）；0 行受影响后做精确诊断（fencing 优先于 CAS）；
  * token 0（无所有权开发流）退化为纯 CAS；
  * 写结果不确定（网络超时）时由调用方以 read 校验 revision 决定，不盲目重写。

  ## 依赖（可选）

  本框架零第三方依赖；本 Provider 需要**用户应用**提供 `postgrex`（JSON 列以 TEXT
  存放，无需 Postgrex 类型扩展）与 `jason`（postgrex 自带 jason 依赖）。
  Props/payload 经 JSON 往返：**keys 为 binary string**（与 Troupe.js 的 JSON 语义
  对齐；atom-key props 需自行转换）。

  用法：

      {:ok, troupe} =
        Troupe.form(
          actors: [MyApp.CartActor],
          store: {Troupe.PropStore.Postgres, [url: "postgresql://troupe:troupe@127.0.0.1:55444/troupe"]},
          ownership: true,
          cues: [provider: :store],
          cluster: [seeds: [:"a@host"], mode: :production]
        )
  """

  @behaviour Troupe.PropStore

  @lease_default_ms 15_000

  # -- 监督树：Postgrex 连接（pool） ------------------------------------------

  # 非 behaviour callback：由 Troupe.PropStore.child_spec 委托
  def child_spec(opts, troupe_name) do
    ensure_postgrex!()

    url = Keyword.fetch!(opts, :url)
    pool_size = Keyword.get(opts, :pool_size, 4)

    %{
      id: {Postgrex, troupe_name},
      start:
        {Postgrex, :start_link,
         [
           [
             name: Troupe.Names.via(troupe_name, :pg_conn),
             hostname: hostname_of(url),
             port: port_of(url),
             username: userinfo_of(url)[:username] || System.get_env("PGUSER") || "postgres",
             password: userinfo_of(url)[:password] || System.get_env("PGPASSWORD"),
             database: dbname_of(url),
             pool_size: pool_size
           ]
         ]}
    }
  end

  # resolve 阶段构建 ctx：连接经注册名查找（监督树已启动）
  def init(opts) do
    %{conn: nil, opts: opts}
  end

  # Troupe.PropStore.resolve 传 troupe_name 时调用（本 Provider 需要查连接）
  def init(_opts, troupe_name) do
    %{conn: Troupe.Names.lookup!(troupe_name, :pg_conn), opts: []}
  end

  @impl Troupe.PropStore
  def read(%{conn: conn}, key) do
    case q(conn, "SELECT props, revision, schema_version, fence FROM troupe_ex_props WHERE actor_key = $1", [key(key)]) do
      %{rows: [[props, revision, sv, fence]]} ->
        {:ok, %{props: json_decode(props), revision: revision, schema_version: sv, fence: fence}}

      %{rows: []} ->
        {:ok, nil}

      other ->
        {:error, {:read, other}}
    end
  end

  # 创建：以取得所有权后的 fencing token 为初始 epoch（Issue #1 顺序契约——
  # 本 Provider 总在所有权持有后被调用；冲突（已存在）= 并发激活竞态，调用方读取采纳）
  @impl Troupe.PropStore
  def create(%{conn: conn}, key, props, schema_version, opts) do
    token = Keyword.get(opts, :fencing_token, 0)
    k = key(key)

    case q(conn,
           "INSERT INTO troupe_ex_props (actor_key, props, revision, schema_version, fence, epoch) " <>
             "VALUES ($1, $2, 1, $3, $4, $4) ON CONFLICT (actor_key) DO NOTHING",
           [k, json_encode(props), schema_version, token]
         ) do
      %{num_rows: 1} ->
        {:ok, %{props: json_encode_return(props), revision: 1, schema_version: schema_version}}

      _ ->
        {:error, {:conflict, :exists}}
    end
  end

  @impl Troupe.PropStore
  def write(%{conn: conn}, key, props, opts) do
    k = key(key)
    expected_revision = Keyword.fetch!(opts, :expected_revision)
    sv = Keyword.fetch!(opts, :schema_version)
    token = Keyword.get(opts, :fencing_token, 0)

    {rows, kind} =
      if token > 0 do
        # 单条 UPDATE：CAS + fencing（ownership 表 epoch 匹配且租约有效）原子完成
        %{num_rows: rows} =
          q(conn,
            "UPDATE troupe_ex_props SET props = $2, revision = revision + 1, schema_version = $3, " <>
              "fence = GREATEST(fence, $4), epoch = $4 " <>
              "WHERE actor_key = $1 AND revision = $5 AND $4 > 0 " <>
              "AND (SELECT epoch FROM troupe_ex_ownership WHERE actor_key = $1 " <>
              "     AND lease_expires_at > now()) = $4",
            [k, json_encode(props), sv, token, expected_revision]
          )

        {rows, :fenced}
      else
        # 无所有权开发流：退化为纯 CAS（fence 单调前移）
        %{num_rows: rows} =
          q(conn,
            "UPDATE troupe_ex_props SET props = $2, revision = revision + 1, schema_version = $3, " <>
              "fence = GREATEST(fence, $4) WHERE actor_key = $1 AND revision = $5",
            [k, json_encode(props), sv, token, expected_revision]
          )

        {rows, :cas}
      end

    cond do
      rows == 1 ->
        # 读回权威 revision（写后读，结果确定——UPDATE 返回行即成功）
        case read(%{conn: conn}, key) do
          {:ok, stored} -> {:ok, stored}
          {:error, _} -> {:error, :post_write_read}
        end

      kind == :fenced ->
        # 精确诊断：fencing 优先于 CAS（对齐 cr docs/persistence.md）
        if fencing_valid?(conn, k, token) do
          {:error, {:conflict, :revision}}
        else
          {:error, {:conflict, :fencing, token: token}}
        end

      true ->
        {:error, {:conflict, :revision}}
    end
  end

  @impl Troupe.PropStore
  def delete(%{conn: conn}, key) do
    k = key(key)
    q(conn, "DELETE FROM troupe_ex_props WHERE actor_key = $1", [k])
    q(conn, "DELETE FROM troupe_ex_ownership WHERE actor_key = $1", [k])
    :ok
  end

  @impl Troupe.PropStore
  def cross_node_safe, do: true

  # -- 共享所有权 / Cue 上下文（form 期由 Troupe 解析） ------------------------

  @doc "共享所有权 Provider 与本 PropStore 同库同事务模型。"
  def ownership_module, do: Troupe.Ownership.Postgres

  @doc "所有权 ctx 复用同一连接（单库）。接受 resolve 产物 {module, ctx} 或裸 ctx。"
  def ownership_ctx({Troupe.PropStore.Postgres, ctx}), do: ownership_ctx(ctx)
  def ownership_ctx(%{conn: conn}), do: %{conn: conn, lease_ms: @lease_default_ms}

  @doc "共享 CueStore 驱动 ctx（门面按 %{impl: mod} 分发）。接受 resolve 产物或裸 ctx。"
  def cue_ctx({Troupe.PropStore.Postgres, ctx}), do: cue_ctx(ctx)
  def cue_ctx(%{conn: conn}), do: %{impl: Troupe.CueStore.Postgres, conn: conn}

  # -- schema / 诊断 ------------------------------------------------------------

  @doc "建表（幂等）。child_spec 不自动执行——测试/部署脚本显式调用。"
  def ensure_schema(%{conn: conn}) do
    Enum.each(schema_sql(), &q(conn, &1, []))
    :ok
  end

  def ensure_schema(conn) when is_pid(conn) do
    Enum.each(schema_sql(), &q(conn, &1, []))
    :ok
  end

  @doc "PostgreSQL 版本串（测试验收真实拓扑用）。"
  def server_version(%{conn: conn}), do: one(conn, "SELECT version()")
  def server_version(conn) when is_pid(conn), do: one(conn, "SELECT version()")

  @doc "所有权行快照（测试/诊断）：{epoch, owner, lease_until_ms} | nil。"
  def ownership_row(%{conn: conn}, key) do
    ownership_row(conn, key)
  end

  def ownership_row(conn, key) do
    case one_row(conn, "SELECT epoch, owner, lease_expires_at FROM troupe_ex_ownership WHERE actor_key = $1", [
           key(key)
         ]) do
      nil ->
        nil

      [epoch, owner, ts] ->
        {epoch, owner, ms_of(ts)}
    end
  end

  @doc "把所有权租约拨到过期（测试接管窗口用）。"
  def expire_ownership!(%{conn: conn}, key), do: expire_ownership!(conn, key)

  def expire_ownership!(conn, key) do
    q(conn, "UPDATE troupe_ex_ownership SET lease_expires_at = to_timestamp(0) WHERE actor_key = $1", [key(key)])
    :ok
  end

  # -- 内部 ------------------------------------------------------------------

  defp fencing_valid?(conn, k, token) do
    case one_row(
           conn,
           "SELECT epoch, lease_expires_at FROM troupe_ex_ownership WHERE actor_key = $1 AND lease_expires_at > now()",
           [k]
         ) do
      [^token, _ts] -> true
      _ -> false
    end
  end

  defp schema_sql do
    [
      "CREATE TABLE IF NOT EXISTS troupe_ex_ownership (" <>
        "actor_key TEXT PRIMARY KEY, epoch BIGINT NOT NULL, owner TEXT NOT NULL, " <>
        "lease_expires_at TIMESTAMPTZ NOT NULL)",
      "CREATE TABLE IF NOT EXISTS troupe_ex_props (" <>
        "actor_key TEXT PRIMARY KEY, props TEXT NOT NULL, revision BIGINT NOT NULL, " <>
        "schema_version INTEGER NOT NULL, fence BIGINT NOT NULL DEFAULT 0, epoch BIGINT NOT NULL DEFAULT 0)",
      "CREATE TABLE IF NOT EXISTS troupe_ex_cues (" <>
        "id TEXT PRIMARY KEY, namespace TEXT NOT NULL, role_id TEXT NOT NULL, stage_name TEXT NOT NULL, " <>
        "name TEXT NOT NULL, payload TEXT, schedule TEXT NOT NULL, next_due BIGINT NOT NULL, " <>
        "status_version BIGINT NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'active', " <>
        "owner TEXT, lease_until TIMESTAMPTZ)",
      "CREATE INDEX IF NOT EXISTS troupe_ex_cues_due_idx ON troupe_ex_cues (status, next_due)"
    ]
  end

  # actor_key：JSON 数组编码（无 \0，可读、可 LIKE 前缀清理）
  def key({ns, role_id, stage_name}), do: json_encode([ns, role_id, stage_name])

  defp q(conn, sql, params) when is_pid(conn), do: Postgrex.query!(conn, sql, params)

  defp one(conn, sql) do
    %{rows: [[v]]} = Postgrex.query!(conn, sql, [])
    v
  end

  defp one_row(conn, sql, params) do
    case Postgrex.query!(conn, sql, params).rows do
      [row | _] -> row
      [] -> nil
    end
  end

  defp ms_of(%DateTime{} = dt) do
    DateTime.to_unix(dt, :millisecond)
  end

  defp json_encode(term) do
    ensure_jason!()
    Jason.encode!(term)
  end

  # create 成功路径免二次往返：以本地值回填 stored
  defp json_encode_return(props), do: json_decode(json_encode(props))

  defp json_decode(nil), do: nil

  defp json_decode(bin) when is_binary(bin) do
    ensure_jason!()
    Jason.decode!(bin)
  end

  defp ensure_postgrex! do
    unless Code.ensure_loaded?(Postgrex) do
      raise ArgumentError,
            "Troupe.PropStore.Postgres requires the :postgrex package " <>
              "(add {:postgrex, \"~> 0.17\"} to your deps — the troupe package itself stays dependency-free)"
    end
  end

  defp ensure_jason! do
    unless Code.ensure_loaded?(Jason) do
      raise ArgumentError, "Troupe.PropStore.Postgres requires the :jason package (postgrex pulls it in)"
    end
  end

  # -- URL 解析（零依赖；仅支持 postgresql://user:pass@host:port/db） ------------

  defp hostname_of(url) do
    uri = URI.parse(url)
    uri.host || "127.0.0.1"
  end

  defp port_of(url) do
    case URI.parse(url).port do
      nil -> 5432
      p -> p
    end
  end

  defp dbname_of(url), do: String.trim_leading(URI.parse(url).path || "/postgres", "/")

  defp userinfo_of(url) do
    case URI.parse(url).userinfo do
      nil ->
        %{}

      info ->
        [user, pass] = String.split(info, ":", parts: 2)
        %{username: user, password: pass}
    end
  end
end

defmodule Troupe.Ownership.Postgres do
  @moduledoc """
  PostgreSQL 共享所有权（跨节点单激活仲裁，DB 时钟）：

  * **acquire**：单条 INSERT ON CONFLICT——他人持有效租约 → `{:error, :busy}`；
    租约过期或无记录 → epoch 单调 +1 取得；同 owner 重入不涨 epoch；
  * **renew**：owner + epoch + 租约有效三者 WHERE 校验；
  * **release**：保留 epoch 墓碑（lease 拨到 to_timestamp(0)），fencing 不回退；
  * **cross_node_safe**：production 集群单激活证明的前提（能力门禁）。
  """

  @behaviour Troupe.Ownership

  @impl Troupe.Ownership
  def acquire(%{conn: conn}, key, owner, lease_ms) do
    case Postgrex.query!(
           conn,
           "INSERT INTO troupe_ex_ownership (actor_key, epoch, owner, lease_expires_at) " <>
             "VALUES ($1, 1, $2, now() + make_interval(secs => $3::float8)) " <>
             "ON CONFLICT (actor_key) DO UPDATE SET " <>
             "  epoch = CASE WHEN troupe_ex_ownership.owner = EXCLUDED.owner " <>
             "               THEN troupe_ex_ownership.epoch ELSE troupe_ex_ownership.epoch + 1 END, " <>
             "  owner = EXCLUDED.owner, " <>
             "  lease_expires_at = EXCLUDED.lease_expires_at " <>
             "WHERE troupe_ex_ownership.lease_expires_at <= now() OR troupe_ex_ownership.owner = EXCLUDED.owner " <>
             "RETURNING epoch",
           [Troupe.PropStore.Postgres.key(key), owner_str(owner), lease_ms / 1000.0]
         ).rows do
      [[epoch]] -> {:ok, epoch}
      [] -> {:error, :busy}
    end
  end

  @impl Troupe.Ownership
  def renew(%{conn: conn}, key, owner, epoch, lease_ms) do
    case Postgrex.query!(
           conn,
           "UPDATE troupe_ex_ownership SET lease_expires_at = now() + make_interval(secs => $4::float8) " <>
             "WHERE actor_key = $1 AND owner = $2 AND epoch = $3 AND lease_expires_at > now()",
           [Troupe.PropStore.Postgres.key(key), owner_str(owner), epoch, lease_ms / 1000.0]
         ) do
      %{num_rows: 1} -> :ok
      _ -> {:error, :lost}
    end
  end

  @impl Troupe.Ownership
  def release(%{conn: conn}, key, owner, epoch) do
    # 释放但保留 epoch 墓碑：下次接管 epoch 单调递增
    Postgrex.query!(
      conn,
      "UPDATE troupe_ex_ownership SET lease_expires_at = to_timestamp(0) " <>
        "WHERE actor_key = $1 AND owner = $2 AND epoch = $3",
      [Troupe.PropStore.Postgres.key(key), owner_str(owner), epoch]
    )

    :ok
  end

  @impl Troupe.Ownership
  def cross_node_safe, do: true

  # owner：ActorServer 传 pid；TEXT 列存稳定字符串（跨节点可区分）
  defp owner_str(pid) when is_pid(pid), do: :erlang.pid_to_list(pid) |> List.to_string()
  defp owner_str(term), do: inspect(term)
end

defmodule Troupe.CueStore.Postgres do
  @moduledoc """
  PostgreSQL 共享 CueStore（跨节点 reminders，DB 时钟；对齐 trouperb `PostgresCueStore`）：

  * **due**：active 且到期，或 running 但租约已过期（宕机重投）——全部 DB 时钟判定；
  * **claim**：`FOR UPDATE SKIP LOCKED` 行锁 + WHERE 守卫（active / running 租约过期）
    + statusVersion CAS——多 Stage 同时扫描不互相阻塞，争用失败 `{:error, :stolen}`；
  * **complete**：statusVersion CAS；`:done` 移除 / 整数推进 nextDue；
  * 至少一次语义不变：投递失败不确认，租约到期重投。
  """

  alias Troupe.CueStore

  # 门面 ctx = %{impl: __MODULE__, conn: conn}

  def upsert(%{conn: conn}, key, name, payload, spec, next_due) do
    {ns, role_id, stage_name} = key

    Postgrex.query!(
      conn,
      "INSERT INTO troupe_ex_cues (id, namespace, role_id, stage_name, name, payload, schedule, next_due) " <>
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) " <>
        "ON CONFLICT (id) DO UPDATE SET namespace = EXCLUDED.namespace, role_id = EXCLUDED.role_id, " <>
        "stage_name = EXCLUDED.stage_name, name = EXCLUDED.name, payload = EXCLUDED.payload, " <>
        "schedule = EXCLUDED.schedule",
      [
        id(key, name),
        ns,
        role_id,
        stage_name,
        name,
        payload && json_encode(payload),
        encode_spec(spec),
        next_due
      ]
    )

    :ok
  end

  def due(%{conn: conn}, _now_ignored, limit) do
    # DB 时钟：客户端 now 不参与（跨节点时钟漂移免疫）
    Postgrex.query!(
      conn,
      "SELECT id, namespace, role_id, stage_name, name, payload, schedule, next_due, " <>
        "status_version, status FROM troupe_ex_cues " <>
        "WHERE next_due <= (extract(epoch from now()) * 1000)::bigint " <>
        "AND (status = 'active' OR (status = 'running' AND lease_until <= now())) " <>
        "ORDER BY next_due LIMIT $1",
      [limit]
    ).rows
    |> Enum.map(&row_to_record/1)
  end

  def claim(%{conn: conn}, id, owner, lease_ms) do
    case Postgrex.query!(
           conn,
           "UPDATE troupe_ex_cues SET status = 'running', owner = $2, " <>
             "lease_until = now() + make_interval(secs => $3::float8), status_version = status_version + 1 " <>
             "WHERE id IN (SELECT id FROM troupe_ex_cues WHERE id = $1 FOR UPDATE SKIP LOCKED) " <>
             "AND (status = 'active' OR (status = 'running' AND lease_until <= now())) " <>
             "RETURNING status_version",
           [id, owner_str(owner), lease_ms / 1000.0]
         ).rows do
      [[sv]] -> {:ok, sv}
      [] -> {:error, :stolen}
    end
  end

  def complete(%{conn: conn}, id, sv, :done) do
    %{num_rows: 1} =
      Postgrex.query!(
        conn,
        "DELETE FROM troupe_ex_cues WHERE id = $1 AND status_version = $2",
        [id, sv]
      )

    :ok
  end

  def complete(%{conn: conn}, id, sv, next_due) when is_integer(next_due) do
    case Postgrex.query!(
           conn,
           "UPDATE troupe_ex_cues SET status = 'active', owner = NULL, lease_until = NULL, " <>
             "next_due = GREATEST($3, (extract(epoch from now()) * 1000)::bigint), " <>
             "status_version = status_version + 1 " <>
             "WHERE id = $1 AND status_version = $2",
           [id, sv, next_due]
         ) do
      %{num_rows: 1} -> :ok
      _ -> {:error, {:stale, nil}}
    end
  end

  def cancel(%{conn: conn}, key, name) do
    Postgrex.query!(conn, "DELETE FROM troupe_ex_cues WHERE id = $1", [id(key, name)])
    :ok
  end

  def list(%{conn: conn}) do
    Postgrex.query!(
      conn,
      "SELECT id, namespace, role_id, stage_name, name, payload, schedule, next_due, " <>
        "status_version, status FROM troupe_ex_cues ORDER BY next_due",
      []
    ).rows
    |> Enum.map(&row_to_record/1)
  end

  # -- 内部 ------------------------------------------------------------------

  defp row_to_record([id, ns, role_id, stage_name, name, payload, schedule, next_due, sv, status]) do
    %CueStore{
      id: id,
      key: {ns, role_id, stage_name},
      name: name,
      payload: payload && json_decode(payload),
      spec: decode_spec(schedule),
      next_due: next_due,
      sv: sv,
      owner: status,
      created_at: 0
    }
  end

  defp encode_spec({:at, ts}), do: "at:#{ts}"
  defp encode_spec({:every, ms}), do: "every:#{ms}"

  defp decode_spec("at:" <> ts), do: {:at, String.to_integer(ts)}
  defp decode_spec("every:" <> ms), do: {:every, String.to_integer(ms)}

  defp id({ns, role_id, stage_name}, name), do: "#{ns}:#{role_id}:#{stage_name}:#{name}"

  defp owner_str(pid) when is_pid(pid), do: :erlang.pid_to_list(pid) |> List.to_string()
  defp owner_str(term), do: inspect(term)

  defp json_encode(term) do
    Code.ensure_loaded?(Jason) || raise(ArgumentError, "Troupe PG provider requires :jason")
    Jason.encode!(term)
  end

  defp json_decode(bin) when is_binary(bin) do
    Code.ensure_loaded?(Jason) || raise(ArgumentError, "Troupe PG provider requires :jason")
    Jason.decode!(bin)
  end
end

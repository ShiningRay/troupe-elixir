defmodule Troupe.CallTimeoutError do
  @moduledoc """
  调用超时（响应期限与执行占用分离）。

  * `phase: :queued` —— 排队过期：Call 从未开始执行、零副作用，可安全重试。
  * `phase: :executing` —— 执行中超时（本地观察到的是调用方放弃）：**结果未知**，
    执行占用保持到原执行 settle，后续 Turn 不放行。
  """
  defexception [:phase, :role_id, :stage_name, :method, message: "call timed out"]

  @impl true
  def message(e) do
    case e.phase do
      :queued ->
        "call #{e.role_id}.#{e.method} (#{e.stage_name}) timed out while queued" <>
          " — never executed, safe to retry"

      :executing ->
        "call #{e.role_id}.#{e.method} (#{e.stage_name}) timed out while executing" <>
          " — outcome unknown, retry with an idempotency key"
    end
  end
end

defmodule Troupe.UnknownOutcomeError do
  @moduledoc """
  执行结果未知：超时 / 连接断开 / 对端停机。**勿假定失败**——按幂等键重试。
  """
  defexception [:role_id, :stage_name, :method, message: "call outcome unknown"]

  @impl true
  def message(e) do
    "outcome of #{e.role_id}.#{e.method} (#{e.stage_name}) is unknown" <>
      " (timeout/disconnect) — do not assume failure; retry with an idempotency key"
  end
end

defmodule Troupe.CallRejectedError do
  @moduledoc """
  请求拒绝（非业务异常）：背压 / 排空 / 白名单 / 停机 / 漂移 / 未取得所有权。

  `retryable` 指示调用方按 reason 决定重试还是修复。
  """
  defexception [
    :reason,
    :role_id,
    :stage_name,
    :method,
    :detail,
    retryable: false,
    message: "call rejected"
  ]

  @impl true
  def message(e) do
    "call #{e.role_id}.#{e.method} (#{e.stage_name}) rejected: #{e.reason}" <>
      if(e.detail, do: " (#{e.detail})", else: "") <>
      if e.retryable, do: " [retryable]", else: ""
  end
end

defmodule Troupe.ConflictError do
  @moduledoc """
  存储冲突（CAS 合同）。`kind: :exists` = create 时记录已存在；
  `kind: :revision` = write 时期望 revision 不匹配或记录缺失。
  调用方（ActorServer）收到后默认实例失效，下次调用从最后已提交快照恢复。
  """
  defexception [:kind, :role_id, :stage_name, message: "props conflict"]

  @impl true
  def message(e),
    do: "props conflict (#{e.kind}) for #{e.role_id}/#{e.stage_name} — instance invalidated"
end

defmodule Troupe.IdempotencyConflictError do
  @moduledoc "幂等键冲突：同键不同参数指纹。修正调用方 bug，而不是重试。"
  defexception [:key, :fingerprint, message: "idempotency key reused with different fingerprint"]

  @impl true
  def message(e),
    do:
      "idempotency key #{inspect(e.key)} already used with a different fingerprint " <>
        "(had #{inspect(e.fingerprint)})"
end

defmodule Troupe.UnknownRoleError do
  @moduledoc "cast 了未注册的角色。错误信息列出当前 Repertoire。"
  defexception [:module_or_role, :repertoire, message: "unknown role"]

  @impl true
  def message(e),
    do: "unknown role #{inspect(e.module_or_role)} — repertoire: #{inspect(e.repertoire)}"
end

defmodule Troupe.RoleConflictError do
  @moduledoc "角色名冲突：两个模块解析出同一 roleId，启动 fail fast。"
  defexception [:role_id, :modules, message: "role conflict"]

  @impl true
  def message(e),
    do: "role #{e.role_id} declared by multiple actor modules: #{inspect(e.modules)}"
end

defmodule Troupe.ActivationError do
  @moduledoc "激活失败：加载、迁移、校验或 on_stage 回调抛错。实例进失效，下次调用重试。"
  defexception [:role_id, :stage_name, :cause, message: "activation failed"]

  @impl true
  def message(e),
    do: "activation of #{e.role_id}/#{e.stage_name} failed: #{Exception.message(e.cause)}"
end

defmodule Troupe.BadReturnError do
  @moduledoc "Actor 方法返回了框架无法解释的形状。"
  defexception [:module, :method, :got, message: "bad return value"]

  @impl true
  def message(e),
    do: "#{inspect(e.module)}.#{e.method}/1 returned an unsupported shape: #{inspect(e.got)}"
end

defmodule Troupe.StoreError do
  @moduledoc "PropStore/CueStore 操作失败（存储不可用等）。保存失败 → 实例失效。"
  defexception [:store, :op, :cause, message: "store failure"]

  @impl true
  def message(e),
    do: "#{e.store}.#{e.op} failed: #{Exception.message(e.cause)}"
end

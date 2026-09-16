# 核心概念

## Virtual Actor 模型

| 概念 | 命名 | 说明 |
|---|---|---|
| 集群 | Troupe | 整个系统；`Troupe.form/1` 组团 |
| 节点 | Stage | 一个 BEAM 节点 = 一个 Stage |
| 虚拟 Actor | Actor | 逻辑上永远存在；`(role_id, stage_name)` 全局寻址；实现为纯函数模块 |
| 代理 | Agent | `Troupe.cast/3` 返回的 `%Troupe.Agent{}`；方法调用即消息（Call） |
| 激活/钝化 | OnStage / OffStage | 首条 Call 自动登台；空闲 Intermission 自动下场 |
| 状态 | Props | Actor 的持久化状态（不可变 map）；显式 `{:save, ...}` 提交 |
| 存储 | PropStore | Memory（ETS）/ File（默认，进程级）/ 共享 Provider（契约接入） |
| 消息信箱 | CallBoard | BEAM 进程信箱 + 显式调度账本（pending/in_flight） |
| 定时 | Cue / Reminder | 内存 Cue（声明式）/ 持久提醒（CueStore，at-least-once） |
| 流 | Show / Audience | `:pg` 话题化 pub/sub；Audience = Actor 订阅或外部 {m, f, args} 代理 |
| 管理 | Director / CLI | `Troupe.Director` + `mix troupe.*` |
| 测试 | Rehearsal | `Troupe.Case`：全内存、每测试隔离、async 安全 |

## 执行模型（严格串行 + Turn）

```
Call 到达 → 归属判定（HashRing；漂移转发 ≤2 跳）
  → 白名单校验（方法面 = 导出函数 − 保留函数）
  → 激活（单飞）：load → 迁移 → 校验 → 所有权 → on_stage
  → 入 CallBoard（信箱 + 账本）
  → 按 Turn 执行：strict 独占；标注方法可交错
  → 空闲超过 Intermission → 排空 → off_stage → 落盘 → 移除
```

关键契约：

1. **排队过期不执行**：deadline 已过的排队 Call 直接拒绝（`Troupe.CallTimeoutError` phase: `:queued`），零副作用，可安全重试
2. **执行占用**：调用方超时收到的是"结果未知"——**旧 Turn 继续执行完毕**，串行通道不放行；后续调用在旧 Turn settle 后才执行
3. **生命周期状态机**：未激活 → 激活中 → 运行中 → 排空中 → 失效；激活单飞；排空期间新请求停泊（`drain_policy: :wait` 默认）或明确拒绝（`:reject`）
4. **保存失败即失效**：`{:save, ...}` 失败/CAS 冲突 → 实例失效，下次调用从最后已提交快照重新加载——不会基于未提交状态继续服务

## 交错执行（opt-in）

默认**严格串行**。声明 `read_only:` / `improv:` 的方法可交错：

| 组合 | 可否交错 | Elixir 语义 |
|---|---|---|
| 未标注 × 任何 | ❌（独占） | — |
| `read_only` × `read_only` | ✅ | 各自并发进程持快照执行（真并行） |
| `improv` × 标注方法 | ✅ | 并发执行；提交按**完成序整 Props 覆盖** |

防护：strict 方法入队后停止启动新标注 Turn（写不被读饿死）；strict 启动仍按 FIFO。

⚠️ **improv 不提供跨多步一致性**：并发 improv 的提交按完成序覆盖整个 Props——
需要一致性的变更请用 strict，或把变更压缩为一次提交。

## 数据边界（本地/远程/测试一致）

所有 Call 经 BEAM 消息传递——**参数与返回值天然拷贝、数据不可变**：

* 调用后修改传入对象、或修改拿到的结果对象，都不影响 Actor 内部状态（不可能影响）
* Props 中禁止 pid/reference/function（存盘不可序列化；校验靠 `validate_props/1` 约定）

推论：**本地、远程、Rehearsal 三种模式数据行为天然一致**（Troupe.js 用 Codec 手工保证的语义）。

## 错误语义

| 错误 | 含义 | 调用方动作 |
|---|---|---|
| `CallTimeoutError(phase: :queued)` | 排队过期，从未执行 | 安全重试 |
| `UnknownOutcomeError` | 执行中超时/断连，**结果未知** | 按幂等键重试；勿假定失败 |
| `CallRejectedError(reason:, retryable:)` | 请求拒绝（背压/排空/停机/白名单/漂移/所有权） | 按 reason 决定重试或修复 |
| `ConflictError` | CAS 冲突 | 实例已自动失效；重试从新快照开始 |
| `IdempotencyConflictError` | 同幂等键不同参数指纹 | 修正调用方 bug |
| `ActivationError` | 加载/迁移/校验/on_stage 失败 | 实例已失效；修复后重试 |
| 业务异常 | 方法 raise，原样回传（stack 不回传） | 业务处理 |

`Troupe.call/4` 返回 `{:ok, reply} | {:error, exception}`；`Troupe.call!/4` 解包 raise。

## 持久成功与幂等

* `{:save, reply, props}`：**持久成功**语义——提交成功后才回复
* `Troupe.Actor.idempotent/4`：幂等执行。同键同指纹重放原结果；同键异指纹抛
  `IdempotencyConflictError`；**fn 内只改内存**——由方法以 `{:save, ...}` 返回，
  状态 + 幂等记录原子落盘
* 跨 Actor 流程无事务：失败路径显式业务补偿（或 outbox）

# Troupe for Elixir 设计总览

> 状态 v0.1 · 2026-09-16。本文档是 Troupe.js（DESIGN.md v2 / PLAN.md v4）到 Elixir/BEAM 的移植设计：
> 概念映射、执行模型、契约保真与**有意偏差清单**。命名体系沿用剧团词典。

---

## 1. 定位与设计原则（承袭 Troupe.js）

1. **简单优先**：集群元数据零外部依赖；Roster/探测直接委托 Erlang distribution
2. **零配置启动，渐进式披露**：`Troupe.form(actors: [...])` 即可运行；每个旋钮有默认值
3. **剧团隐喻命名**：Troupe/Stage/Actor/Role/Agent/OnStage/OffStage/Repertoire/Playbill/
   Roster/Casting/CallBoard/Call/Props/PropStore/Cue/CueStore/Improv/Intermission/Show/
   Audience/Rehearsal/Director
4. **约定优于配置，但错误必须响**：角色冲突、未注册角色、未知方法声明、白名单外方法均 fail-fast
5. **无代码生成**：方法即导出函数，调用面在 form 期由模块导出表推导
6. **承诺必须可验证**：执行/失败/持久成功/所有权契约对应测试场景（见 `test/`）

## 2. 概念映射

| 概念 | Troupe.js | Elixir 版实现 |
|---|---|---|
| 集群 | Troupe 进程内单例 | `%Troupe{}` 运行时（监督树），支持同节点多实例（测试互不污染） |
| 节点 | Stage（进程） | BEAM 节点（`--sname/--name`） |
| Actor | class + 实例 | **纯函数模块**（`use Troupe.Actor`），无实例、无进程内可变状态 |
| 激活 | StageManager new Actor | `Troupe.Casting` 单飞启动 `Troupe.ActorServer`（GenServer） |
| Turn 调度 | 手工 CallBoard FIFO | 进程信箱 + 账本（pending/in_flight + pending_strict 计数） |
| 串行 | 上一 Turn settle 前不取下一条 | GenServer 语义（天然） |
| Agent | Proxy 对象 | `%Troupe.Agent{}` 纯数据（可序列化、可存 Props） |
| Props | JSON 可序列化对象 | 不可变 map（term 存储，禁止 pid/ref/fun） |
| PropStore | InMemory/SQLite/Postgres | Memory（ETS）/ **File**（默认，快照+原子改名）/ Dets（可选）/ behaviour |
| Cue（内存） | registerCue 闭包 | 声明式 `cues: [method: every]`，Tick 经调度串行执行 |
| 持久提醒 | CueStore + at-least-once | `Troupe.CueStore` + `Troupe.CueRunner`（claim 租约 + statusVersion CAS） |
| Show/Audience | 自建 pub/sub + TCP 转发 | OTP `:pg`（进程组，天然跨节点） |
| Roster | gossip 反熵 + ping/ACK 探测 | `Node.list()` + `:net_kernel.monitor_nodes`（distribution 心跳） |
| Playbill | xxh32 HashRing 转发 | `:erlang.phash2` HashRing（160 vnodes）+ erpc 转发（≤2 跳） |
| Director | 系统角色 + CLI 瘦客户端 | `Troupe.Director` + `mix troupe.*` 任务（erpc + cookie 鉴权边界） |
| Rehearsal | NODE_ENV=test 全内存 | `Troupe.Rehearsal` + `Troupe.Case`（ExUnit，async 安全） |
| 交错执行 | @Improv/@ReadOnly 事件循环交错 | 标注方法派发到并发进程（快照 + 完成序提交），strict 独占 |

## 3. Actor 编程模型：纯函数

Troupe.js 的 swap 守则第一条是"状态全在 props"；Elixir 的不可变数据让它成为语言事实：

```elixir
def add(props, item) do                      # props 快照入参
  {:save, :ok, Map.update!(props, :items, &[item | &1])}
end
```

* 无实例字段、无 `this`、无隐式共享 → 不存在"跨 await 中间态"之外的污染路径
* 方法可在**无进程**的情况下单测（Rehearsal 覆盖生命周期/并发/持久化）
* 生命周期回调同样纯：`on_stage/2`、`off_stage/2` 返回 `{:ok, props}`（可带 effects）

### 返回形状

| 返回 | 语义 |
|---|---|
| `{:reply, result}` | 回复，Props 不变 |
| `{:reply, result, props}` | 回复 + 内存提交 |
| `{:reply, result, props, [effects: [...]]}` | 同上 + effects |
| `{:save, result, props}` | **持久成功**：CAS 落盘成功后才回复 |
| read_only 方法 | 裸值（快照只读，改动天然不生效） |

### Effects（副作用的声明式出口）

纯函数不产生副作用，方法返回后由框架执行：
`{:reminder, name, spec}`、`{:cancel_reminder, name}`、`{:publish, topic, event}`、
`{:subscribe, topic}`、`{:unsubscribe, topic}`。
对应 Troupe.js 的 `this.scheduleReminder/publishShow/subscribe`。

## 4. 执行模型（契约保真）

`Troupe.ActorServer`（舞台监督）承载 DESIGN §5.1/§5.4 的全部契约：

```
Call 到达（本地 Agent / erpc 远程 / 提醒 / Show 事件）
  → Playbill 判定归属（HashRing）；误投经 erpc 转发（≤2 跳）
  → 白名单校验（方法面 = 导出函数 − 保留函数；生命周期钩子不可经 RPC）
  → deadline 检查：已过期 → CallTimeoutError(:queued)，零副作用
  → 激活（Casting 单飞）：load → schemaVersion 迁移 → validate → 所有权 → on_stage
  → 分类调度：
      strict   : 账本空闲 → 本进程内联执行；否则 FIFO 入队（队首阻塞）
      annotated: 无 strict 排队 → 派发并发 Task（快照执行，完成序提交）
                 strict 排队 → 封锁新标注 Turn（写饥饿防护）
  → 完成：提交/落盘/effects/回复；排队项中已过期的直接拒绝（不执行）
  → 空闲超过 Intermission → 排空（off_stage → CAS 落盘 → 停止）
      排空中到达的请求：drain_policy :wait 停泊 → 排空后重新激活服务
```

关键契约与测试对应：

| 契约 | 场景 | 测试 |
|---|---|---|
| 激活单飞 | 并发首调只登台一次 | `CoreTest`（Butterfly on_stage 恰一次） |
| 串行性 | 并发 100 add 无交错（最后提交覆盖即反证） | `CoreTest` count==100 |
| 执行占用 | 调用方超时后旧 Turn 继续到 settle，B 不与 A 交错 | `CoreTest` 事件顺序 |
| 排队过期不执行 | deadline 过的排队 Call 零副作用拒绝 | `CoreTest` |
| 保存失败即失效 | 下次调用从最后已提交快照恢复 | `PersistenceTest`（FlakyStore 注入） |
| CAS 冲突 | 外部并发写入 → ConflictError + 失效 | `PersistenceTest` |
| 迁移 | schemaVersion 逐级 +1，产物落盘；失败 = 激活失败 | `PersistenceTest` |
| 组合矩阵 | RO×RO / improv×annotated 并行；strict 独占 | `InterleaveTest` |
| 写饥饿防护 | strict 入队后封锁新标注 Turn（FIFO） | `InterleaveTest` |
| 提醒 | 下场后触发重新登台；at 一次性完成移除；cancel 竞争 | `CuesShowTest` |
| cueSkipped | Tick 迟到跳拍计数 | `CuesShowTest` |
| Show | on_stage effects 订阅；内部 Call 投递（串行）；Audience 代理 | `CuesShowTest` |
| 幂等 | 同键同指纹重放；同键异指纹拒绝；记录随 Props 原子提交 | `PersistenceTest` |

### 交错执行语义（有意收紧处）

JS 的 improv 在 await 点交错并共享 `this.props` 对象；Elixir 无共享可变对象，
标注方法的语义为：

* **read_only**：并发进程持 Props 快照执行——真并行、零串行通道占用
* **improv**：并发进程持快照执行，提交按**完成序整 Props 覆盖**（last-commit-wins）
  ——跨多步一致性请用 strict，或把变更压缩为一次提交
* 写饥饿防护、strict FIFO 与 JS 语义一致

## 5. 持久化

PropStore 契约（revision CAS + schemaVersion 分离 + 可选 fencing token）不变。

* **Memory**（ETS）：Rehearsal/测试
* **File**（默认）：每键一个 term 快照文件，tmp + 同卷原子改名；GenServer 串行化 → CAS 原子。
  零依赖，Windows/Linux 均验证
* **Dets**（可选）：OTP `:dets`。⚠️ 本项目开发所用的某 Windows/OTP 组合上 `:dets.open_file/2`
  本身不稳定（ArgumentError），故默认不启用，保留给 Linux 服务器环境
* 持久级别界限不变：进程级（File/Dets）担保进程重启恢复，不担保节点丢失/扩缩容；
  多节点生产必须接共享存储（节点级 Provider，见"未移植"）

## 6. 集群

* **放置**：`phash2({ns, role, name})` → HashRing（160 vnodes/节点）。phash2 跨节点恒定，
  保证放置一致
* **成员**：`[node() | Node.list()]`；nodeup/nodedown 推送 Playbill 重建环。
  故障探测 = net_kernel 心跳（不自建 gossip/SWIM——distribution 即成员协议）
* **路由**：本地直调；远端 `:erpc.call(owner, Troupe.Router, :enter, ...)`；
  漂移期转发 ≤2 跳，超限 `CallRejectedError(reason: :drift, retryable: true)`
* **所有权**：`Troupe.Ownership` behaviour（acquire/renew/release + 单调 epoch），
  激活前置、lease/3 续期、丢失即排空让位、epoch 随 write 传给 PropStore（fencing）。
  内置 Memory 实验实现；生产多节点需共享存储实现
* **无协调模式**（默认）定位**实验性**：允许双活，revision CAS 防静默丢更新，
  不承诺强一致——与 DESIGN §6.5 一致

### 错误分类（wire 兼容 JS 语义）

`CallTimeoutError(phase: :queued/:executing)`、`UnknownOutcomeError`（勿假定失败）、
`CallRejectedError(reason/retryable)`、`ConflictError`、`IdempotencyConflictError`、
`ActivationError`、业务异常原样回传（BEAM term 序列化，stack 不回传）。

`Troupe.call/4` 返回 `{:ok, reply} | {:error, exception}`；`call!` 解包 raise。

## 7. 可观测

* **Metrics**：每角色 calls/errors/rejections/cue_skipped + 延迟环形缓冲（1024）→ p50/p99/max
* **Trace**：调用事件环形缓冲（10_000，节点级）+ 实时订阅（`{:troupe_trace, event}` 消息）
* **Director**：status/roster/actors/inspect/invoke/off/cues/ring/methods + 集群扇出
* **CLI**：`mix troupe.{status,roster,ps,inspect,call,off,cues,ring} [--node a@host] [--json]`

## 8. 有意偏差清单（相对 Troupe.js）

| 偏差 | 理由 |
|---|---|
| 方法 = 纯函数 `(props, ...args) -> {reply, props}` | BEAM 不可变数据；"状态全在 props"的语言化 |
| `initial_props` 为模块函数 `initial_props/0`（非 static 工厂） | 同上；仍在保留名单（不入 RPC 面） |
| improv 提交 = 完成序整 Props 覆盖 | 无共享可变对象；JS 跨 await 中间态语义的 BEAM 化 |
| read_only 并行于并发进程（JS 为同线程交错） | 不可变数据使快照读严格更安全 |
| Roster/探测 = Erlang distribution | 心跳/断连是运行时原生能力，不自建协议 |
| 传输 = erpc term 序列化（非 TCP+JSON/Codec） | 消息即拷贝：本地/远程/Rehearsal 天然同一边界 |
| 持久化默认 File（非 SQLite） | OTP 无内置 SQLite；快照+原子改名零依赖且够进程级 |
| 内存 Cue 声明式（cues: [method: every]） | 纯函数模型下闭包注册不适用；覆盖攒批/游戏循环模式 |
| 背压深度作用于显式账本 | strict 在线执行期间的洪峰由 BEAM 信箱吸收（信箱即 CallBoard）；信箱级探测列为后续 |
| 提醒存储节点级 | 与默认 PropStore 持久级别一致；共享 CueStore 随共享 PropStore 里程碑接入 |

## 9. 未移植（后续里程碑）

* BoxOffice（WS 客户端网关）、Booth（Web 面板）——需要 Web 服务器依赖，破坏零依赖
* dev 热重载 / 生产热替换（L1/L2）——BEAM 有 `:code.load/2` 与 release_handler；
  虚拟 Actor 使"滚动重启 + 按需重激活"（L0）即默认能力
* OpenTelemetry 桥（`onSpan` 等价的 `on_span` 回调预留于架构，未接线）
* 节点级共享 PropStore/CueStore（Postgres/Redis）与生产级所有权 fencing 校验
* Extra（每 Stage 池化群众演员）、BoxOffice ticket、cron 表达式提醒（当前 :at/:every）
* Show 持久订阅与至少一次投递升级（当前尽力扇出）

# Troupe for Elixir 与 Orleans 的能力差距

> 对标 Orleans 10.0；统一规划：[troupe-planning](https://github.com/ShiningRay/troupe-planning)。

## 当前定位

本仓具备可测试的虚拟 Actor 单机核心：按需激活、GenServer 严格串行、标注快照并发、Props CAS/迁移、Intermission、Cue、`:pg` Show、Director/CLI 和 Erlang distribution 路由。BEAM 提供进程、监督、分布式消息等优势，但这些优势不自动提供 Orleans 的目录、持久状态、共享 reminders、可靠流和故障接管语义。

## 差距与证据

| 领域 | 当前实现 | 必要差距 |
|---|---|---|
| Lifecycle | GenServer、激活/失效/下场、迁移、校验 | 先修所有权前的写入；再定义 activation incarnation 与生命周期扩展契约 |
| 调度 | strict 串行；read_only/improv 快照并发 | 与 Orleans 的 turn/reentrancy 并非完全同义；需补超时取消与信箱总预算 |
| 集群 | Erlang distribution、HashRing、erpc 转发 | 缺生产角色协商、共享目录/放置、真实多节点故障验收 |
| Ownership | Memory 租约/epoch | 无跨节点仲裁；File/DETS 不校验 fencing，不能作为生产单激活证明 |
| Props | Memory/File/DETS，revision CAS | 缺共享 Provider；迁移写缺少 fencing；Provider 生态与版本化类型契约有限 |
| Cue | Memory/DETS，至少一次 claim | 节点级，不能跨节点恢复；需要共享 reminders 和原子调度 |
| Show | `:pg` 实时扇出，当前进程订阅 | 无持久事件、离线订阅恢复、游标、重放或队列平衡 |
| 观测 | Metrics、Trace、Director/CLI | 缺 Booth、OTel 接线、持久集群视图与持续 CI |
| 高级模块 | 未实现 | persistent streams、extensions、transactions、event sourcing 不应由幂等/CAS 冒充 |

## 缺陷与 Issues

- [#1 所有权前迁移写](https://github.com/ShiningRay/troupe-elixir/issues/1)：先 load/migrate，后 acquire，是共享存储上线前必须修复的顺序问题。
- [#2 Roster 角色校验声明未实现](https://github.com/ShiningRay/troupe-elixir/issues/2)：并补真实双节点路由/接管测试。
- [#3 共享 Props/Cue/fencing 生产闭环](https://github.com/ShiningRay/troupe-elixir/issues/3)：不能仅添加数据库连接便宣称容错。

## 语言特有意差

- `read_only` 的快照并行读适合 BEAM；`improv` 完成序整 Props 覆盖可能丢失并发增量，必须单独定义，不能称与 Orleans/JS 相同。
- 继续保留纯函数 Actor 和 OTP 监督，不照搬 .NET class/DI 模型。
- 普通 strict 执行期间的 BEAM 信箱需要显式背压策略，不能只限制内部 pending 账本。

详细步骤与门禁见 [ROADMAP.md](./ROADMAP.md)。

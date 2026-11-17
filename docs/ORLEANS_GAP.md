# Troupe for Elixir 与 Orleans 的能力差距

> 对标 Orleans 10.0；统一规划：[troupe-planning](https://github.com/ShiningRay/troupe-planning)。

## 当前定位

本仓具备可测试的虚拟 Actor 单机核心 + **生产共享存储闭环**（2026-11-17）：按需激活、
GenServer 严格串行、标注快照并发、Props CAS/迁移、Intermission、Cue、`:pg` Show、
Director/CLI、Erlang distribution 路由；**Issue #1–#3 已闭环**——所有权先于一切共享写
（fencing 贯穿 create/migrate/save）、Roster 入团协商与 production 门禁、PostgreSQL
Provider（Props/Ownership/Cue 同库、单语句 CAS+fencing、DB 时钟租约、SKIP LOCKED claim）。
双真实 BEAM 节点验收（透明路由、协商拒绝、死亡重激活）入 CI。

BEAM 提供进程、监督、分布式消息等优势，但这些优势不自动提供 Orleans 的目录演进、
可靠流、事务与版本化类型契约。

## 差距与证据

| 领域 | 当前实现 | 剩余差距 |
|---|---|---|
| Lifecycle | 激活即单飞、fencing 前置、失败释放租约 | activation incarnation 可观测事件、生命周期扩展契约 |
| 调度 | strict 串行；read_only/improv 快照并发；排队过期；UnknownOutcome；**幂等键（记录随 Props 原子提交）**；**协作取消（advisory 置位/观察/settle 清除）** | 信箱总预算背压（strict 长任务期间的内存增长测量） |
| 集群 | Erlang distribution、HashRing、erpc 转发、**入团协商 + production 门禁** | 目录/放置演进（哈希 vs 版本化目录 ADR）、真实网络分区演练 |
| Ownership | Memory 租约 + **Postgres epoch 墓碑/DB 时钟租约**，fencing 拒绝旧写 | Redis/etcd 等其他共享 Provider（同一契约） |
| Props | Memory/File/DETS + **Postgres（CAS+fencing 单语句）** | 版本化类型契约、Provider 生态扩展 |
| Cue | Memory/DETS + **Postgres（SKIP LOCKED claim、DB 时钟重投）** | 归属过滤（跨节点按环过滤）、批量 claim |
| Show | `:pg` 实时扇出（明确定位尽力通知） | 持久事件、离线订阅恢复、游标/重放（独立设计，不复用 `:pg`） |
| 观测 | Metrics、Trace、Director/CLI、**OTel OTLP JSON 导出（零依赖 sink）**、CI（矩阵+PG+分布式 leg） | W3C traceparent 传播接线、Booth、持久集群视图 |
| 高级模块 | 未实现 | persistent streams、extensions、transactions、event sourcing 不应由幂等/CAS 冒充 |

## 已闭环（原 Issues）

- [#1 所有权前迁移写](https://github.com/ShiningRay/troupe-elixir/issues/1)——已修复：
  激活顺序改为 acquire → read → migrate（携 token 写）→ on_stage；失败释放租约。
- [#2 Roster 角色校验](https://github.com/ShiningRay/troupe-elixir/issues/2)——已实现：
  manifest 交换互检、production fail-fast、双节点 CI 验收（含 child_spec 传参缺陷修复）。
- [#3 共享 PropStore/CueStore 闭环](https://github.com/ShiningRay/troupe-elixir/issues/3)——已实现：
  PostgreSQL 三表共库；两节点不能同时服务同一 Actor；旧 epoch save/migrate/reminder
  写均被拒；崩溃后 Props/reminder 在另一节点恢复；自动化测试入 CI PG/分布式 leg。

## 语言特有意差

- `read_only` 快照并行读是 BEAM 红利；`improv` 完成序整 Props 覆盖的**丢失更新反例**
  已入测试（并发 improv 各自 +1 最终只计一次）——写增量必须走 strict 或标 read_only。
- 继续保留纯函数 Actor 和 OTP 监督，不照搬 .NET class/DI 模型。
- strict 执行期间的 BEAM 信箱背压仍待显式策略（排队过期与 queue_depth 只覆盖账本）。

详细步骤与门禁见 [ROADMAP.md](./ROADMAP.md)。

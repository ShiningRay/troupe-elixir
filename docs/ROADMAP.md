# Troupe for Elixir Orleans 对标路线图

## 0. 修正正确性与验证基线 —— ✅ 完成（2026-11-17）

- [x] [#1](https://github.com/ShiningRay/troupe-elixir/issues/1)：激活顺序改为取得所有权 → 读取 → 校验/逐级迁移 → 携 fencing 写 → on_stage；失败释放租约。
- [x] [#2](https://github.com/ShiningRay/troupe-elixir/issues/2)：manifest（协议/namespace/角色集合）交换互检；production 不兼容 fail-fast 断连；dev 记录 rejected；`Roster.child_spec` 传参缺陷修复。
- [x] CI 执行 `mix test`：矩阵（Elixir 1.15/1.17/1.18 × ubuntu + windows）、PG 必跑 leg、双真实 BEAM 节点分布式必跑 leg（skip 即失败纪律）。
- [ ] 统一 pending 队列与进程信箱的过载防护，测量 strict 长任务期间内存增长（Phase 2）。
- [x] 为 `improv` 的整 Props 覆盖补丢失更新反例和清晰使用边界（`Troupe.InterleaveTest` 反例 + Actor 文档）。

**门禁达成**：未持有 epoch 的创建/迁移/保存均被 PG Provider 拒绝（fencing 优先于 CAS 诊断）；
角色不兼容不入生产环；46 项测试全绿含真实双节点用例。

## 1. 共享状态、所有权与 reminders —— ✅ 完成（2026-11-17）

- [x] PostgreSQL：Props CAS、ownership fencing、CueStore 三表共库；单语句 `UPDATE … WHERE` 原子 CAS+fencing，不用应用层先读后写假装原子。
- [x] 迁移写与 save 统一携 fencing（`create/5` 契约 + `fencing_opts` 全路径覆盖）；接管 epoch 单调、release 留墓碑、DB 时钟租约。
- [x] 共享 CueStore：`FOR UPDATE SKIP LOCKED` 原子认领、租约、完成 statusVersion CAS、租约过期重投（至少一次）。
- [x] production 集群模式能力门禁：非 cross-node-safe 的 store/ownership 被拒绝；dev 保留零依赖体验。
- [x] [#3](https://github.com/ShiningRay/troupe-elixir/issues/3) 独立节点 kill、接管、旧 epoch 写拒绝均有自动化测试（PG leg + 分布式 leg）。

**门禁达成**：Props、所有权和 reminders 在节点故障后可恢复；旧 epoch 无法写入；
本地文件/内存状态不冒充节点级持久。

## 2. 调度、投递和生命周期契约 —— 部分完成

- [ ] 定义逻辑 Actor identity 与 activation incarnation 的关系，支持可观测 lifecycle。
- [x] timeout、排队过期、UnknownOutcome（结果未知勿假定失败）已有语义与测试；远程取消/幂等重试契约待补文档。
- [x] 纯函数/不可变 Props 保持；strict / read_only snapshot / improv commit 的一致性差异已用反例定义。
- [ ] 节点级健康、租约丢失和 drain 纳入 OTP 监督策略（租约丢失当前仅 drain 该 Actor）。

**门禁**：每种调度/失败组合有测试和可观察事件；长任务、慢存储和节点恢复不会悄悄越过串行/所有权约束。

## 3. 实时 Show 与持久流分轨

- [x] `:pg` Show 明确定位为在线、尽力通知（文档与 ORLEANS_GAP 已声明）。
- [ ] 若需 Orleans persistent streams，独立设计事件 ID、持久日志、订阅游标、ack/replay、保留、慢消费者和幂等。
- [ ] 优先复用成熟消息系统 Provider，不把 `:pg` 进程组扩展为伪持久日志。

**门禁**：实时 Show 不宣称持久；可靠模式需通过订阅者离线、发布者宕机、coordinator 迁移和重放验证。

## 4. 目录、放置与互操作（统一 ADR 后）

- [ ] 决定 HashRing+ownership 与版本化目录的演进关系；设计注册/缓存失效/视图交接/崩溃恢复。
- [ ] 利用 BEAM capabilities/资源指标提供可插拔 placement，而不是只按哈希选择。
- [ ] 与统一规划协商语言中立 identity、方法/类型 ID、schema、错误、deadline/cancellation 和 trace context。
- [ ] 不将 Erlang term 或 erpc 当成其他语言可直接接入的公共协议；外部协议与节点内传输分开。

## 5. 后续运维与高级模块

- [x] OTel OTLP JSON v1 导出（零依赖 sink 回调，`Troupe.OTel`，对齐 troupe.cr M6/trouperb）；W3C traceparent 传播接线待统一 ADR。
- [ ] Booth、发布/回滚和协议兼容矩阵。
- [ ] 可靠 Outbox 后再评估事件溯源与分布式事务；不由 CAS 推导 ACID 跨 Actor 事务。

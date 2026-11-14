# Troupe for Elixir Orleans 对标路线图

## 0. 修正正确性与验证基线

- [ ] [#1](https://github.com/ShiningRay/troupe-elixir/issues/1)：激活顺序改为取得所有权 → 读取 → 校验/逐级迁移 → 携 fencing 写 → on_stage；失败释放租约。
- [ ] [#2](https://github.com/ShiningRay/troupe-elixir/issues/2)：实现 production 协议/角色集合协商，修正 Roster 注释；不兼容 fail-fast。
- [ ] 建立 CI 执行 `mix test`，并新增两个真实 BEAM 节点的分布式测试任务。
- [ ] 统一 pending 队列与进程信箱的过载防护，测量 strict 长任务期间内存增长。
- [ ] 为 `improv` 的整 Props 覆盖补丢失更新反例和清晰使用边界。

**门禁**：未持有 epoch 的任何创建/迁移/保存均被存储拒绝；角色不兼容不能进入生产环；单机 37 项保持通过且新增分布式用例真实运行。

## 1. 共享状态、所有权与 reminders

- [ ] 优先 PostgreSQL：同一事务/行锁模型实现 Props CAS、ownership 和 fencing，不使用仅应用层先读后写假装原子。
- [ ] 迁移写与 save 统一携 fencing；覆盖并发迁移和冻结恢复。
- [ ] 共享 CueStore：原子 due claim、租约、完成 statusVersion、取消竞争、归属过滤与至少一次重投。
- [ ] production 拒绝 File/DETS 等无法提供节点级保证的 Provider；dev 保留零依赖体验。
- [ ] [#3](https://github.com/ShiningRay/troupe-elixir/issues/3) 以独立节点 kill、冻结、恢复验证接管和旧写拒绝。

**门禁**：Props、所有权和 reminders 在节点故障后可恢复；旧 epoch 无法写入；不会通过文件本地状态冒充节点级持久。

## 2. 调度、投递和生命周期契约

- [ ] 定义逻辑 Actor identity 与 activation incarnation 的关系，支持可观测 lifecycle。
- [ ] 明确 timeout、远程取消、排队过期、UnknownOutcome 和幂等重试；不默认把超时当业务失败。
- [ ] 保持纯函数/不可变 Props，分别定义 strict、read_only snapshot、improv commit 的一致性。
- [ ] 将节点级健康、租约丢失和 drain 纳入 OTP 监督策略，避免异常仅记录而持续服务。

**门禁**：每种调度/失败组合有测试和可观察事件；长任务、慢存储和节点恢复不会悄悄越过串行/所有权约束。

## 3. 实时 Show 与持久流分轨

- [ ] `:pg` Show 明确定位为在线、尽力通知，订阅进程不存在时不保证补投。
- [ ] 若需 Orleans persistent streams，独立设计事件 ID、持久日志、订阅游标、ack/replay、保留、慢消费者和幂等。
- [ ] 优先复用成熟消息系统 Provider，不把 `:pg` 进程组扩展为伪持久日志。

**门禁**：实时 Show 不宣称持久；可靠模式需通过订阅者离线、发布者宕机、coordinator 迁移和重放验证。

## 4. 目录、放置与互操作（统一 ADR 后）

- [ ] 决定 HashRing+ownership 与版本化目录的演进关系；设计注册/缓存失效/视图交接/崩溃恢复。
- [ ] 利用 BEAM capabilities/资源指标提供可插拔 placement，而不是只按哈希选择。
- [ ] 与统一规划协商语言中立 identity、方法/类型 ID、schema、错误、deadline/cancellation 和 trace context。
- [ ] 不将 Erlang term 或 erpc 当成其他语言可直接接入的公共协议；外部协议与节点内传输分开。

## 5. 后续运维与高级模块

- OTel/W3C trace、Booth、发布/回滚和协议兼容矩阵。
- 可靠 Outbox 后再评估事件溯源与分布式事务；不由 CAS 推导 ACID 跨 Actor 事务。

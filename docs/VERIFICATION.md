# 验证基线（2026-11-17）

环境：Windows、Elixir 1.20.4 / Erlang OTP 29；PostgreSQL 15.19（真实实例）。

## 测试结果

- `mix test`（单机）：**38 passed**——含 OTel 导出验收（span 映射/状态码/monotonic Sum/gauge/sink 闭环）——串行、生命周期、迁移、保存失败、交错、Cue 与 Show。
- `mix test`（含 PG，`TROUPE_TEST_PG`）：**43 passed**（+6，另有 1 项 improv 丢失更新反例在单机套件）——Provider 契约/CAS/fencing 拒绝、
  epoch 单调与墓碑、production 门禁、崩溃恢复、Cue claim/complete/租约过期重投（DB 时钟）。
- `mix test`（含分布式，`TROUPE_DIST_TEST=1`）：**51 passed**（+3，双真实 BEAM 节点）——
  透明调用跨节点路由（erpc 转发 + 环放置）、production 角色不兼容 fail-fast 断连、
  dev 模式拒绝记录、节点死亡后重激活与环自愈。

## 已验证的生产语义（对应 Issues #1–#3）

- 激活顺序：取得所有权 → 读取 → 校验/迁移 → **携 fencing 写** → on_stage（#1）；
  失败释放租约，未持有 epoch 的创建/迁移写被共享存储拒绝。
- 入团协商：manifest（协议/namespace/角色集合）互检；production 不兼容 fail-fast 断连，
  dev 记录 rejected 保持实验语义（#2）；`Troupe.Roster.child_spec` 传参缺陷已修复。
- 共享存储：`Troupe.PropStore.Postgres` 单语句 `UPDATE … WHERE` 原子 CAS+fencing；
  ownership epoch 单调、release 留墓碑、DB 时钟租约（#3）；Cue `FOR UPDATE SKIP LOCKED`
  原子认领、租约过期重投（至少一次）；production 集群模式对非 cross-node-safe
  store/ownership 门禁拒绝。
- OTel OTLP JSON v1 导出经 sink 闭环验收（spans 结构/状态映射/monotonic Sum/周期 metrics/stop 后积压丢弃）；improv 整 Props 覆盖的丢失更新反例已入测试（并发 improv 各自 +1 最终只计一次），
  使用边界见 `Troupe.Actor` 文档与 `Troupe.InterleaveTest`。

## 仍未验证的边界

- Show 仍为 `:pg` 实时尽力扇出；持久流/离线订阅恢复不在本轮。
- erlang distribution 跨 WAN/分区（net split 仅由节点死亡模拟）；真实网络分区演练未做。
- OTel 导出、Booth、协议兼容矩阵（ROADMAP Phase 4/5）。

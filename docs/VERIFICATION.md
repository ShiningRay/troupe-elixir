# 验证基线（2026-11-14）

环境：Windows、Elixir 1.20.4 / Erlang OTP 29，工作树 `f014c5f`（文档提交前）。

- `mix test`：**37 passed，0 failed**，耗时约 6.3 秒。
- 覆盖以单机核心为主：串行、生命周期、迁移、保存失败、交错、Cue 与 Show。
- 本轮未执行真实多 BEAM 节点路由/接管、共享存储 fencing、跨节点 Cue 重投或 Show 离线恢复；这些能力不得由 37 项通过推断。
- 源码审计另发现所有权前迁移写与 Roster 角色校验声明不一致，详见 [Issues #1–#3](https://github.com/ShiningRay/troupe-elixir/issues)。

本文是文档更新时的验证快照，不表示 Issues 已修复。

# Troupe（剧团）for Elixir

**Elixir 版 Virtual Actor 框架**——[Troupe.js](../troupejs)（面向 Node.js 的 Virtual Actor 框架，思想对标 Microsoft Orleans 与 EA Orbit）的 BEAM 移植。命名、API 与默认值全面围绕"剧团（Troupe）"隐喻，以**零配置开箱即用**为一级原则，**零第三方依赖**。

> Actor 永远存在：用 `(role_id, stage_name)` 全局寻址；首条 Call 到达时自动登台（OnStage），
> 空闲超过 Intermission 自动下场（OffStage）；故障即重激活，Props 从 PropStore 恢复。

## 为什么 Elixir 版天然契合

Troupe.js 在 Node 单线程上手工构建的机制，BEAM 直接免费提供：

| Troupe.js 手工构建 | Elixir 版的天然对应 |
|---|---|
| CallBoard（每 Actor FIFO 队列） | 进程信箱（本身就是 FIFO） |
| Turn 严格串行 | GenServer 一次处理一条消息 |
| Codec 复制边界（本地/远程/测试一致） | BEAM 消息传递天然拷贝、数据不可变 |
| TCP + JSON 传输 + gossip 成员协议 | Erlang distribution（`erpc` + `net_kernel` 心跳） |
| Show pub/sub | OTP `:pg`（进程组，天然分布式） |
| Actor 实例 + 原型 | **纯函数模块**：`(props, ...args) -> {reply, props}` |

## 快速开始

```elixir
# 定义演员：方法即 RPC，第一个参数是 Props 快照，返回 {回复, 新 Props}
defmodule MyApp.CartActor do
  use Troupe.Actor

  def initial_props, do: %{items: []}

  def add(props, item) do
    {:save, :ok, Map.update!(props, :items, &[item | &1])}   # 持久成功：落盘后才回复
  end

  def peek(props), do: {:reply, length(props.items), props}
end

# 组团、选角、调用
{:ok, troupe} = Troupe.form(actors: [MyApp.CartActor])
cart = Troupe.cast(troupe, MyApp.CartActor, "user-123")

:ok = Troupe.call!(cart, :add, ["apple"])
1 = Troupe.call!(cart, :peek)
```

要点：

* **无需显式创建 Actor**：`cast` 即全局寻址，首调自动激活，从 PropStore 恢复 Props
* **严格串行**：同一 Actor 一次一条 Call（Turn），并发 100 个 `add` 不会交错
* **持久成功**：`{:save, reply, props}` 原子提交成功后才回复；`{:reply, reply, props}` 只提交内存
* **故障即重激活**：进程崩溃后下一条 Call 在（同机或其它）Stage 重新登台

## 测试

```elixir
defmodule MyApp.CartTest do
  use ExUnit.Case, async: true
  use Troupe.Case, actors: [MyApp.CartActor]

  test "cart", %{troupe: troupe} do
    cart = Troupe.cast(troupe, MyApp.CartActor, "t1")
    assert {:ok, :ok} = Troupe.call(cart, :add, ["a"])
    assert {:ok, 1} = Troupe.call(cart, :peek, [])
  end
end
```

`use Troupe.Case`（Rehearsal，强制全内存）每测试一个隔离 troupe，`async: true` 安全。

## 集群（两个节点组团）

```bash
# 节点 A
elixir --sname a@127.0.0.1 -S mix
# 节点 B（seeds 指向 A）
TROUPE_SEEDS=a@127.0.0.1 elixir --sname b@127.0.0.1 -S mix
```

```elixir
{:ok, troupe} =
  Troupe.form(
    actors: [MyApp.CartActor],
    cluster: [seeds: [:"a@127.0.0.1"]],
    ownership: [lease_ms: 5_000]      # 多节点生产建议启用（防双活）
  )
```

放置 = 一致性哈希环（`phash2`，160 虚拟节点/节点），无目录、无注册；
漂移期误投经 erpc 转发（跳数上限 2）。入团时节点交换 manifest（协议/namespace/角色集合）：
**production 模式不兼容即 fail-fast 断连**，dev 记录拒绝、保持实验语义。

## 生产共享存储（PostgreSQL）

多节点生产必须用共享存储 Provider（节点级持久不等于跨节点单激活）：

```elixir
{:ok, troupe} =
  Troupe.form(
    actors: [MyApp.CartActor],
    store: {Troupe.PropStore.Postgres, [url: "postgres://troupe:troupe@db:5432/mydb"]},
    ownership: true,                  # 与 Props 同库：epoch 墓碑 + DB 时钟租约
    cues: [provider: :store],         # 共享 reminders（SKIP LOCKED 认领、至少一次）
    cluster: [seeds: [:"a@host"], mode: :production]
  )
```

* Props 写 = 单语句 `UPDATE … WHERE` 原子 CAS + fencing（旧 epoch 一律拒绝，
  fencing 优先于 CAS 诊断）；迁移写同契约
* 所有权 = epoch 单调、同 owner 重入不涨、release 留墓碑、租约判定用**数据库时钟**
* production 集群模式有**能力门禁**：非 cross-node-safe 的 store/ownership 直接拒绝组建
* Provider 需要 `postgrex`/`jason`（用户应用侧提供；troupe 包本身零依赖）

## 协作取消（advisory）

```elixir
# 方法内每步观察取消标志，自行决定收束（可提前 settle 提交部分状态）
def churn(props, max_steps) do
  steps = step(0, max_steps)
  {:save, steps, Map.put(props, :progress, steps)}
end
defp step(n, max) when n >= max, do: n
defp step(n, max) do
  if Troupe.Actor.cancel_requested?(), do: n, else: (Process.sleep(10); step(n + 1, max))
end
```

`Troupe.cancel(agent)` 置位标志：在飞/下一 Turn 内 `cancel_requested?/0` 返回 true，
Turn settle 后自动清除。**不可强行终止**（strict Turn 内联）；外部副作用不受保护。

## 可观测

```elixir
Troupe.metrics(troupe)                  # 每角色 calls/errors/rejections + p50/p99
Troupe.traces(troupe, 50)               # 调用事件环形缓冲
Troupe.Director.ps(troupe.name)         # 在场演员
Troupe.Director.inspect(troupe.name, "Cart", "user-123")   # Props 快照
Troupe.Director.off(troupe.name, "Cart", "user-123")       # 强制下场
```

### OTel 导出（OTLP JSON v1）

```elixir
stop =
  Troupe.OTel.attach_exporter(troupe, fn
    :trace, json -> http_post("http://collector:4318/v1/traces", json)   # 传输由宿主实现
    :metrics, json -> http_post("http://collector:4318/v1/metrics", json)
  end,
  every_ms: 10_000)
```

Trace 事件 → span（W3C traceparent 可选解析；outcome → status）、每角色计数 →
monotonic Sum、延迟分位 → gauge。JSON 编码零依赖（OTP 27+ 内置 `:json`，或用户侧 Jason）。

CLI：`mix troupe.status --node a@host`、`mix troupe.ps`、`mix troupe.inspect Cart user-123`、
`mix troupe.call Cart user-123 peek '[]'`、`mix troupe.ring` 等（需与目标节点共享 cookie）。

## 文档

* [DESIGN.md](DESIGN.md) —— Elixir 版设计总览：概念映射、执行模型、与 Troupe.js 的差异清单
* [docs/benchmarks.md](docs/benchmarks.md) —— 基准报告：消息吞吐与创建性能（原生 Process / GenServer / Actor × 本机/跨节点）
* [docs/getting-started.md](docs/getting-started.md) —— 快速上手
* [docs/concepts.md](docs/concepts.md) —— 核心概念与契约
* [docs/api-reference.md](docs/api-reference.md) —— API 参考
* [docs/operations.md](docs/operations.md) —— 部署与运维
* [docs/ORLEANS_GAP.md](docs/ORLEANS_GAP.md) / [ROADMAP](docs/ROADMAP.md) —— Orleans 对标差距、生产门禁与 Issues
* [docs/VERIFICATION.md](docs/VERIFICATION.md) —— 当前测试结果与未验证边界
* [docs/patterns.md](docs/patterns.md) —— 模式与配方（秒杀/延时任务/Show 广播）

## 安装

```elixir
def deps do
  [{:troupe, "~> 0.1.0"}]
end
```

要求 Elixir ~> 1.15（无任何第三方依赖）。

## 状态

v0.1.0：核心运行时（虚拟激活、严格串行、交错执行、生命周期、持久化、Cue/提醒、Show、Director/CLI）与设计契约已实现并有测试覆盖（47 例，含真实 PostgreSQL 与双 BEAM 节点）。Issues #1–#3 已闭环：所有权先于一切共享写（fencing 贯穿 create/migrate/save）、Roster 入团协商 + production 能力门禁、PostgreSQL Provider（Props/Ownership/Cue 同库、DB 时钟租约、SKIP LOCKED 重投）。Redis/etcd Provider、持久流、OTel 接线见 [ROADMAP](docs/ROADMAP.md)。

License: Apache-2.0

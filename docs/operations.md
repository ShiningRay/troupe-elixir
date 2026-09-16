# 运维手册

## 部署形态

| 形态 | 配置 | 保证 |
|---|---|---|
| 单机开发 | `Troupe.form(actors: [...])` | 本机，File 存储进程级持久 |
| 同机多 troupe | `Troupe.form(name: :app1, ...)` | 实例隔离，互不污染（测试友好） |
| 多节点 | `cluster: [seeds: [...]]` + `ownership: [...]` + 共享 PropStore | 节点级恢复；所有权交接（共享存储 Provider 接入后） |

⚠️ **无协调模式下的多节点集群定位为实验性**（与 Troupe.js DESIGN §6.5 一致）：
分区/扩缩容期间同一 Actor 可能双活，revision CAS 使冲突写入可检测（防静默丢更新），
不承诺强一致。强一致业务必须部署共享存储 PropStore 并启用 ownership。

## 节点与 cookie（集群前提）

Erlang distribution 即集群通道与鉴权边界：

```bash
# 节点 A（设置 cookie）
elixir --sname a@127.0.0.1 --cookie top-secret -S mix run --no-halt
# 节点 B
TROUPE_SEEDS=a@127.0.0.1 elixir --sname b@127.0.0.1 --cookie top-secret -S mix run --no-halt
```

* **受控网络 + 保密 cookie 是集群部署前提**（Director 管理面同样经 distribution）
* production 要素：固定节点名、必须成功入团（`cluster: [mode: :production, seeds: [...]]`
  在 seeds 全部不可达时启动失败）、角色集合一致性由 Repertoire 校验

## 所有权（防双活，实验性）

```elixir
Troupe.form(actors: [...], cluster: [...], ownership: [lease_ms: 5_000])
```

* 激活前置 acquire（租约）；lease/3 周期续约；丢失即排空让位
* epoch 作为 `fencing_token` 随 Props 写入传递给 PropStore——共享存储 Provider
  应拒绝旧 token（暂停后恢复的旧持有者写入被存储拒绝）；内置 Memory/File 实现单写者模型不校验
* ⚠️ 租约不保护**外部副作用**：HTTP/消息接收方需校验 epoch 或用 outbox

## 优雅停机

`Troupe.shutdown(troupe)`：广播 leave → 拒收新 Call → 存量 settle → 全员
offStage + saveProps → 停止运行时（宽限 `shutdown_grace`，默认 10s）。
BEAM release 的 SIGTERM 处理（heart/release_handler）由部署层接入此通道。

## 全局背压

* 每 Actor：显式账本深度（1024，`queue_depth`）→ 超限 `CallRejectedError(reason: :backpressure, retryable: true)`
* **边界**：strict 在线执行期间，后续消息滞留在 BEAM 信箱（信箱即 CallBoard），
  信箱级洪峰由 BEAM 队列吸收；信箱深度探测列为后续增强

## 可观测

* **Metrics**：`Troupe.metrics(troupe)`——每角色 calls/errors/rejections/cue_skipped + 延迟 p50/p99/max
* **Trace**：`Troupe.traces(troupe, 50, filter)`；`Troupe.subscribe_trace(filter)` 实时订阅
* **Director**：status / roster / actors / inspect / invoke / off / cues / ring（集群扇出聚合）
* **CLI**：`mix troupe.status --node a@host`、`mix troupe.ps --role Cart --json`、
  `mix troupe.call Cart user-123 peek '[]'`（本地默认 troupe 可省略 --node）

## 调用链

每次调用产 Trace 事件（调用方 → 被调方、方法、耗时、结果），节点级环形缓冲 10_000 条：

```elixir
Troupe.traces(troupe, 20, role_id: "Cart")
un = Troupe.subscribe_trace(method: :add)    # 实时（分布式 tail -f 的数据源）
```

Actor 内跨 Actor 调用自动携带调用方身份（`caller` 字段）——集群经 wire 透传。

## 基准

完整报告（消息吞吐 + 创建性能，含环境、口径、多轮数据与解读）见 [benchmarks.md](benchmarks.md)。

### 消息吞吐（`mix bench`）

度量同步往返吞吐（msg/s，每 msg = 请求 + 回复）与单客户端延迟（p50/p99/avg），
对比六个场景：原生 Process / GenServer（参考）/ Troupe Actor × 同节点 / 跨节点。

```bash
# 只跑同节点场景
mix bench --no-cluster

# 跨节点：终端 1 起 worker（承载跨节点目标）
elixir --name bench-worker@127.0.0.1 --cookie benchcookie -S mix run bench/benchmark.exs --role worker
# 终端 2 跑基准（结束时自动停止 worker；--keep-peer 可保留）
mix bench --node bench-main@127.0.0.1 --peer bench-worker@127.0.0.1
# 旋钮：--clients N（并发客户端，默认 8）、--target-ms（测量窗口，默认 1500）、--json
```

参考结果（Windows 10 / Elixir 1.20 / OTP 29 / 8 schedulers，单 Actor 串行通道）：

| 场景 | 吞吐 msg/s | 占同节点原生比 |
|---|---|---|
| 原生 Process · 同节点 | ~85 万 | 100% |
| 原生 Process · 跨节点 | ~2.4 万 | ~2.7% |
| GenServer · 同节点 | ~45 万 | ~52% |
| Troupe Actor · 同节点 | ~4.5–7 万 | ~5–8% |
| Troupe Actor · 跨节点 | ~7k–12k | ~0.8–1.2% |

注意：以上为**单热点 Actor**数字——串行 Turn 是容量事实；实际业务按 stageName 分片，
吞吐随 Actor 数水平扩展。跨节点 Troupe 的成本主要是 erpc 往返（每次调用一个对端代理进程）。

### 创建性能（`mix run bench/creation.exs`）

度量"诞生成本"：每秒创建数、单次 p50/p99、每实例进程内存：

```bash
mix run bench/creation.exs                                   # 本机场景
# 跨节点激活（worker 同消息基准）：
mix run bench/creation.exs --peer bench-worker@127.0.0.1
```

参考结果（同机环境，单节点 Casting 串行启动）：

| 场景 | 创建/s（顺序） | 创建/s（并发8） | KB/实例 |
|---|---|---|---|
| 原生 Process · spawn（即退/常驻） | ~58 万 / ~41 万 | ~105 万 / ~10 万 | 2.5 |
| [参考] GenServer · start | ~10 万 | ~6.5 万 | 2.6 |
| Troupe Actor · 激活（memory） | ~2.1 万 | ~1.9 万 | 9.2 |
| Troupe Actor · 激活（file 持久） | ~200 | ~190 | 7.2 |
| Troupe Actor · 激活（跨节点） | ~4.3 千 | ~8.8 千 | 3.6 |

要点：Actor 激活 ≈ 5× GenServer start（虚拟寻址 + 存储 + 生命周期全套）；
file 存储的激活成本由同步落盘主导（持久成功的代价，~4.6ms p50）；
并发激活经单节点 Casting 串行——激活吞吐的水平扩展靠多 Stage。

## 存储运维

* File 存储：`./.troupe/props/<key>.props`（term 快照）；备份 = 拷贝目录；
  单文件损坏只影响该 Actor（下次激活报 `StoreError` → 实例失效）
* 内存压力：Intermission 默认 5 分钟自动下场回收；显式批量下场用 `Troupe.drain_all/2`
* 键命名空间：`namespace` 选项 / `TROUPE_NAMESPACE`——多环境共用目录不串数据

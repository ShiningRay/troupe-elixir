# 基准报告：消息吞吐 与 创建性能

> 环境：Windows 10 x64 / Elixir 1.20.4 / OTP 29 / 8 schedulers / 8 逻辑核。
> 零依赖实现（File PropStore、`:pg`、erlang distribution）。
> 以下数字为该机器的**相对比较**参考，绝对值随硬件浮动；跨场景的比例关系更有意义。

---

## 1. 消息吞吐（`mix bench`）

### 度量口径

* **一个"消息" = 一次完整同步往返**（请求 + 回复各算一条消息）
* 吞吐 = N 并发客户端（默认 8）同时压测，自适应测量窗口（默认 ~1.5s）
* 延迟 = 单客户端顺序往返的 p50 / p99 / 平均（ns 采样，µs 呈现）
* 每场景先预热再测量；三轮运行取典型值

### 场景说明

| 场景 | 目标 |
|---|---|
| 原生 Process · 同节点 | `spawn` 的裸进程，`send` + `receive`（ref 匹配），零框架 |
| 原生 Process · 跨节点 | 同上，echo 进程 `spawn` 在远端 Stage，消息走 distribution |
| [参考] GenServer · 同/跨节点 | OTP 服务器 `handle_call`（monitor + 回复路由的基线） |
| Troupe Actor · 同节点 | `Troupe.call!` 完整路径：HashRing 放置判定 → Registry 查址 → 账本 → strict Turn |
| Troupe Actor · 跨节点 | 同上，目标 Actor 的归属在远端 Stage：erpc 转发 → 远端本地执行 |

### 实测（8 并发客户端，单热点 Actor/进程，多轮典型值）

| 场景 | 吞吐 msg/s | RTT avg µs | p99 µs | 占同节点原生 |
|---|---:|---:|---:|---:|
| 原生 Process · 同节点 | 70–93 万 | 9–19 | ~100 | 100% |
| 原生 Process · 跨节点 | 2.0–2.7 万 | 300–670 | 400–1000 | ~2.2–3.1% |
| [参考] GenServer · 同节点 | 40–48 万 | 17–34 | ~100 | ~43–56% |
| [参考] GenServer · 跨节点 | 0.8–1.3 万 | 730–1200 | 500–1200 | ~0.9–1.4% |
| **Troupe Actor · 同节点** | **3.7–7.1 万** | 113–234 | ~100–205 | **~4–8%** |
| **Troupe Actor · 跨节点** | **0.7–1.2 万** | 870–1370 | 820–1370 | **~0.8–1.3%** |

> 延迟分位注记：同节点场景 p50 常 <1µs（命中快路径），p99 由调度抖动主导（~100–200µs）；
> 跨节点场景 p50 即 ~205–410µs（distribution 往返下限）。

### 解读

1. **BEAM 原生下限极高**：裸进程往返 ~85 万 msg/s——这是所有框架开销的分母。
2. **GenServer 收取约一半**：monitor 建立与回复路由是 OTP 服务器的固有成本。
3. **Troupe Actor 同节点 ~4–8%**：在 GenServer 基线之上，每次调用还有放置判定（一次调度）、
   Registry 查址、白名单校验、Turn 账本记账。换取：虚拟寻址、按需激活、故障重激活、
   生命周期与持久化。**对绝大多数业务（Actor 逻辑本身 ≥ 微秒级），框架开销可忽略。**
4. **跨节点成本由 distribution 主导**：Troupe 跨节点（~0.9%）与原生跨节点（~2.7%）是同一量级
   ——远端框架附加值占比很小，瓶颈在网络往返。
5. **并发客户端数 8→16 时各场景吞吐同向变化**（原生 ~88 万，Actor 同节点 ~6.8 万），排名不变。

---

## 2. 创建性能（`mix run bench/creation.exs`）

### 度量口径

* **一次"创建" = 诞生一个全新实例**
  * 原生：`spawn`（即退 = 含退出回收；常驻 = 仅创建，结束后批量回收）
  * Actor：`cast + 首调冷启动`完整路径——寻址未命中 → Casting 单飞 → DynamicSupervisor
    启动 → 读/建初始 Props（file 场景含同步落盘）→ on_stage → 首个 Turn 执行
* 顺序创建给 p50/p99 与每实例进程内存增量（KB，不含存储文件）；并发 8 客户端给吞吐

### 实测（含跨节点激活）

| 场景 | 创建/s（顺序） | 创建/s（并发8） | p50 µs | p99 µs | KB/实例 |
|---|---:|---:|---:|---:|---:|
| 原生 Process · spawn（即退） | ~59 万 | ~105 万 | <1 | ~100 | — |
| 原生 Process · spawn（常驻） | ~41 万 | ~10 万 | <1 | ~100 | 2.5 |
| [参考] GenServer · start（常驻） | ~10 万 | ~6.5 万 | <1 | ~100 | 2.6 |
| **Troupe Actor · 激活（memory）** | **~2.1 万** | **~1.9 万** | <1 | ~100 | **9.2** |
| **Troupe Actor · 激活（file 持久）** | **~200** | **~190** | **4600** | **11500** | **7.2** |
| **Troupe Actor · 激活（跨节点）** | ~4.3 千 | ~8.8 千 | 205 | 720 | 3.6 |

### 解读

1. **Actor 激活 ≈ 5× GenServer start**（2.1 万 vs 10 万/s）："虚拟"的全部内容
   ——寻址、单飞、存储、生命周期——就是这 5 倍的所在。
2. **file 持久激活 ~200/s**：p50 4.6ms 几乎全是同步落盘（fsync）。这正是"持久成功"
   语义的价格；接受丢失窗口（RPO = 冲刷间隔）的业务应走攒批/内存级路径。
3. **内存 9.2KB/实例**（含 ActorServer 进程 + Props；不含存储文件）：
   10 万级在线 Actor ≈ 0.9GB——与 Troupe.js 基准（6.3KB/Actor）同一量级。
4. **跨节点激活并发（8.8k/s）> 顺序（4.3k/s）**：erpc 往返可流水重叠；
   激活本身在归属节点的 Casting 串行执行。

---

## 3. 容量事实与选型指引

| 事实 | 含义 |
|---|---|
| 单热点 Actor = 一条串行通道（~4–7 万 msg/s） | 热点业务按 stageName 分片（如按 SKU/房间/用户），吞吐随 Actor 数水平扩展 |
| 激活 ~2 万/s（memory）/ ~200/s（file 持久） | 突发冷启动洪峰（如全员同时上线）需要预热或分片；持久成功级别是 fsync 价 |
| 在线常驻 ~9.2KB/实例 | 十万级 Actor ~1GB 进程内存；Intermission（默认 5m）自动回收不活跃实例 |
| 跨节点任何操作 ≈ ms 级 | 跨节点调用/激活比本机贵 1–2 个数量级——把强互调的 Actor 放同节点（一致哈希按 key 前缀分片即可） |

## 4. 复现

```bash
# 消息吞吐（只跑同节点）
mix bench --no-cluster
# 消息吞吐（跨节点，两个终端；cookie 一致）
elixir --name bench-worker@127.0.0.1 --cookie benchcookie -S mix run bench/benchmark.exs --role worker
mix bench --node bench-main@127.0.0.1 --peer bench-worker@127.0.0.1

# 创建性能（本机）
mix run bench/creation.exs
# 创建性能（含跨节点激活，worker 同上）
mix run bench/creation.exs --peer bench-worker@127.0.0.1

# 通用旋钮：--clients N（并发，默认 8）、--target-ms（窗口，默认 1500）、--keep-peer
```

说明：

* 跨节点场景由 worker 终端承载远端目标（`:peer` 自动拉起节点在部分 Windows/OTP 组合
  不可用，故为手动双终端）；主节点结束时默认停止 worker（`--keep-peer` 保留）
* 基准的 ping/push 均**不做持久化写入**（除 file 激活场景的初始 Props 落盘），
  与 Troupe.js 的吞吐口径对齐；业务若每次调用持久化，按本机存储的 fsync 价另计
* 数字为开发机参考值，发布前建议在目标硬件复测（`--clients`/`--target-ms` 可调）

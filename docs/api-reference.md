# API 参考

## Troupe.form(opts)

| 选项 | 默认 | 说明 |
|---|---|---|
| `actors` | 必填 | Actor 模块列表（显式注册；fail-fast 校验） |
| `name` | `:troupe`（被占自动编号） | Troupe 逻辑名（跨节点一致） |
| `store` | `:file` | `:memory` / `:file` / `:dets` / `{module, init_opts}` |
| `namespace` | `"default"`（`TROUPE_NAMESPACE`） | 存储键命名空间 |
| `intermission` | `"5m"` | 空闲下场时长（整数 ms 或 `"10m"` 形式） |
| `default_timeout` | `"30s"`（`TROUPE_TIMEOUT`） | Call 超时；可被 `timeouts:` 按方法覆盖 |
| `queue_depth` | 1024（`TROUPE_QUEUE_DEPTH`） | 每 Actor 显式账本深度上限 |
| `drain_policy` | `:wait` | 排空期间新请求 `:wait` / `:reject` |
| `cues` | false | `true` 或 `[tick: ms, persist: :memory | :dets]`——启用持久提醒（内存 Cue 无需此项） |
| `show` | false | 启用 Show（`:pg` pub/sub） |
| `ownership` | 未启用 | `true` / `[lease_ms: 5_000]`——租约 + epoch（多节点生产建议） |
| `cluster` | 无 | `[seeds: [:"a@host"], mode: :dev | :production]` |
| `props_dir` | `".troupe"`（`TROUPE_PROPS_DIR`） | File/Dets 存储目录 |
| `cue_tick` | `"1s"`（`TROUPE_CUE_TICK`） | 提醒扫描周期 |
| `shutdown_grace` | `"10s"`（`TROUPE_SHUTDOWN_GRACE`） | 优雅停机宽限 |

方法：`cast/3` · `call/4` / `call!/4` · `off/2` · `drain_all/2` · `shutdown/2` ·
`destroy/1`（kill -9 模拟，测试用）· `publish/3` · `subscribe/3` · `subscribe_mf/3` ·
`unsubscribe/3` · `by_name/1` · `default/0` · `metrics/1` · `traces/3` · `subscribe_trace/1` ·
`director/1`。

## use Troupe.Actor(opts)

| 声明 | 说明 |
|---|---|
| `role_id: "cart"` | 角色名（默认模块末段去 "Actor" 后缀；持久业务必须显式） |
| `intermission: ms \| "5m"` | 空闲下场（默认 "5m"） |
| `schema_version: n` | 结构版本（迁移驱动，默认 1） |
| `read_only: [:m, ...]` | 只读方法（快照并行执行；返回裸值） |
| `improv: [:m, ...]` | 即兴方法（并发执行；完成序整 Props 覆盖） |
| `timeouts: [m: ms, ...]` | 按方法超时覆盖 |
| `cues: [m: every, ...]` | 内存 Cue：每 `every` 调 `m/1`（可 `[every:, name:]`） |
| `rpc: [:m, ...]` | 收窄 RPC 白名单（默认全部公开方法） |

初始 Props：定义公开函数 `def initial_props, do: %{...}`（无结构要求可省略，默认 `%{}`）。

### 方法签名与返回

```elixir
def method(props, arg1, arg2)   # (Props 快照, ...args)
```

| 返回 | 语义 |
|---|---|
| `{:reply, result}` | 回复；Props 不变 |
| `{:reply, result, props}` | 回复 + 内存提交 |
| `{:reply, result, props, [effects: [...]]}` | 同上 + effects |
| `{:save, result, props}` / `{:save, result, props, [effects: [...]]}` | 持久成功（CAS 落盘后回复） |
| read_only 方法 | 裸值 |

### 生命周期回调（可选覆写）

```elixir
def on_stage(props, ctx), do: {:ok, props}                  # 登台后（可带 effects）
def off_stage(props, ctx), do: {:ok, props}                 # 下场前（排空后框架总落盘）
def on_cue(props, name, payload)                            # 持久提醒入口
def on_event(props, event)                                  # Show 订阅默认回调
def migrate_props(stored_props, from_version)               # schemaVersion 逐级迁移（返回 +1 级）
def validate_props(props)                                   # 读入校验：:ok | {:error, reason}
```

`ctx = %{troupe_name:, role_id:, stage_name:}`；方法体内等价助手 `Troupe.Actor.ctx/0`。

### Effects

`{:reminder, name, [at: ts] | [every: ms] | [..., payload: p]}` ·
`{:cancel_reminder, name}` · `{:publish, topic, event}` · `{:subscribe, topic}` ·
`{:unsubscribe, topic}`

### 幂等助手

```elixir
Troupe.Actor.idempotent(props, key, fingerprint, fn props -> {result, props} end)
# → {:executed, result, props} | {:replayed, result, props} | raise IdempotencyConflictError
```

记录存于 Props 保留键 `:__troupe_idempotency__`，随 `{:save, ...}` 原子提交；
保留期限由业务自管（建议加清理 Cue）。

## PropStore 契约

```elixir
@callback read(ctx, key) :: {:ok, stored | nil} | {:error, term}
@callback create(ctx, key, props, schema_version) :: {:ok, stored} | {:error, {:conflict, :exists}} | ...
@callback write(ctx, key, props, opts) :: {:ok, stored} | {:error, {:conflict, :revision}} | ...
  # opts: [expected_revision:, schema_version:, fencing_token:]
@callback delete(ctx, key) :: :ok | {:error, term}
# key = {namespace, role_id, stage_name}; stored = %{props:, revision:, schema_version:}
```

## Troupe.call

```elixir
{:ok, reply} | {:error, exception} = Troupe.call(agent, fun, args, opts \\ [])
result = Troupe.call!(agent, fun, args, opts \\ [])   # raise 版本
# opts: [deadline: ms]；默认：角色 timeouts: 声明 > 全局 default_timeout
```

## Show / Audience

```elixir
Troupe.publish(troupe, topic, event)                    # 尽力扇出，返回投递成员数
Troupe.subscribe(troupe, topic, actor_pid)              # Actor 进程订阅（事件经内部 Call）
un = Troupe.subscribe_mf(troupe, topic, {M, :f, [args]}) # 外部代理订阅（f 收到 args ++ [event]）
un.()                                                    # 退订
```

## Director / CLI

```elixir
Troupe.Director.status(name)        # 集群总览
Troupe.Director.roster(name)        # Stage 列表
Troupe.Director.actors(name, role: "Cart")   # 在场演员
Troupe.Director.inspect(name, "Cart", "user-123")   # Props 快照 + 状态机
Troupe.Director.invoke(name, "Cart", "user-123", :peek, [])
Troupe.Director.off(name, "Cart", "user-123")
Troupe.Director.cues(name)          # 全部持久提醒
Troupe.Director.ring(name)          # 哈希环分布
```

CLI：`mix troupe.status` / `troupe.roster` / `troupe.ps` / `troupe.inspect Cart user-123` /
`troupe.call Cart user-123 peek '[]'` / `troupe.off` / `troupe.cues` / `troupe.ring`
——通用选项 `--node a@host`（共享 cookie）、`--json`、`--name`。

## 可观测

```elixir
Troupe.metrics(troupe)
# %{"Cart" => %{calls:, errors:, rejections:, cue_skipped:, latency: %{p50:, p99:, max:, samples:}}}
Troupe.traces(troupe, 50, role_id: "Cart")     # 最新在前；filter: role_id/method/outcome
un = Troupe.subscribe_trace(role_id: "Cart")   # 消息 {:troupe_trace, event}
un.()
```

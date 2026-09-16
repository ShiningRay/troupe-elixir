# 快速开始

> 要求 Elixir ~> 1.15、OTP 26+。零第三方依赖：`mix deps.get` 都不需要。

## 定义演员

```elixir
defmodule MyApp.CartActor do
  use Troupe.Actor,
    intermission: "10m"          # 空闲 10 分钟自动下场（默认 "5m"）

  def initial_props, do: %{items: []}   # 首次登台的初始状态（无结构要求可省略，默认 %{}）

  # 下场前钩子（可选）；排空后框架总会落盘
  def off_stage(props, _ctx), do: {:ok, props}

  # 公开函数 = RPC 方法；第一个参数是 Props 快照
  def add(props, item) do
    props = Map.update!(props, :items, &[item | &1])
    {:save, :ok, props}          # 持久成功：CAS 落盘成功后才回复
  end

  def peek(props), do: {:reply, length(props.items), props}
end
```

角色名默认 = 模块末段去 `Actor` 后缀（`MyApp.CartActor` → `"Cart"`）；
持久业务建议显式 `use Troupe.Actor, role_id: "cart"`（类改名不破坏寻址）。

## 组团与调用

```elixir
{:ok, troupe} = Troupe.form(actors: [MyApp.CartActor])

cart = Troupe.cast(troupe, MyApp.CartActor, "user-123")   # → %Troupe.Agent{}（选角）

{:ok, :ok} = Troupe.call(cart, :add, ["apple"])           # 发一条 Call
{:ok, 1}   = Troupe.call(cart, :peek, [])
1          = Troupe.call!(cart, :peek, [])                # raise 版本
```

要点：

* **无需显式创建 Actor**：`cast` 即全局寻址；首调自动激活（登台），从 PropStore 恢复 Props
* **严格串行**：同一 Actor 一次一条 Call（Turn）；并发 100 个 `add` 不会交错
* **故障即重激活**：进程崩溃后下一条 Call 在（同机或其它）Stage 重新登台

## 返回形状速查

| 方法返回 | 语义 |
|---|---|
| `{:reply, result}` | 回复；Props 不变 |
| `{:reply, result, props}` | 回复 + 内存提交 |
| `{:save, result, props}` | **持久成功**：落盘成功后才回复 |
| `{:reply, result, props, [effects: [...]]}` | 附带 effects（提醒/发布/订阅） |
| read_only 方法 | 返回裸值（Props 快照只读） |

## 测试

```elixir
defmodule MyApp.CartTest do
  use ExUnit.Case, async: true
  use Troupe.Case, actors: [MyApp.CartActor]   # Rehearsal：全内存、每测试隔离

  test "cart", %{troupe: troupe} do
    cart = Troupe.cast(troupe, MyApp.CartActor, "t1")
    assert {:ok, :ok} = Troupe.call(cart, :add, ["a"])
    assert {:ok, 1} = Troupe.call(cart, :peek, [])
  end
end
```

纯函数方法还可以**不经进程**直接单测：`MyApp.CartActor.add(%{items: []}, "a")`。

## 持久化

```bash
# 默认：File 存储（进程级持久，./.troupe/props/，零配置）
# 多节点生产：共享存储 Provider（Postgres/Redis 等，节点级）——按 PropStore 契约接入
TROUPE_PROPSTORE=file mix ...
```

## 集群（两个节点组团）

```bash
# 节点 A
elixir --sname a@127.0.0.1 -S mix run
# 节点 B
TROUPE_SEEDS=a@127.0.0.1 elixir --sname b@127.0.0.1 -S mix run
```

```elixir
{:ok, troupe} =
  Troupe.form(
    actors: [MyApp.CartActor],
    cluster: [seeds: [:"a@127.0.0.1"]],
    ownership: [lease_ms: 5_000]     # 多节点生产建议启用（租约 + epoch fencing）
  )
```

两节点互相 `cast` 调用透明；Actor 按 HashRing 落在各节点。
详见 [operations.md](operations.md)。

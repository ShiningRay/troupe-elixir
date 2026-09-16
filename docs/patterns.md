# 模式与配方

## 库存/秒杀（单 Actor 串行 + 持久成功）

```elixir
defmodule InventoryActor do
  use Troupe.Actor

  def initial_props, do: %{remaining: 0, reserved: %{}}

  def reserve(props, request_id, qty) do
    {tag, reply, props} =
      Troupe.Actor.idempotent(props, "reserve:#{request_id}", {request_id, qty}, fn props ->
        if props.remaining < qty do
          {{:denied, props.remaining}, props}
        else
          props =
            props
            |> Map.update!(:remaining, &(&1 - qty))
            |> put_in([:reserved, request_id], qty)

          {{:confirmed, props.remaining}, props}
        end
      end)

    {:save, {tag, reply}, props}   # 幂等记录随状态原子落盘
  end
end
```

热点容量事实：单 Actor 一个串行通道；扩容 = 更多 Actor（不同 stageName）/更多 Stage。

## 订单超时取消（延时任务 = 持久提醒）

```elixir
def create(props, order_id) do
  {:reply, :ok, Map.put(props, :status, :pending),
   [effects: [{:reminder, "payment-timeout", [at: Troupe.Time.now_ms() + 30 * 60_000]}]]}
end

def pay(props) do
  {:save, :ok, Map.put(props, :status, :paid),
   [effects: [{:cancel_reminder, "payment-timeout"}]]}   # 注销竞争由 statusVersion CAS 裁决
end

def on_cue(props, "payment-timeout", _payload) do
  if props.status == :pending do
    {:save, :ok, Map.put(props, :status, :cancelled)}
  else
    {:reply, :ok, props}
  end
end
```

## 跨 Actor 编排（无事务，显式补偿）

```elixir
def place(props, order_id, sku, qty) do
  # Actor 互调：用当前执行上下文的 troupe 解析目标 Agent
  troupe = Troupe.by_name(Troupe.Actor.ctx().troupe_name)
  inv = Troupe.cast(troupe, InventoryActor, sku)

  case Troupe.call!(inv, :reserve, [order_id, qty]) do
    {:confirmed, _} -> {:reply, {:ok, :placed}, props}
    {:denied, remaining} -> {:reply, {:rejected, {:sold_out, remaining}}, props}
  end
end
```

失败路径显式业务补偿——单 Actor 串行不能推导跨 Actor 事务。

## 攒批写入（内存 Cue，热路径零 IO）

```elixir
defmodule Feed do
  use Troupe.Actor, cues: [flush: "2s"]

  def initial_props, do: %{items: [], dirty: false}

  def add(props, x) do
    {:reply, :ok, Map.update!(props, :items, &[x | &1]) |> Map.put(:dirty, true)}
  end

  def flush(props) do
    if props.dirty do
      {:save, :ok, Map.put(props, :dirty, false)}   # 周期冲刷
    else
      {:reply, :ok, props}
    end
  end
end
```

攒批必须明示丢失窗口（RPO = flush 间隔，仅崩溃场景；正常下场总会落盘）。
要"响应即已落盘"就在调用里 `{:save, ...}`（持久成功）。
Cue 变慢会体现在 `metrics[role].cue_skipped`——周期任务变慢的首要信号。

## 只读扩容（read_only 真并行）

```elixir
defmodule Session do
  use Troupe.Actor, read_only: [:peek]

  def initial_props, do: %{state: nil, hits: 0}

  def hit(props), do: {:save, :ok, Map.update!(props, :hits, &(&1 + 1))}

  # 快照只读：并发 peek 不占串行通道，与 hit 之外的读天然并行
  def peek(props), do: {:reply, props.state, props}
end
```

## 失败重试（幂等受理 + at 提醒驱动状态机）

```elixir
def submit(props, request_id) do
  {tag, reply, props} =
    Troupe.Actor.idempotent(props, "accept:#{request_id}", request_id, fn props ->
      {:ok, Map.put(props, :request_id, request_id)}
    end)

  props = start_retry(props)
  {:save, {tag, reply}, props, [effects: [{:reminder, "retry", [at: Troupe.Time.now_ms() + 400]}]]}
end

defp start_retry(props), do: props
```

⚠️ 不要把"暂不可用"作为幂等结果记录——重放会短路重试。幂等的是"受理"，重试是内部状态机。

## 聊天室（Show 广播 + Audience 推送）

```elixir
defmodule Room do
  use Troupe.Actor

  def initial_props, do: %{messages: []}

  def on_stage(props, _ctx) do
    {:ok, props, [effects: [{:subscribe, "room:lobby"}]]}   # 登台即订阅
  end

  def on_event(props, event) do
    {:reply, :ok, Map.update!(props, :messages, &[event | &1])}
  end

  def send_msg(props, text) do
    {:reply, :ok, props, [effects: [{:publish, "room:lobby", text}]]}
  end
end
```

**边缘推送**（WS/SSE 转发，非 Actor）用外部 Audience 代理订阅——保持快速，
长活逻辑交给 Actor：

```elixir
un = Troupe.subscribe_mf(troupe, "room:lobby", {MyWeb.Push, :relay, [socket]})
# MyWeb.relay(socket, event) = :gen_tcp.send(socket, Jason.encode!(event))
un.()   # 退订
```

## 状态机 + schemaVersion 迁移

```elixir
defmodule CartV2 do
  use Troupe.Actor, schema_version: 2

  def initial_props, do: %{items: []}

  # 逐级迁移：from 版本的存储形态 → from+1 版本
  def migrate_props(stored, 1), do: %{items: Map.get(stored, :names, [])}
  def migrate_props(stored, _from), do: stored

  def get(props), do: {:reply, props, props}
end
```

规则：只允许逐级 +1；迁移失败 = 激活失败（不得以半迁移状态服务）。
校验钩子 `validate_props/1`（`:ok | {:error, reason}`）可挂运行时断言。

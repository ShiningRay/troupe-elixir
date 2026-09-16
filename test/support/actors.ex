defmodule Troupe.TestSink do
  @moduledoc "测试事件 sink：按 stage_name 注册测试进程，async 安全。"
  @table :troupe_test_sink

  def init do
    if :ets.info(@table) == :undefined do
      :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
    end

    :ok
  end

  def register(stage_name, pid), do: :ets.insert(@table, {stage_name, pid})
  def unregister(stage_name), do: :ets.delete(@table, stage_name)

  def report(stage_name, tag) do
    case :ets.lookup(@table, stage_name) do
      [{^stage_name, pid}] -> send(pid, {:troupe_test_event, stage_name, tag})
      [] -> :ok
    end
  end
end

defmodule Troupe.TestTap do
  @moduledoc "Audience 代理中继：{Troupe.TestTap, :relay, [pid, tag]} —— relay(pid, tag, event)"
  def relay(pid, tag, event), do: send(pid, {tag, event})
end

defmodule Troupe.TestActors do
  @moduledoc "测试用 Actor 定义。方法经 Troupe.TestSink 向测试进程报告执行事件（断言调度顺序）。"

  defmodule Cart do
    use Troupe.Actor

    def initial_props, do: %{items: []}

    def add(props, item) do
      {:save, :ok, Map.update!(props, :items, &[item | &1])}
    end

    def peek(props), do: {:reply, length(props.items), props}
  end

  defmodule Sequencer do
    use Troupe.Actor

    def initial_props, do: %{count: 0}

    # 并发 100 个 push：若发生交错（同快照读-改-写），最后提交覆盖 → count < 100，串行性反证
    def push(props, n) do
      Troupe.TestSink.report(Troupe.Actor.ctx().stage_name, {:push_start, n})
      {:save, :ok, Map.update!(props, :count, &(&1 + 1))}
    end

    def count(props), do: {:reply, props.count, props}

    # 独占慢方法（执行占用测试）
    def slow(props, ms) do
      Troupe.TestSink.report(Troupe.Actor.ctx().stage_name, :slow_start)
      Process.sleep(ms)
      Troupe.TestSink.report(Troupe.Actor.ctx().stage_name, :slow_done)
      {:save, :ok, Map.update!(props, :count, &(&1 + 1))}
    end

    def mark(props, tag) do
      Troupe.TestSink.report(Troupe.Actor.ctx().stage_name, {:mark, tag})
      {:reply, :ok, props}
    end
  end

  defmodule ImprovStage do
    use Troupe.Actor, improv: [:improv_wait], read_only: [:ro_wait]

    def initial_props, do: %{props_state: 0}

    def improv_wait(props, ms) do
      Troupe.TestSink.report(Troupe.Actor.ctx().stage_name, :improv_start)
      Process.sleep(ms)
      Troupe.TestSink.report(Troupe.Actor.ctx().stage_name, :improv_done)
      {:reply, :ok, Map.update!(props, :props_state, &(&1 + 1))}
    end

    def ro_wait(props, ms) do
      Troupe.TestSink.report(Troupe.Actor.ctx().stage_name, :ro_start)
      Process.sleep(ms)
      {:reply, {:ro, props.props_state}, props}
    end

    def strict_mark(props) do
      Troupe.TestSink.report(Troupe.Actor.ctx().stage_name, :strict_start)
      {:reply, :ok, props}
    end

    def state(props), do: {:reply, props.props_state, props}
  end

  defmodule Butterfly do
    use Troupe.Actor, intermission: 120

    def initial_props, do: %{stage_passes: 0, memories: []}

    def on_stage(props, _ctx) do
      Troupe.TestSink.report(Troupe.Actor.ctx().stage_name, :on_stage)
      {:ok, Map.update!(props, :stage_passes, &(&1 + 1))}
    end

    def off_stage(props, _ctx) do
      Troupe.TestSink.report(Troupe.Actor.ctx().stage_name, :off_stage)
      {:ok, props}
    end

    def remember(props, x), do: {:save, :ok, Map.update!(props, :memories, &[x | &1])}
    def recall(props), do: {:reply, Enum.reverse(props.memories), props}
    def passes(props), do: {:reply, props.stage_passes, props}
  end

  defmodule Ticket do
    use Troupe.Actor

    def initial_props, do: %{remaining: 2, reserved: %{}}

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

      {:save, {tag, reply}, props}
    end
  end

  defmodule Migrator do
    use Troupe.Actor, schema_version: 2

    def initial_props, do: %{names: [], items: nil}

    def migrate_props(stored, 1) do
      Troupe.TestSink.report(Troupe.Actor.ctx().stage_name, :migrated)
      %{names: Map.get(stored, :names, []), items: Map.get(stored, :names, [])}
    end

    def migrate_props(stored, _from), do: stored

    def get(props), do: {:reply, props, props}
  end

  defmodule Alarm do
    use Troupe.Actor

    def initial_props, do: %{rings: 0}

    def on_cue(props, _name, _payload) do
      Troupe.TestSink.report(Troupe.Actor.ctx().stage_name, :ring)
      {:save, :ok, Map.update!(props, :rings, &(&1 + 1))}
    end

    def arm(props, in_ms, name) do
      at = Troupe.Time.now_ms() + in_ms
      {:reply, :ok, props, [effects: [{:reminder, name, [at: at]}]]}
    end

    def disarm(props, name) do
      {:reply, :ok, props, [effects: [{:cancel_reminder, name}]]}
    end

    def rings(props), do: {:reply, props.rings, props}
  end

  defmodule Metronome do
    use Troupe.Actor, cues: [tick: 50]

    def initial_props, do: %{ticks: 0}

    def tick(props) do
      Troupe.TestSink.report(Troupe.Actor.ctx().stage_name, :tick)
      {:save, :ok, Map.update!(props, :ticks, &(&1 + 1))}
    end

    def ticks(props), do: {:reply, props.ticks, props}
  end

  defmodule LazyMetronome do
    use Troupe.Actor, cues: [slow: [every: 40, name: "slow-cue"]]

    def initial_props, do: %{n: 0}

    def slow(props) do
      Process.sleep(120)
      {:save, :ok, Map.update!(props, :n, &(&1 + 1))}
    end
  end

  defmodule Room do
    use Troupe.Actor

    def initial_props, do: %{received: []}

    def on_stage(props, _ctx) do
      {:ok, props, [effects: [{:subscribe, "room:1"}]]}
    end

    def on_event(props, event) do
      Troupe.TestSink.report(Troupe.Actor.ctx().stage_name, {:show, event})
      {:reply, :ok, Map.update!(props, :received, &[event | &1])}
    end

    def say(props, text) do
      {:reply, :ok, props, [effects: [{:publish, "room:1", text}]]}
    end

    def received(props), do: {:reply, Enum.reverse(props.received), props}
  end

  defmodule Fragile do
    use Troupe.Actor

    def initial_props, do: %{n: 0}

    def boom(_props, msg), do: raise(RuntimeError, message: msg)
    def safe(props), do: {:reply, :ok, props}
    def bump(props), do: {:save, :ok, Map.update!(props, :n, &(&1 + 1))}
  end

  defmodule Narrow do
    use Troupe.Actor, rpc: [:visible]

    def visible(props), do: {:reply, :yes, props}
    def hidden(props), do: {:reply, :no, props}
  end
end

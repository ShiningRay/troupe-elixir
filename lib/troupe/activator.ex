defmodule Troupe.Activator do
  @moduledoc "本节点在场 Actor 的动态监督者（每 Troupe 一个）。ActorServer restart: :temporary——崩溃不自动重启，下一条 Call 触发重新登台（故障即重激活）。"

  def child_spec(opts) do
    troupe_name = Keyword.fetch!(opts, :troupe_name)

    %{
      id: {__MODULE__, troupe_name},
      start: {DynamicSupervisor, :start_link, [[name: Troupe.Names.via(troupe_name, :activator)]]}
    }
  end

  @doc "本节点全部在场 ActorServer（troupe 停机/destroy 用）。"
  def which_children(troupe_name) do
    case Troupe.Names.lookup(troupe_name, :activator) do
      nil -> []
      sup -> DynamicSupervisor.which_children(sup) |> Enum.map(&elem(&1, 1))
    end
  end
end

defmodule Troupe.Show do
  @moduledoc """
  Show（演出）：话题化 pub/sub，底层 = OTP `:pg`（进程组，天然分布式）。

  * Actor 订阅：actor 进程加入组，事件作为内部 Call 进入其 CallBoard（严格串行）
  * 外部回调订阅（Audience，`{module, function}`）：框架派生的代理进程收事件——
    用于边缘推送（WS/SSE 转发），不进 Actor 调度
  * 发布为尽力扇出；投递语义、持久订阅随共享 Show 存储后续接入
  """

  use GenServer

  def child_spec(opts) do
    troupe_name = Keyword.fetch!(opts, :troupe_name)
    %{id: {__MODULE__, troupe_name}, start: {__MODULE__, :start_link, [[troupe_name: troupe_name]]}}
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :troupe_name)
    GenServer.start_link(__MODULE__, name, name: Troupe.Names.via(name, :show))
  end

  @impl true
  def init(troupe_name) when is_atom(troupe_name) do
    # :pg scope 全局共享（scope 必须为 atom）；话题按 {troupe_name, topic} 分组
    {:ok, %{scope: :troupe_shows, troupe_name: troupe_name}}
  end

  # -- API ------------------------------------------------------------------

  @doc "Actor 订阅话题：事件经内部 Call 投递（默认 on_event/2）。"
  @spec subscribe(map(), term(), pid()) :: :ok
  def subscribe(%{show: troupe_name}, topic, pid) when is_atom(troupe_name) and not is_nil(troupe_name),
    do: :pg.join(:troupe_shows, {troupe_name, topic}, pid)

  @doc "外部 {module, function}（或 {m, f, args}）订阅（Audience 代理）。返回退订函数。"
  @spec subscribe_mf(map(), term(), {module(), atom()} | {module(), atom(), [term()]}) :: (-> :ok)
  def subscribe_mf(show, topic, {m, f}) when is_atom(m) and is_atom(f),
    do: subscribe_mf(show, topic, {m, f, []})

  def subscribe_mf(show, topic, {m, f, args}) when is_atom(m) and is_atom(f) and is_list(args) do
    %{show: troupe_name} = show

    if is_nil(troupe_name) do
      raise "show is not enabled on troupe #{inspect(show.name)}"
    end

    group = {troupe_name, topic}

    {:ok, proxy} = GenServer.start(Troupe.Show.Proxy, {m, f, args})
    :ok = :pg.join(:troupe_shows, group, proxy)

    fn ->
      :pg.leave(:troupe_shows, group, proxy)
      GenServer.stop(proxy, :normal)
      :ok
    end
  end

  def unsubscribe(%{show: troupe_name}, topic, pid) when is_atom(troupe_name) and not is_nil(troupe_name),
    do: :pg.leave(:troupe_shows, {troupe_name, topic}, pid)

  @doc "发布事件到话题（尽力扇出，本地 + 远程成员）。返回投递成员数。"
  @spec publish(map(), term(), term()) :: non_neg_integer()
  def publish(troupe, topic, event) do
    %Troupe{show: troupe_name} = troupe

    if is_nil(troupe_name) do
      raise "show is not enabled on troupe #{inspect(troupe.name)}"
    end

    members = :pg.get_members(:troupe_shows, {troupe_name, topic})

    for pid <- members do
      send(pid, {:troupe_show, topic, event})
    end

    length(members)
  end

  @doc "话题成员数（跨节点）。"
  def members(troupe, topic) do
    %Troupe{show: troupe_name} = troupe

    if is_nil(troupe_name) do
      raise "show is not enabled on troupe #{inspect(troupe.name)}"
    end

    length(:pg.get_members(:troupe_shows, {troupe_name, topic}))
  end
end

defmodule Troupe.Show.Proxy do
  @moduledoc false
  use GenServer
  require Logger

  def init(mf), do: {:ok, mf}

  def handle_info({:troupe_show, _topic, event}, {m, f, args} = st) do
    try do
      apply(m, f, args ++ [event])
    rescue
      e -> Logger.error("Troupe.Show audience #{inspect(m)}.#{f} failed: #{Exception.message(e)}")
    end

    {:noreply, st}
  end

  def handle_info(_, st), do: {:noreply, st}
end

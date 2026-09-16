defmodule Troupe.Config do
  @moduledoc """
  零配置的默认值表。优先级链：**显式传参 > 环境变量 > 约定默认值**。

  用户只为"偏离默认"付费；每个旋钮都有默认值（Troupe.js DESIGN §8）。
  """

  alias Troupe.Time

  @enforce_keys []
  defstruct [
    :namespace,
    :intermission_ms,
    :default_timeout_ms,
    :queue_depth,
    :props_dir,
    :store,
    :cue_tick_ms,
    :reminder_lease_ms,
    :shutdown_grace_ms,
    :drain_policy,
    :cluster,
    :ownership
  ]

  @type t :: %__MODULE__{}

  @doc "合并显式 opts 与环境变量，产出完整配置。"
  @spec build(keyword()) :: t()
  def build(opts) do
    %__MODULE__{
      namespace: opt(opts, :namespace, "TROUPE_NAMESPACE", "default"),
      intermission_ms: opt_ms(opts, :intermission, nil, 5 * 60_000),
      default_timeout_ms: opt_ms(opts, :default_timeout, "TROUPE_TIMEOUT", 30_000),
      queue_depth: opt_int(opts, :queue_depth, "TROUPE_QUEUE_DEPTH", 1024),
      props_dir: opt(opts, :props_dir, "TROUPE_PROPS_DIR", ".troupe"),
      store: Keyword.get(opts, :store) || env_store() || :file,
      cue_tick_ms: opt_ms(opts, :cue_tick, "TROUPE_CUE_TICK", 1_000),
      reminder_lease_ms: opt_ms(opts, :reminder_lease, nil, 15_000),
      shutdown_grace_ms: opt_ms(opts, :shutdown_grace, "TROUPE_SHUTDOWN_GRACE", 10_000),
      drain_policy: Keyword.get(opts, :drain_policy) || :wait,
      cluster: cluster(opts),
      ownership: ownership(opts)
    }
  end

  defp opt(opts, key, env, default) do
    Keyword.get(opts, key) || System.get_env(env) || default
  end

  defp opt_int(opts, key, env, default) do
    Keyword.get(opts, key) || (System.get_env(env) && String.to_integer(System.get_env(env))) ||
      default
  end

  defp opt_ms(opts, key, env, default) do
    v = Keyword.get(opts, key) || (env && System.get_env(env)) || default
    Time.parse(v)
  end

  # store: :memory | :file | :dets | {module, opts}
  defp env_store do
    case System.get_env("TROUPE_PROPSTORE") do
      "memory" -> :memory
      "file" -> :file
      "dets" -> :dets
      _ -> nil
    end
  end

  defp cluster(opts) do
    case Keyword.fetch(opts, :cluster) do
      {:ok, nil} -> nil
      {:ok, kw} when is_list(kw) -> kw
      :error -> nil
    end
  end

  defp ownership(opts) do
    case Keyword.fetch(opts, :ownership) do
      {:ok, true} -> [lease_ms: 5_000]
      {:ok, kw} when is_list(kw) -> kw
      {:ok, nil} -> nil
      :error -> nil
    end
  end
end

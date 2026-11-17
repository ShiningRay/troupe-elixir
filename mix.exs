defmodule Troupe.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/troupe-elixir/troupe"

  def project do
    [
      app: :troupe,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      # Postgres Provider 的可选依赖：运行时由用户应用提供（Code.ensure_loaded? 门禁），
      # dev/prod 编译时不因此产生 xref 警告
      elixirc_options: [no_warn_undefined: [Postgrex, Jason]],
      deps: deps(),
      description: description(),
      package: package(),
      name: "Troupe",
      docs: docs(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Troupe.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support", "bench"]
  defp elixirc_paths(_), do: ["lib", "bench"]

  # 运行时零依赖（hex 包不含任何 deps）；Postgres Provider（lib/troupe/postgres.ex）
  # 在用户应用侧提供 postgrex。test env 固定携带（真实 PG 验证 fencing/接管闭环，
  # Issue #3），避免 TROUPE_TEST_PG 条件编译导致的 _build/deps 不一致。
  defp deps do
    [{:postgrex, "~> 0.17", only: :test}, {:jason, "~> 1.4", only: :test}]
  end

  defp description do
    "Virtual Actor framework for Elixir, following the Troupe design (Orleans/Orbit-style)." <>
      " Actors are always-on, addressable by (role, stage_name), activated on demand," <>
      " passivated when idle. 剧团隐喻：Troupe/Stage/Actor/Role/Agent/Props。"
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE DESIGN.md docs)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      extras: ["README.md", "DESIGN.md" | Path.wildcard("docs/*.md")]
    ]
  end

  defp aliases do
    [
      bench: ["run bench/benchmark.exs"]
    ]
  end
end

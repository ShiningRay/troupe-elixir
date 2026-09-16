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

  defp deps, do: []

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

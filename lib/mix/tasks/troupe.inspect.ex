defmodule Mix.Tasks.Troupe.Inspect do
  @shortdoc "Troupe CLI: inspect"
  @moduledoc "See `Troupe.CLI`. Usage: mix troupe.inspect [--node a@host] [--json]"

  use Mix.Task

  @impl Mix.Task
  def run(args), do: Troupe.CLI.run("inspect", args)
end

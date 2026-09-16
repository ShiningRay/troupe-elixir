defmodule Mix.Tasks.Troupe.Ps do
  @shortdoc "Troupe CLI: ps"
  @moduledoc "See `Troupe.CLI`. Usage: mix troupe.ps [--node a@host] [--json]"

  use Mix.Task

  @impl Mix.Task
  def run(args), do: Troupe.CLI.run("ps", args)
end

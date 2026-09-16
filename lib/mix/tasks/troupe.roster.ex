defmodule Mix.Tasks.Troupe.Roster do
  @shortdoc "Troupe CLI: roster"
  @moduledoc "See `Troupe.CLI`. Usage: mix troupe.roster [--node a@host] [--json]"

  use Mix.Task

  @impl Mix.Task
  def run(args), do: Troupe.CLI.run("roster", args)
end

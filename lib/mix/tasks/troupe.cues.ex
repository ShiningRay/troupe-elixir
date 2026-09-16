defmodule Mix.Tasks.Troupe.Cues do
  @shortdoc "Troupe CLI: cues"
  @moduledoc "See `Troupe.CLI`. Usage: mix troupe.cues [--node a@host] [--json]"

  use Mix.Task

  @impl Mix.Task
  def run(args), do: Troupe.CLI.run("cues", args)
end

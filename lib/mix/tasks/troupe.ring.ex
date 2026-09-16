defmodule Mix.Tasks.Troupe.Ring do
  @shortdoc "Troupe CLI: ring"
  @moduledoc "See `Troupe.CLI`. Usage: mix troupe.ring [--node a@host] [--json]"

  use Mix.Task

  @impl Mix.Task
  def run(args), do: Troupe.CLI.run("ring", args)
end

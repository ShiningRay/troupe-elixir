defmodule Mix.Tasks.Troupe.Status do
  @shortdoc "Troupe CLI: status"
  @moduledoc "See `Troupe.CLI`. Usage: mix troupe.status [--node a@host] [--json]"

  use Mix.Task

  @impl Mix.Task
  def run(args), do: Troupe.CLI.run("status", args)
end

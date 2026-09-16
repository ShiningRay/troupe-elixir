defmodule Mix.Tasks.Troupe.Off do
  @shortdoc "Troupe CLI: off"
  @moduledoc "See `Troupe.CLI`. Usage: mix troupe.off [--node a@host] [--json]"

  use Mix.Task

  @impl Mix.Task
  def run(args), do: Troupe.CLI.run("off", args)
end

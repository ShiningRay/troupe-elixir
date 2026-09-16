defmodule Mix.Tasks.Troupe.Call do
  @shortdoc "Troupe CLI: call"
  @moduledoc "See `Troupe.CLI`. Usage: mix troupe.call [--node a@host] [--json]"

  use Mix.Task

  @impl Mix.Task
  def run(args), do: Troupe.CLI.run("call", args)
end

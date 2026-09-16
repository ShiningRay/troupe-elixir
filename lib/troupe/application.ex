defmodule Troupe.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    Troupe.Metrics.init_tables()
    Troupe.Trace.init_tables()

    if :ets.info(:troupe_globals) == :undefined do
      :ets.new(:troupe_globals, [:set, :public, :named_table])
    end

    children = [
      {Registry, keys: :unique, name: Troupe.Names},
      {Registry, keys: :unique, name: Troupe.Actors},
      {Task.Supervisor, name: Troupe.GlobalTaskSup},
      %{id: Troupe.ShowPG, start: {:pg, :start_link, [:troupe_shows]}}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Troupe.AppSup)
  end
end

defmodule Horde.Application do
  use Application

  import Supervisor.Spec

  def start(_, _) do
    children = [
      worker(Horde.Storage.Ets, []),
      supervisor(Horde.Supervisor, [])
    ]
    Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__)
  end
end

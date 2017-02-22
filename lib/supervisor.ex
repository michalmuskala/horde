defmodule Horde.Supervisor do
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    children = [worker(Horde.Server, [], restart: :temporary)]

    supervise(children, strategy: :simple_one_for_one)
  end

  def register(name) do
    Supervisor.start_child(__MODULE__, [name])
  end
end

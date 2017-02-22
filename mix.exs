defmodule Horde.Mixfile do
  use Mix.Project

  def project do
    [app: :horde,
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  def application do
    [mod: {Horde.Application, []},
     extra_applications: [:logger]]
  end

  defp deps do
    [{:swarm, "~> 3.0"}]
  end
end
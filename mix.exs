defmodule Horde.Mixfile do
  use Mix.Project

  def project do
    [app: :horde,
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     elixirc_paths: elixirc_paths(Mix.env),
     deps: deps()]
  end

  def application do
    [mod: {Horde.Application, []},
     extra_applications: [:logger]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [{:swarm, "~> 3.0"}]
  end
end

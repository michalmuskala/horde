defmodule Horde.Storage.EtsTest do
  use ExUnit.Case

  alias Horde.Storage

  setup do
    Storage.Ets.clean
  end

  test "write & load back" do
    assert :ok == Storage.Ets.write(__MODULE__, 1, 0, :foo)
    assert {:ok, 0, :foo} == Storage.Ets.load(__MODULE__, 1)
  end

  test "subsequent writes" do
    assert :ok == Storage.Ets.write(__MODULE__, 1, 0, :foo)
    assert :error == Storage.Ets.write(__MODULE__, 1, 0, :bar)
    assert :ok == Storage.Ets.write(__MODULE__, 1, 1, :bar)
    assert {:ok, 1, :bar} == Storage.Ets.load(__MODULE__, 1)
  end

  test "concurrent write & load" do
    # we simulate this condition by manually placing data in the table
    assert :ok == Storage.Ets.write(__MODULE__, 1, 0, :foo)
    :ets.insert(Storage.Ets, {{__MODULE__, 1, 1}, :bar})
    assert {:ok, 1, :bar} == Storage.Ets.load(__MODULE__, 1)
  end
end

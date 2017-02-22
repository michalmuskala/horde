defmodule Horde.Storage.EtsTest do
  use ExUnit.Case

  alias Horde.Storage

  setup do
    Storage.Ets.clean
  end

  test "write & load back" do
    assert :ok == Storage.Ets.write(__MODULE__, 1, 1, :foo)
    assert {:ok, 1, :foo} == Storage.Ets.load(__MODULE__, 1)
  end

  test "subsequent writes" do
    assert :ok == Storage.Ets.write(__MODULE__, 1, 1, :foo)
    assert :error == Storage.Ets.write(__MODULE__, 1,	1, :bar)
    assert :ok == Storage.Ets.write(__MODULE__, 1, 2, :bar)
    assert {:ok, 2, :bar} == Storage.Ets.load(__MODULE__, 1)
  end

  test "concurrent write & load" do
    # we simulate this condition by manually placing data in the table
    assert :ok == Storage.Ets.write(__MODULE__, 1, 1, :foo)
    :ets.insert(Storage.Ets, {{__MODULE__, 1, 2}, :bar})
    assert {:ok, 2, :bar} == Storage.Ets.load(__MODULE__, 1)
  end
end

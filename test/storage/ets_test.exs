defmodule Horde.Storage.EtsTest do
  use ExUnit.Case

  alias Horde.Storage

  setup do
    Storage.Ets.clean
  end

  test "default load" do
    assert {:ok, 0, :foo} == Storage.Ets.load(__MODULE__, 0, :foo)
  end

  test "write & load back" do
    assert {:ok, 1, :foo} == Storage.Ets.write(__MODULE__, 1, 0, :foo)
    assert {:ok, 1, :foo} == Storage.Ets.load(__MODULE__, 1, :bar)
  end

  test "subsequent writes" do
    assert {:ok, 2, :foo} == Storage.Ets.write(__MODULE__, 1, 1, :foo)
    assert :error == Storage.Ets.write(__MODULE__, 1,	1, :bar)
    assert {:ok, 3, :bar} == Storage.Ets.write(__MODULE__, 1, 2, :bar)
    assert {:ok, 3, :bar} == Storage.Ets.load(__MODULE__, 1, :baz)
  end

  test "concurrent write & load" do
    # we simulate this condition by manually placing data in the table
    assert {:ok, 1, :foo} == Storage.Ets.write(__MODULE__, 1, 0, :foo)
    :ets.insert(Storage.Ets, {{__MODULE__, 1, 2}, :bar})
    assert {:ok, 2, :bar} == Storage.Ets.load(__MODULE__, 1, :baz)
  end
end

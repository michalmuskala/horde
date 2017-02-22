defmodule Storage do
  @type t :: module

  @type data :: any
  @type version :: pos_integer
  @type id :: binary

  @callback load(module, id) :: {:ok, version, data} | :error
  @callback write(module, id, version, data) :: :ok | :error
end

defmodule Storage do
  @type t :: module

  @type id :: term

  @callback load(module, id, data :: term) ::
    {:ok, next_version :: pos_integer, new_data :: term} | :error
  @callback write(module, id, version :: integer, data :: term) ::
    {:ok, next_version :: pos_integer, new_data :: term} | :error
end

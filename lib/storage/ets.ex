defmodule Horde.Storage.Ets do
  use GenServer

  def start_link do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def load(module, id, data) do
    case :ets.match(__MODULE__, {{module, id, :"$1"}, :"$2"}) do
      [] ->
        {:ok, 0, data}
      results ->
        [version, data] = Enum.max_by(results, &hd/1)
        {:ok, version, data}
    end
  end

  def clean do
    GenServer.call(__MODULE__, :clean)
  end

  def write(module, id, version, data) do
    GenServer.call(__MODULE__, {:write, module, id, version, data})
  end

  def init(:ok) do
    extra_opts = Application.get_env(:horde, :ets_storage_opts, [])
    :ets.new(__MODULE__, [:named_table, read_concurrency: true] ++ extra_opts)
    {:ok, %{}}
  end

  def handle_call(:clean, _from, state) do
    :ets.delete_all_objects(__MODULE__)
    {:reply, :ok, state}
  end

  def handle_call({:write, module, id, version, data}, _from, state) do
    case load(module, id, data) do
      {:ok, loaded_version, _data} when loaded_version < version + 1 ->
        insert_new(module, id, version + 1, data, state)
      _other ->
        {:reply, :error, state}
    end
  end

  defp insert_new(module, id, version, data, state) do
    case :ets.insert_new(__MODULE__, {{module, id, version}, data}) do
      true ->
        ms = [{{{:"$1", :"$2", :"$3"}, :_},
               [{:"=:=", {:const, module}, :"$2"}, {:"=:=", {:const, id}, :"$1"}],
               [{:<, :"$3", version}]}]
        :ets.match_delete(__MODULE__, ms)
        {:reply, {:ok, version, data}, state}
      false ->
        {:reply, :error, state}
    end
  end
end

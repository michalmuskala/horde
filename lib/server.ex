defmodule Horde.Server do
  @callback init(id :: binary) ::
    {:load, Storage.t, state} |
    :ignore |
    {:stop, reason :: any} when state: any

  @callback loaded(data :: any, state) ::
    {:ok, state} |
    {:ok, state, timeout | :hibernate} |
    {:stop, reason :: any} when state: any

  @callback terminate(reason, state :: term) :: term when reason: :normal | :shutdown | {:shutdown, term} | term

  @callback handle_cast(request :: term, state :: term) ::
    {:noreply | :persist, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term

  @callback handle_info(msg :: :timeout | term, state :: term) ::
    {:noreply | :persist, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term

  # @callback handle_call(request :: term, GenServer.from, state :: term) ::
  #   {:reply | :persist, reply, new_state} |
  #   {:reply, reply, new_state, timeout | :hibernate}
  #   # {:noreply | :persist, new_state} |
  #   {:noreply, new_state, timeout | :hibernate}
  #   {:stop, reason, reply, new_state} |
  #   {:stop, reason, new_state} when reply: term, new_state: term, reason: term

  @callback format_status(reason, pdict_and_state :: list) :: term when reason: :normal | :terminate
  @optional_callbacks [format_status: 2]

  @callback code_change(old_vsn, state :: term, extra :: term) ::
    {:ok, new_state :: term} |
    {:reload, Storage.t, new_state :: term} |
    {:error, reason :: term} when old_vsn: term | {:down, term}

  @behaviour :gen_statem

  def start_link({module, id}, opts \\ []) do
    :gen_statem.start_link(__MODULE__, {module, id}, opts)
  end

  def ping({module, id}) do
    with {:ok, pid} <- lookup(module, id) do
      :gen_statem.call(pid, :ping, {:dirty_timeout, 5_000})
    end
  end

  defp lookup(module, id) do
    name = {module, id}
    case Swarm.register_name(name, Horde.Supervisor, :register, [name]) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_registered, pid}} -> {:ok, pid}
      error -> error
    end
  end

  # Callbacks

  def callback_mode, do: :state_functions

  def init({id, module}) do
    case module.init(id) do
      {:load, storage, inner} ->
        data = %{mod: module, id: id, storage: storage, inner: inner}
        {:ok, :loading, data, {:next_event, :internal, :load}}
      :ignore ->
        :ignore
      {:stop, reason} ->
        {:stop, reason}
      other ->
        {:stop, {:bad_return, other}}
    end
  end

  def loading(:internal, :load, %{storage: storage, id: id, mod: mod, inner: inner} = data) do
    case storage.load(mod, id) do
      {:ok, version, loaded} ->
        case mod.loaded(loaded, inner) do
          {:ok, inner} ->
            data = Map.merge(data, %{inner: inner, version: version})
            {:next_state, :loaded, data}
          {:ok, inner, timeout} ->
            data = Map.merge(data, %{inner: inner, version: version})
            {:next_state, :loaded, data, map_timeout(timeout)}
          {:stop, reason} ->
            {:stop, reason}
        end
      :error ->
        {:stop, :not_loaded}
    end
  end

  def loaded(:cast, event, %{mod: mod, inner: inner} = data) do
    handle_resp(mod.handle_cast(event, inner), data)
  end

  def loaded(:info, event, %{mod: mod, inner: inner} = data) do
    handle_resp(mod.handle_info(event, inner), data)
  end

  def loaded({:call, from}, event, %{mod: mod, inner: inner} = data) do
    case mod.handle_call(event, from, inner) do
      {:reply, reply, inner} ->
        {:keep_state, %{data | inner: inner}, {:reply, from, reply}}
      {:reply, reply, inner, timeout} ->
        {:keep_state, %{data | inner: inner}, [{:reply, from, reply}, map_timeout(timeout)]}
      {:persist, reply, inner} ->
        {:keep_state, %{data | inner: inner},
         [{:reply, from, reply}, persist_event(inner)]}
      {:persist, inner} ->
        {:keep_state, %{data | inner: inner},
         {:next_event, :internal, {:persist, inner}}}
      {:stop, reason, reply, inner} ->
        {:stop_and_reply, reason, {:reply, from, reply}, %{data | inner: inner}}
      other ->
        handle_resp(other, data)
    end
  end

  def loaded(:timeout, :inner, %{mod: mod, inner: inner} = data) do
    handle_resp(mod.handle_info(:timeout, inner), data)
  end

  def loaded(:internal, {:persist, inner}, %{storage: storage, mod: mod, id: id, version: version} = data) do
    case storage.write(mod, id, version + 1, inner) do
      :ok ->
        {:keep_state, %{data | version: version + 1}}
      :error ->
        {:stop, :cant_save, data}
    end
  end

  defp handle_resp({:noreply, inner}, data),
    do: {:keep_state, %{data | inner: inner}}
  defp handle_resp({:persist, inner}, data),
    do: {:keep_state, %{data | inner: inner}, persist_event(inner)}
  defp handle_resp({:noreply, inner, timeout}, data),
    do: {:keep_state, %{data | inner: inner}, map_timeout(timeout)}
  defp handle_resp({:stop, reason, inner}, data),
    do: {:stop, reason, %{data | inner: inner}}
  defp handle_resp(other, data),
    do: {:stop, {:bad_return, other}, data}

  defp persist_event(inner), do: {:next_event, :internal, {:persists, inner}}

  def code_change(old_vsn, state, %{mod: mod, id: id, inner: inner} = data, extra) do
    case mod.code_change(old_vsn, inner, extra) do
      {:ok, inner} ->
        {:ok, state, %{data | inner: inner}}
      {:reload, storage, inner} ->
        case storage.load(mod, id) do
          {:ok, version, loaded} ->
            case mod.loaded(loaded, inner) do
              {:ok, inner} ->
                data = Map.merge(data, %{inner: inner, version: version})
                {:ok, state, data}
              {:ok, state, _timeout} ->
                data = Map.merge(data, %{inner: inner, version: version})
                {:ok, state, data}
              {:stop, reason} ->
                {:error, reason}
            end
          :error ->
            {:error, :not_loaded}
        end
      other ->
        other
    end
  end

  def format_status(:normal, [pdict, _state, %{mod: mod, inner: inner}]) do
    try do
      apply(mod, :format_status, [:normal, [pdict, inner]])
    catch
      _, _ ->
        [{:data, [{'State', inner}]}]
    else
      mod_status ->
        mod_status
    end
  end
  def format_status(:terminate, [pdict, _state, %{mod: mod, inner: inner}]) do
    try do
      apply(mod, :format_status, [:terminate, [pdict, inner]])
    catch
      _, _ ->
        inner
    else
      mod_state ->
        mod_state
    end
  end

  def terminate(reason, _state, %{mod: mod, inner: inner}) do
    mod.terminate(reason, inner)
  end

  defp map_timeout(:hibernate), do: :hibernate
  defp map_timeout(other), do: {:timeout, other, :inner}
end

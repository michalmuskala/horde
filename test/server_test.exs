defmodule Horde.ServerTest do
  use ExUnit.Case, async: true

  alias Horde.Server
  alias Horde.Storage

  defmodule EvalServer do
    @behaviour Server

    def init(fun) when is_function(fun, 0), do: fun.()
    def init(state), do: {:ok, state}

    def loaded(data, fun), do: fun.(data)

    def handle_call(:state, _, state), do: {:reply, state, state}
    def handle_call(fun, from, state), do: fun.(from, state)

    def handle_cast(fun, state), do: fun.(state)

    def handle_info(:timeout, fun), do: fun.()
    def handle_info(fun, state), do: fun.(state)

    def format_status(_, [pdict, state]) do
      case Keyword.get(pdict, :format_status) do
        nil -> state
        fun -> fun.(state)
      end
    end

    def terminate({:shutdown, fun}, state), do: fun.(state)
    def terminate({:abnormal, fun}, state), do: fun.(state)
  end

  describe "init/1" do
    test "{:ok, storage, state}" do
      fun = fn -> {:ok, Storage.Ets, 1} end
      assert {:ok, pid} = Server.start_link({EvalServer, fun})
      assert Server.call(pid, :state) == 1
    end

    test "{:ok, storage, state, timeout}" do
      timeout = fn -> {:noreply, :timedout} end
      fun = fn -> {:ok, Storage.Ets, timeout, 0} end
      assert {:ok, pid} = Server.start_link({EvalServer, fun})
      assert Server.call(pid, :state) == :timedout
    end

    test "{:ok, storage, state, :hibernate}" do
      fun = fn -> {:ok, Storage.Ets, 1, :hibernate} end
      assert {:ok, pid} = Server.start_link({EvalServer, fun})
      assert_hibernated pid
    end

    test ":ignore" do
      Process.flag(:trap_exit, true)
      fun = fn -> :ignore end
      assert Server.start_link({EvalServer, fun}) == :ignore
      assert_receive {:EXIT, _, :normal}
    end

    test "{:stop, reason}" do
      Process.flag(:trap_exit, true)
      fun = fn -> {:stop, :normal} end
      assert Server.start_link({EvalServer, fun}) == {:error, :normal}
      assert_receive {:EXIT, _, :normal}
    end

    test "invalid" do
      Process.flag(:trap_exit, true)
      fun = fn -> :oops end
      assert Server.start_link({EvalServer, fun}) == {:error, {:bad_return_value, :oops}}
      assert_receive {:EXIT, _, {:bad_return_value, :oops}}
    end
  end

  describe "loaded/2" do
    test "{:ok, state}" do
      fun = fn -> {:load, Storage.Ets, fn nil -> {:ok, 1} end} end
      assert {:ok, pid} = Server.start_link({EvalServer, fun})
      assert Server.call(pid, :state) == 1
    end

    test "{:ok, state} with data in storage" do
      fun = fn -> {:load, Storage.Ets, fn data -> {:ok, data} end} end
      Storage.Ets.write(EvalServer, fun, 1, :hello)
      assert {:ok, pid} = Server.start_link({EvalServer, fun})
      assert Server.call(pid, :state) == :hello
    end

    test "{:ok, state, timeout}" do
      timeout = fn -> {:noreply, :timedout} end
      fun = fn -> {:load, Storage.Ets, fn _ -> {:ok, timeout, 0} end} end
      assert {:ok, pid} = Server.start_link({EvalServer, fun})
      assert Server.call(pid, :state) == :timedout
    end

    test "{:ok, state, :hibernate}" do
      fun = fn -> {:load, Storage.Ets, fn _ -> {:ok, 1, :hibernate} end} end
      assert {:ok, pid} = Server.start_link({EvalServer, fun})
      assert_hibernated pid
    end

    test "{:stop, reason, state}" do
      Process.flag(:trap_exit, true)
      parent = self()
      shutdown = fn state -> send(parent, {:terminate, state}) end
      fun = fn -> {:load, Storage.Ets, fn _ -> {:stop, {:shutdown, shutdown}, 1} end} end
      assert {:ok, pid} = Server.start_link({EvalServer, fun})
      assert_receive {:terminate, 1}
      assert_receive {:EXIT, _, {:shutdown, _}}
    end
  end

  defp assert_hibernated(pid) do
    :timer.sleep(10)
    assert Process.info(pid, :current_function) ==
      {:current_function, {:erlang, :hibernate, 3}}
  end
end

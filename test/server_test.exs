defmodule Horde.ServerTest do
  use ExUnit.Case, async: true

  alias Horde.Server
  alias Horde.Storage

  defmodule EvalServer do
    @behaviour Server

    def init(fun) when is_function(fun, 0), do: fun.()
    def init(state), do: {:ok, Storage.Ets, state}

    def loaded(data, fun), do: fun.(data)

    def handle_call(:state, _, state), do: {:reply, state, state}
    def handle_call(fun, from, state), do: fun.(from, state)

    def handle_cast(fun, state), do: fun.(state)

    def handle_info(:timeout, fun), do: fun.()
    def handle_info(fun, state), do: fun.(state)

    def reconcile(remote, fun), do: fun.(remote)

    def format_status(_, [pdict, state]) do
      case Keyword.get(pdict, :format_status) do
        nil -> state
        fun -> fun.(state)
      end
    end

    def terminate(:handoff, state), do: state.()
    def terminate({:shutdown, fun}, state), do: fun.(state)
    def terminate({:abnormal, fun}, state), do: fun.(state)
  end

  setup do
    Storage.Ets.clean
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
      fun = fn -> {:load, Storage.Ets, fn _ -> {:ok, 1} end} end
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
      assert_receive {:EXIT, ^pid, {:shutdown, _}}
    end
  end

  describe "handle_cast/2" do
    test "{:noreply, state}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn n ->
        send(parent, n)
        {:noreply, n+1}
      end
      assert Server.cast(pid, fun) == :ok
      assert_receive 1
      assert Server.call(pid, :state) == 2
    end

    test "{:noreply, state, timeout}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn(n) ->
        timeout = fn() ->
          send(parent, {:timeout, n+1})
          {:noreply, n+1}
        end
        send(parent, n)
        {:noreply, timeout, 0}
      end
      Server.cast(pid, fun)
      assert_receive 1
      assert_receive {:timeout, 2}
    end

    test "{:noreply, state, :hibernate}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn(n) ->
        send(parent, n)
        {:noreply, n+1, :hibernate}
      end
      Server.cast(pid, fun)
      assert_receive 1
      assert_hibernated pid
      assert Server.call(pid, :state) === 2
    end

    test "{:persist, state}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn(n) ->
        send(parent, n)
        {:persist, n+1}
      end
      Server.cast(pid, fun)
      assert_receive 1
      assert Server.call(pid, :state) === 2
      assert {:ok, 1, 2} = Storage.Ets.load(EvalServer, 1, :foo)
    end

    test "{:stop, reason, state}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      _ = Process.flag(:trap_exit, true)
      parent = self()
      fun = fn(n) ->
        terminate = fn(m) ->
          send(parent, {:terminate, m})
        end
        send(parent, n)
        {:stop, {:shutdown, terminate}, n+1}
      end
      Server.cast(pid, fun)
      assert_receive 1
      assert_receive {:terminate, 2}
      assert_receive {:EXIT, ^pid, {:shutdown, _}}
    end
  end

  describe "handle_info/2" do
    test "{:noreply, state}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn n ->
        send(parent, n)
        {:noreply, n+1}
      end
      send(pid, fun)
      assert_receive 1
      assert Server.call(pid, :state) == 2
    end

    test "{:noreply, state, timeout}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn(n) ->
        timeout = fn() ->
          send(parent, {:timeout, n+1})
          {:noreply, n+1}
        end
        send(parent, n)
        {:noreply, timeout, 0}
      end
      send(pid, fun)
      assert_receive 1
      assert_receive {:timeout, 2}
    end

    test "{:noreply, state, :hibernate}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn(n) ->
        send(parent, n)
        {:noreply, n+1, :hibernate}
      end
      send(pid, fun)
      assert_receive 1
      assert_hibernated pid
      assert Server.call(pid, :state) === 2
    end

    test "{:persist, state}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn(n) ->
        send(parent, n)
        {:persist, n+1}
      end
      send(pid, fun)
      assert_receive 1
      assert Server.call(pid, :state) === 2
      assert {:ok, 1, 2} = Storage.Ets.load(EvalServer, 1, :foo)
    end

    test "{:stop, reason, state}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      _ = Process.flag(:trap_exit, true)
      parent = self()
      fun = fn(n) ->
        terminate = fn(m) ->
          send(parent, {:terminate, m})
        end
        send(parent, n)
        {:stop, {:shutdown, terminate}, n+1}
      end
      send(pid, fun)
      assert_receive 1
      assert_receive {:terminate, 2}
      assert_receive {:EXIT, ^pid, {:shutdown, _}}
    end
  end

  describe "handle_call/3" do
    test "{:reply, reply, state}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      fun = fn(_, n) -> {:reply, n, n+1} end
      assert Server.call(pid, fun) === 1
      assert Server.call(pid, :state) === 2
    end

    test "{:persist, reply, state}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      fun = fn(_, n) -> {:persist, n, n+1} end
      assert Server.call(pid, fun) === 1
      assert Server.call(pid, :state) === 2
      assert {:ok, 1, 2} = Storage.Ets.load(EvalServer, 1, :foo)
    end

    test "{:reply, reply, state, timeout}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn(_, n) ->
        timeout = fn() ->
          send(parent, {:timeout, n})
          {:noreply, n+1}
        end
        {:reply, n, timeout, 0}
      end
      assert Server.call(pid, fun) === 1
      assert_receive {:timeout, 1}
    end

    test "{:noreply, state}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      fun = fn(from, n) ->
        Server.reply(from, n)
        {:noreply, n+1}
      end
      assert Server.call(pid, fun) === 1
      assert Server.call(pid, :state) === 2
    end

    test "{:persist, state}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      fun = fn(from, n) ->
        Server.reply(from, n)
        {:persist, n+1}
      end
      assert Server.call(pid, fun) === 1
      assert Server.call(pid, :state) === 2
      assert {:ok, 1, 2} = Storage.Ets.load(EvalServer, 1, :foo)
    end

    test "{:noreply, state, timeout}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()

      fun = fn(from, n) ->
        timeout = fn() ->
          send(parent, {:timeout, n})
          {:noreply, n+1}
        end
        Server.reply(from, n)
        {:noreply, timeout, 0}
      end
      assert Server.call(pid, fun) === 1
      assert_receive {:timeout, 1}
    end

    test "{:reply, reply, state, :hibernate}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      fun = fn(_, n) -> {:reply, n, n+1, :hibernate} end
      assert Server.call(pid, fun) === 1
      assert_hibernated pid
      assert Server.call(pid, :state) === 2
    end

    test "{:noreply, state, :hibernate}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      fun = fn(from, n) ->
        Server.reply(from, n)
        {:noreply, n+1, :hibernate}
      end
      assert Server.call(pid, fun) === 1
      assert_hibernated pid
      assert Server.call(pid, :state) === 2
    end

    test "{:stop, reason, reply, state}" do
      _ = Process.flag(:trap_exit, true)
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn(_, n) ->
        terminate = fn(m) ->
          send(parent, {:terminate, m})
        end
        {:stop, {:shutdown, terminate}, n, n+1}
      end
      assert Server.call(pid, fun) === 1
      assert_receive {:terminate, 2}
      assert_receive {:EXIT, ^pid, {:shutdown, _}}
    end

    test "{:stop, reason, state}" do
      _ = Process.flag(:trap_exit, true)
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn(from, n) ->
        terminate = fn(m) ->
          send(parent, {:terminate, m})
        end
        Server.reply(from, n)
        {:stop, {:shutdown, terminate}, n+1}
      end
      assert Server.call(pid, fun) === 1
      assert_receive {:terminate, 2}
      assert_receive {:EXIT, ^pid, {:shutdown, _}}
    end
  end

  describe "reconcile/2" do
    test "begin_handoff" do
      {:ok, pid} = Server.start_link({EvalServer, 1})
      assert {0, 1} = Server.call(pid, {:swarm, :begin_handoff})
    end

    test "end_handoff remote < version" do
      {:ok, pid} = Server.start_link({EvalServer, 1})
      fun = fn n -> {:persist, n + 1} end
      assert :ok = Server.cast(pid, fun)
      assert :ok = Server.cast(pid, {:swarm, :end_handoff, {0, 1000}})
      assert Server.call(pid, :state) == 2
    end

    test "resolve_conflict remote < version" do
      {:ok, pid} = Server.start_link({EvalServer, 1})
      fun = fn n -> {:persist, n + 1} end
      assert :ok = Server.cast(pid, fun)
      assert :ok = Server.cast(pid, {:swarm, :resolve_conflict, {0, 1000}})
      assert Server.call(pid, :state) == 2
    end

    test "{:noreply, state}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn n ->
        reconcile = fn remote ->
          send(parent, {:reconcile, remote})
          {:noreply, n+1}
        end
        {:persist, reconcile}
      end
      send(pid, fun)
      assert :ok = Server.cast(pid, {:swarm, :resolve_conflict, {3, 1000}})
      assert_receive {:reconcile, 1000}
      assert Server.call(pid, :state) == 2
    end

    test "{:noreply, state, timeout}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn(n) ->
        timeout = fn() ->
          send(parent, {:timeout, n+1})
          {:noreply, n+1}
        end
        reconcile = fn remote ->
          send(parent, {:reconcile, remote})
          {:noreply, timeout, 0}
        end
        {:persist, reconcile}
      end
      send(pid, fun)
      assert :ok = Server.cast(pid, {:swarm, :resolve_conflict, {3, 1000}})
      assert_receive {:reconcile, 1000}
      assert_receive {:timeout, 2}
      assert Server.call(pid, :state) == 2
    end

    test "{:noreply, state, :hibernate}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn(n) ->
        reconcile = fn remote ->
          send(parent, {:reconcile, remote})
          {:noreply, n+1, :hibernate}
        end
        {:persist, reconcile}
      end
      send(pid, fun)
      assert :ok = Server.cast(pid, {:swarm, :resolve_conflict, {3, 1000}})
      assert_receive {:reconcile, 1000}
      assert_hibernated pid
      assert Server.call(pid, :state) === 2
    end

    test "{:persist, state}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      parent = self()
      fun = fn(n) ->
        reconcile = fn remote ->
          send(parent, {:reconcile, remote})
          {:persist, n+1}
        end
        {:persist, reconcile}
      end
      send(pid, fun)
      assert :ok = Server.cast(pid, {:swarm, :resolve_conflict, {3, 1000}})
      assert_receive {:reconcile, 1000}
      assert Server.call(pid, :state) === 2
      assert {:ok, 4, 2} = Storage.Ets.load(EvalServer, 1, :foo)
    end

    test "{:stop, reason, state}" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      _ = Process.flag(:trap_exit, true)
      parent = self()
      fun = fn(n) ->
        terminate = fn(m) ->
          send(parent, {:terminate, m})
        end
        reconcile = fn remote ->
          send(parent, {:reconcile, remote})
          {:stop, {:shutdown, terminate}, n+1}
        end
        {:persist, reconcile}
      end
      send(pid, fun)
      assert :ok = Server.cast(pid, {:swarm, :resolve_conflict, {3, 1000}})
      assert_receive {:reconcile, 1000}
      assert_receive {:terminate, 2}
      assert_receive {:EXIT, ^pid, {:shutdown, _}}
    end
  end

  describe "terminate/2" do
    test "when handing off" do
      {:ok, pid} = Server.start_link({EvalServer, 1})

      _ = Process.flag(:trap_exit, true)
      parent = self()
      fun = fn(n) ->
        terminate = fn ->
          send(parent, {:terminate, n})
        end
        {:noreply, terminate}
      end
      send(pid, fun)
      send(pid, {:swarm, :die})
      assert_receive {:terminate, 1}
      assert_receive {:EXIT, ^pid, {:shutdown, :handoff}}
    end
  end

  defp assert_hibernated(pid) do
    :timer.sleep(10)
    assert Process.info(pid, :current_function) ==
      {:current_function, {:erlang, :hibernate, 3}}
  end
end

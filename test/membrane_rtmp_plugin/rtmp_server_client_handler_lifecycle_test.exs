defmodule Membrane.RTMPServer.ClientHandlerLifecycleTest do
  @moduledoc """
  Who reclaims a client handler once its connection has ended.

  Every connection accepted on the RTMP port starts one `ClientHandler` under the
  server's `DynamicSupervisor`. A connection that never reaches `handle_new_client`
  has no consumer and nothing buffered, so nothing would ever reclaim it: those
  handlers must stop themselves, or the server accumulates one process per
  connection for the lifetime of the node.

  Once a consumer has been handed the connection the opposite holds. The consumer
  owns the lifecycle, because it may still have payloads to collect that the
  handler is holding, so the handler must survive the peer's close and wait to be
  stopped.
  """

  use ExUnit.Case, async: true

  alias Membrane.RTMPServer

  @moduletag :capture_log

  @localhost {127, 0, 0, 1}
  @await_timeout 2_000
  @quiet_period 300

  defmodule NoopHandler do
    @moduledoc false

    @behaviour Membrane.RTMPServer.ClientHandler

    @impl true
    def handle_init(_opts), do: %{}

    @impl true
    def handle_data_available(_payload, state), do: state

    @impl true
    def handle_delete_stream(state), do: state

    @impl true
    def handle_connection_closed(state), do: state

    @impl true
    def handle_info(_msg, state), do: state
  end

  defmodule ReportingHandler do
    @moduledoc false

    @behaviour Membrane.RTMPServer.ClientHandler

    @impl true
    def handle_init(opts), do: opts

    @impl true
    def handle_data_available(_payload, state), do: state

    @impl true
    def handle_delete_stream(state), do: state

    @impl true
    def handle_connection_closed(state) do
      send(state.test, {:connection_closed, self()})
      state
    end

    @impl true
    def handle_info(_msg, state), do: state
  end

  setup do
    {:ok, server} =
      RTMPServer.start_link(
        port: 0,
        use_ssl?: false,
        handle_new_client: fn _client_ref, _app, _stream_key -> NoopHandler end,
        # Long enough that the real timer never fires during a test; the timeout
        # path is driven explicitly below.
        client_timeout: Membrane.Time.seconds(30)
      )

    port = RTMPServer.get_port(server)
    supervisor = :sys.get_state(server).client_supervisor

    %{port: port, supervisor: supervisor}
  end

  describe "a connection that never reached a consumer" do
    test "stops the handler when the peer closes", ctx do
      {socket, handler} = connect(ctx)
      monitor = Process.monitor(handler)

      :ok = :gen_tcp.close(socket)

      assert_receive {:DOWN, ^monitor, :process, ^handler, :normal}, @await_timeout
      assert_no_children(ctx.supervisor)
    end

    test "stops the handler when the socket errors", ctx do
      {_socket, handler} = connect(ctx)
      monitor = Process.monitor(handler)

      send(handler, {:tcp_error, :sys.get_state(handler).socket, :econnreset})

      assert_receive {:DOWN, ^monitor, :process, ^handler, :normal}, @await_timeout
      assert_no_children(ctx.supervisor)
    end

    test "stops the handler when the unpublished client times out", ctx do
      {_socket, handler} = connect(ctx)
      monitor = Process.monitor(handler)

      send(handler, {:client_timeout, "liveapp", "somekey"})

      assert_receive {:DOWN, ^monitor, :process, ^handler, :normal}, @await_timeout
      assert_no_children(ctx.supervisor)
    end

    test "does not accumulate handlers across repeated connect-and-close cycles", ctx do
      for _ <- 1..20 do
        {socket, handler} = connect(ctx)
        monitor = Process.monitor(handler)
        :ok = :gen_tcp.close(socket)
        assert_receive {:DOWN, ^monitor, :process, ^handler, :normal}, @await_timeout
      end

      assert_no_children(ctx.supervisor)
    end
  end

  describe "a connection that reached a consumer" do
    test "keeps the handler alive on a peer close so buffered payloads survive", ctx do
      {socket, handler} = connect(ctx)
      install_consumer(handler)
      monitor = Process.monitor(handler)

      :ok = :gen_tcp.close(socket)

      # The close callback still runs: that is what drives end_of_stream.
      assert_receive {:connection_closed, ^handler}, @await_timeout

      # The consumer, not the handler, decides when the handler goes away.
      refute_receive {:DOWN, ^monitor, :process, ^handler, _reason}, @quiet_period
      assert Process.alive?(handler)
      assert children(ctx.supervisor) == [handler]
    end

    test "keeps the handler alive on a socket error", ctx do
      {_socket, handler} = connect(ctx)
      install_consumer(handler)
      monitor = Process.monitor(handler)

      send(handler, {:tcp_error, :sys.get_state(handler).socket, :econnreset})

      assert_receive {:connection_closed, ^handler}, @await_timeout
      refute_receive {:DOWN, ^monitor, :process, ^handler, _reason}, @quiet_period
      assert Process.alive?(handler)
    end

    test "stops the handler when the consumer rejects the stream key", ctx do
      {_socket, handler} = connect(ctx)
      install_consumer(handler)
      monitor = Process.monitor(handler)

      # How a rejected stream key is torn down: the consumer returns a no-op
      # handler and sends this synthetic timeout to close the connection.
      send(handler, {:client_timeout, "liveapp", "badkey"})

      assert_receive {:connection_closed, ^handler}, @await_timeout
      assert_receive {:DOWN, ^monitor, :process, ^handler, :normal}, @await_timeout
      assert_no_children(ctx.supervisor)
    end

    test "the consumer can still stop the handler itself", ctx do
      {socket, handler} = connect(ctx)
      install_consumer(handler)
      monitor = Process.monitor(handler)

      :ok = :gen_tcp.close(socket)
      assert_receive {:connection_closed, ^handler}, @await_timeout

      :ok = GenServer.stop(handler, :normal, @await_timeout)

      assert_receive {:DOWN, ^monitor, :process, ^handler, :normal}, @await_timeout
      assert_no_children(ctx.supervisor)
    end
  end

  # Opens a client connection and returns it with the handler the server started
  # for it. Waits for the handler so the test never races the accept loop.
  defp connect(%{port: port, supervisor: supervisor}) do
    {:ok, socket} = :gen_tcp.connect(@localhost, port, [:binary, active: false])

    handler =
      await(fn ->
        case children(supervisor) do
          [handler] -> {:ok, handler}
          _other -> :retry
        end
      end)

    {socket, handler}
  end

  # Puts the handler into the state it reaches once handle_new_client has run,
  # without driving a full RTMP publish: a handler module is installed and the
  # connection counts as notified.
  defp install_consumer(handler) do
    test = self()

    :sys.replace_state(handler, fn state ->
      %{
        state
        | notified_about_client?: true,
          handler: ReportingHandler,
          handler_state: %{test: test}
      }
    end)

    :ok
  end

  # A stopped handler must also leave the supervisor, which a restart would
  # violate: `:temporary` children are dropped rather than restarted.
  defp assert_no_children(supervisor) do
    assert await(fn ->
             case children(supervisor) do
               [] -> {:ok, true}
               _other -> :retry
             end
           end)
  end

  defp children(supervisor) do
    for {_id, pid, _type, _modules} <- DynamicSupervisor.which_children(supervisor),
        is_pid(pid),
        do: pid
  end

  defp await(check, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @await_timeout

    case check.() do
      {:ok, value} ->
        value

      :retry ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("condition not reached within #{@await_timeout}ms")
        else
          Process.sleep(10)
          await(check, deadline)
        end
    end
  end
end

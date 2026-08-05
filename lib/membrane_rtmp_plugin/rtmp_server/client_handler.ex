defmodule Membrane.RTMPServer.ClientHandler do
  @moduledoc """
  A behaviour describing the actions that might be taken by the client
  handler in response to different events.
  """

  # It also containts functions responsible for maintaining the lifecycle of the
  # client connection.

  use GenServer

  require Logger
  alias Membrane.RTMP.{Handshake, MessageHandler, MessageParser, Messages}

  @typedoc """
  A type representing a module which implements `#{inspect(__MODULE__)}` behaviour.
  """
  @type t :: module()

  @typedoc """
  Type representing the user defined state of the client handler.
  """
  @type state :: any()

  @doc """
  The callback invoked once the client handler is created.
  It should return the initial state of the client handler.
  """
  @callback handle_init(any()) :: state()

  @doc """
  The callback invoked when new piece of data is received from a given client.
  """
  @callback handle_data_available(payload :: binary(), state :: state()) :: state()

  @doc """
  Callback invoked when the RMTP stream is finished.
  """
  @callback handle_delete_stream(state :: state()) :: state()

  @doc """
  Callback invoked when the socket connection is terminated. In normal
  conditions, `handle_delete_stream` is called before this one. If delete_stream
  is not called and connection_closed is, it might just mean that the
  connection was lost (for instance when TCP socket is closed unexpectedly).

  It is up to the users to determined how to handle it in their case.
  """
  @callback handle_connection_closed(state :: state()) :: state()

  @doc """
  The optional callback invoked when the client handler receives RTMP message with
  metadata information (just like a resolution or a framerate of a video stream).
  The following messages are considered the ones that contain metadata:
  1) OnMetaData
  2) SetDataFrame
  """
  @callback handle_metadata(
              {:metadata_message, Messages.OnMetaData.t() | Messages.SetDataFrame.t()},
              state()
            ) :: state()

  @doc """
  The callback invoked when the client handler receives a message
  that is not recognized as an internal message of the client handler.
  """
  @callback handle_info(msg :: term(), state()) :: state()

  @optional_callbacks handle_metadata: 2

  defguardp is_metadata_message(message)
            when is_struct(message, Messages.OnMetaData) or
                   is_struct(message, Messages.SetDataFrame)

  @doc """
  Makes the client handler ask client for the desired number of buffers
  """
  @spec demand_data(pid(), non_neg_integer()) :: :ok
  def demand_data(client_reference, how_many_buffers_demanded) do
    send(client_reference, {:demand_data, how_many_buffers_demanded})
    :ok
  end

  @impl true
  def init(opts) do
    opts = Map.new(opts)
    message_parser_state = Handshake.init_server() |> MessageParser.init()
    message_handler_state = MessageHandler.init(%{socket: opts.socket, use_ssl?: opts.use_ssl?})

    {:ok,
     %{
       socket: opts.socket,
       use_ssl?: opts.use_ssl?,
       message_parser_state: message_parser_state,
       message_handler_state: message_handler_state,
       handler: nil,
       handler_state: nil,
       app: nil,
       stream_key: nil,
       server: opts.server,
       buffers_demanded: 0,
       published?: false,
       notified_about_client?: false,
       handle_new_client: opts.handle_new_client,
       client_timeout: opts.client_timeout
     }}
  end

  @impl true
  def handle_info({:tcp, socket, data}, %{use_ssl?: false} = state) when state.socket == socket do
    handle_data(data, state)
  end

  # Who reclaims a handler once its connection has ended depends on whether the
  # connection ever reached a consumer.
  #
  # Before `handle_new_client` runs there is no consumer, no handler module and
  # nothing buffered, so the process can only sit in `:gen_server.loop/7`
  # forever. Every accepted connection starts one of these, so leaving them
  # behind leaks a process and a supervisor child entry per connection for the
  # lifetime of the node. These stop themselves.
  #
  # After `handle_new_client` the consumer owns the lifecycle, and the handler
  # must not stop on its own: `Source.ClientHandlerImpl` buffers payloads until
  # the pipeline reaches `handle_playing` and sends `{:send_me_data, pid}`, so a
  # handler that exited on the peer's FIN would discard media the consumer has
  # not collected yet. `handle_connection_closed` still runs, which is what
  # drives `end_of_stream`; the consumer stops the handler afterwards.
  @impl true
  def handle_info({:tcp_closed, socket}, %{use_ssl?: false} = state)
      when state.socket == socket do
    connection_ended(state)
  end

  @impl true
  def handle_info({:tcp_error, socket, reason}, %{use_ssl?: false} = state)
      when state.socket == socket do
    Logger.warning("RTMP client socket error: #{inspect(reason)}")
    connection_ended(state)
  end

  @impl true
  def handle_info({:ssl, socket, data}, %{use_ssl?: true} = state) when state.socket == socket do
    handle_data(data, state)
  end

  @impl true
  def handle_info({:ssl_closed, socket}, %{use_ssl?: true} = state) when state.socket == socket do
    connection_ended(state)
  end

  @impl true
  def handle_info({:ssl_error, socket, reason}, %{use_ssl?: true} = state)
      when state.socket == socket do
    Logger.warning("RTMPS client socket error: #{inspect(reason)}")
    connection_ended(state)
  end

  @impl true
  def handle_info(:control_granted, state) do
    request_data(state)
    {:noreply, state}
  end

  @impl true
  def handle_info({:demand_data, how_many_buffers_demanded}, state) do
    state = finish_handshake(state) |> Map.replace!(:buffers_demanded, how_many_buffers_demanded)
    request_data(state)
    {:noreply, state}
  end

  @impl true
  def handle_info({:client_timeout, app, stream_key}, %{published?: false} = state) do
    Logger.warning("No demand made for client /#{app}/#{stream_key}, terminating connection.")
    close_socket(state)

    # This branch always stops, consumer or not. It fires only while the client
    # is unpublished, which is how a rejected stream key is torn down, and its
    # whole purpose is to terminate the connection rather than wait out a client
    # that will never publish. Closing our own socket delivers no `:tcp_closed`,
    # so it cannot inherit the stop from the clauses above.
    {:stop, :normal, handle_event(:connection_closed, state)}
  end

  @impl true
  def handle_info({:client_timeout, _app, _stream_key}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(other_msg, state) do
    handler_state = state.handler.handle_info(other_msg, state.handler_state)

    {:noreply, %{state | handler_state: handler_state}}
  end

  defp handle_data(data, state) do
    {messages, message_parser_state} =
      MessageParser.parse_packet_messages(data, state.message_parser_state)

    {message_handler_state, events} =
      MessageHandler.handle_client_messages(messages, state.message_handler_state)

    state =
      if message_handler_state.publish_msg != nil and not state.notified_about_client? do
        %{publish_msg: %Membrane.RTMP.Messages.Publish{stream_key: stream_key}} =
          message_handler_state

        if not is_function(state.handle_new_client) do
          raise "handle_new_client is not a function"
        end

        {handler_module, opts} =
          case state.handle_new_client.(self(), state.app, stream_key) do
            {handler_module, opts} -> {handler_module, opts}
            handler_module -> {handler_module, %{}}
          end

        Process.send_after(
          self(),
          {:client_timeout, state.app, stream_key},
          Membrane.Time.as_milliseconds(state.client_timeout, :round)
        )

        %{
          state
          | notified_about_client?: true,
            handler: handler_module,
            handler_state: handler_module.handle_init(opts)
        }
      else
        state
      end

    state = Enum.reduce(events, state, &handle_event/2)

    state =
      if state.notified_about_client? &&
           Kernel.function_exported?(state.handler, :handle_metadata, 2) do
        new_handler_state =
          Enum.reduce(messages, state.handler_state, fn
            {%Membrane.RTMP.Header{}, message}, handler_state
            when is_metadata_message(message) ->
              state.handler.handle_metadata({:metadata_message, message}, handler_state)

            _other, handler_state ->
              handler_state
          end)

        %{state | handler_state: new_handler_state}
      else
        state
      end

    request_data(state)

    {:noreply,
     %{
       state
       | message_parser_state: message_parser_state,
         message_handler_state: message_handler_state
     }}
  end

  defp handle_event(event, state) do
    # call callbacks
    case event do
      :connection_closed ->
        case state.handler do
          nil ->
            state

          handler ->
            new_handler_state = handler.handle_connection_closed(state.handler_state)
            %{state | handler_state: new_handler_state}
        end

      :delete_stream ->
        new_handler_state = state.handler.handle_delete_stream(state.handler_state)
        %{state | handler_state: new_handler_state}

      {:set_chunk_size_required, chunk_size} ->
        new_message_parser_state = %{state.message_parser_state | chunk_size: chunk_size}
        %{state | message_parser_state: new_message_parser_state}

      {:data_available, payload} ->
        new_handler_state =
          state.handler.handle_data_available(payload, state.handler_state)

        %{
          state
          | handler_state: new_handler_state,
            buffers_demanded: state.buffers_demanded - 1
        }

      {:connected, connected_msg} ->
        %{state | app: connected_msg.app}

      {:published, publish_msg} ->
        %{
          state
          | stream_key: publish_msg.stream_key,
            published?: true
        }
    end
  end

  defp request_data(state) do
    if state.buffers_demanded > 0 or state.published? == false do
      if state.use_ssl? do
        :ssl.setopts(state.socket, active: :once)
      else
        :inet.setopts(state.socket, active: :once)
      end
    end
  end

  # Run the close callback, then reclaim the process only when no consumer was
  # ever handed this connection. See the clauses above for why the two cases
  # differ.
  defp connection_ended(state) do
    state = handle_event(:connection_closed, state)

    if state.notified_about_client? do
      {:noreply, state}
    else
      {:stop, :normal, state}
    end
  end

  # Close through the module that opened the socket; `:gen_tcp.close/1` does not
  # close an SSL socket.
  defp close_socket(%{use_ssl?: true} = state), do: :ssl.close(state.socket)
  defp close_socket(state), do: :gen_tcp.close(state.socket)

  defp finish_handshake(state) when not state.published? do
    {message_handler_state, events} =
      MessageHandler.send_publish_success(state.message_handler_state)

    state = Enum.reduce(events, state, &handle_event/2)
    %{state | message_handler_state: message_handler_state}
  end

  defp finish_handshake(state), do: state
end

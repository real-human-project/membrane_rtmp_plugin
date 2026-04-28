defmodule Membrane.RTMP.Responses do
  @moduledoc false

  alias Membrane.RTMP.Messages

  @type transaction_id_t :: float() | non_neg_integer()

  @doc """
  Returns a default success response on connect request.
  """
  @spec connection_success() :: struct()
  def connection_success() do
    %Messages.Anonymous{
      name: "_result",
      # transaction ID is always 1 for connect request/responses
      tx_id: 1,
      properties: [
        # Per RTMP spec, the first argument is the server "Properties Object"
        # and the second is the "Information Object" (status). HaishinKit on
        # iOS, however, looks at `arguments.first` for the `code`/`level`
        # status fields — see `RTMPMessage.swift:340` and the guard in
        # `RTMPConnection.on(status:)` / apivideo_live_stream's
        # `rtmpStatusHandler`. Without `code` in the first arg, both bail out
        # and the publish handshake stalls for ~27 seconds before HaishinKit
        # falls back to a slower path. Mirror `code`/`level` into the first
        # argument too so HaishinKit's first-arg lookup succeeds. Standard
        # clients that key off the second arg (or use a responder pattern)
        # still see the canonical status object below.
        %{
          "fmsVer" => "FMS/3,0,1,123",
          "capabilities" => 31.0,
          "level" => "status",
          "code" => "NetConnection.Connect.Success"
        },
        %{
          "level" => "status",
          "code" => "NetConnection.Connect.Success",
          "description" => "Connection succeeded.",
          "objectEncoding" => 0.0
        }
      ]
    }
  end

  @doc """
  Returns a publishment success message.
  """
  @spec publish_success(String.t()) :: struct()
  def publish_success(stream_key) do
    %Messages.Anonymous{
      name: "onStatus",
      # transaction ID is always 0 for publish request/responses
      tx_id: 0,
      properties: [
        :null,
        %{
          "level" => "status",
          "code" => "NetStream.Publish.Start",
          "description" => "#{stream_key} is now published",
          "details" => stream_key
        }
      ]
    }
  end

  @doc """
  Returns a bandwidth measurement done message.
  """
  @spec on_bw_done() :: struct()
  def on_bw_done() do
    %Messages.Anonymous{
      name: "onBWDone",
      tx_id: 0,
      properties: [
        :null,
        # from ffmpeg rtmp server implementation
        8192.0
      ]
    }
  end

  @doc """
  Returns a default `_result` response with arbitrary body.

  The body can be set by specifying the properties list.
  """
  @spec default_result(transaction_id_t(), [any()]) :: struct()
  def default_result(tx_id, properties) do
    %Messages.Anonymous{
      name: "_result",
      tx_id: tx_id,
      properties: properties
    }
  end
end

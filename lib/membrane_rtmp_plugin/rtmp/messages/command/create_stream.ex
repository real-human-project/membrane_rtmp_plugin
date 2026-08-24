defmodule Membrane.RTMP.Messages.CreateStream do
  @moduledoc false

  @behaviour Membrane.RTMP.Message

  alias Membrane.RTMP.AMF0.Encoder

  defstruct tx_id: 0

  @type t :: %__MODULE__{
          tx_id: non_neg_integer()
        }

  @names ["createStream", "@createStream"]

  @impl true
  def from_data([name, tx_id, :null]) when name in @names do
    %__MODULE__{tx_id: tx_id}
  end

  # Some encoders (e.g. HaishinKit.kt) append extra trailing argument(s) to
  # `createStream` beyond the spec's `[name, tx_id, null]`. They carry no
  # meaning for publishing, so accept and ignore them rather than crashing the
  # client handler with a `function_clause` error.
  def from_data([name, tx_id, :null | _extra]) when name in @names do
    %__MODULE__{tx_id: tx_id}
  end

  defimpl Membrane.RTMP.Messages.Serializer do
    require Membrane.RTMP.Header

    @impl true
    def serialize(%@for{tx_id: tx_id}) do
      Encoder.encode(["createStream", tx_id, :null])
    end

    @impl true
    def type(%@for{}), do: Membrane.RTMP.Header.type(:amf_command)
  end
end

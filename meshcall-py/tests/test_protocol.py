from meshcall.protocol import (
    CallOpenFrame,
    StreamWindowFrame,
    decode_frame,
    encode_frame,
)


def test_frame_round_trip() -> None:
    frame = CallOpenFrame(
        call_id="call-1",
        service="example.v1.ExampleService",
        method="echo",
        payload={"message": "hello"},
    )

    assert decode_frame(encode_frame(frame)) == frame


def test_stream_credit_names_the_producer_direction() -> None:
    frame = StreamWindowFrame(
        call_id="call-1",
        direction="server",
        credit=32,
    )

    decoded = decode_frame(encode_frame(frame))
    assert isinstance(decoded, StreamWindowFrame)
    assert decoded.direction == "server"
    assert decoded.credit == 32

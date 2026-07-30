import pytest
from pydantic import ValidationError

from meshcall.protocol import (
    CallOpenFrame,
    MethodRegistrationResult,
    ServerRegisterAckFrame,
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


def test_registration_ack_reports_partial_method_acceptance() -> None:
    frame = ServerRegisterAckFrame(
        instance_id="instance-1",
        methods=(
            MethodRegistrationResult(
                service="example.v1.ExampleService",
                method="echo",
                accepted=True,
            ),
            MethodRegistrationResult(
                service="example.v1.ExampleService",
                method="exclusive",
                accepted=False,
                reason="balance_disabled",
            ),
        ),
    )

    assert decode_frame(encode_frame(frame)) == frame

    with pytest.raises(ValidationError):
        MethodRegistrationResult(
            service="example.v1.ExampleService",
            method="invalid",
            accepted=False,
        )

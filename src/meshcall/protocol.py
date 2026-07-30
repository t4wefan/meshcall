from __future__ import annotations

from typing import Annotated, Any, Literal, Self, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    model_validator,
)

from meshcall.errors import ProtocolError
from meshcall.ir import BalancePolicy, StreamKind

PROTOCOL_VERSION = "meshcall/1"
Direction: TypeAlias = Literal["client", "server"]


class FrameModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class HelloFrame(FrameModel):
    kind: Literal["hello"] = "hello"
    protocol: str = PROTOCOL_VERSION
    role: Literal["client", "server"]
    peer_id: str
    instance_id: str | None = None


class HelloAckFrame(FrameModel):
    kind: Literal["hello.ack"] = "hello.ack"
    protocol: str = PROTOCOL_VERSION
    connection_id: str


class RegisteredMethod(FrameModel):
    name: str
    stream: StreamKind
    balance: BalancePolicy
    schema_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class RegisteredService(FrameModel):
    name: str
    methods: tuple[RegisteredMethod, ...]


class ServerRegisterFrame(FrameModel):
    kind: Literal["server.register"] = "server.register"
    instance_id: str
    services: tuple[RegisteredService, ...]


class MethodRegistrationResult(FrameModel):
    service: str
    method: str
    accepted: bool
    reason: Literal["schema_mismatch", "balance_disabled"] | None = None

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        if self.accepted == (self.reason is not None):
            raise ValueError(
                "Accepted methods must omit a reason and rejected methods need one"
            )
        return self


class ServerRegisterAckFrame(FrameModel):
    kind: Literal["server.register.ack"] = "server.register.ack"
    instance_id: str
    methods: tuple[MethodRegistrationResult, ...]


class CallOpenFrame(FrameModel):
    kind: Literal["call.open"] = "call.open"
    call_id: str
    service: str
    method: str
    payload: Any
    deadline_unix_ms: int | None = None


class CallResultFrame(FrameModel):
    kind: Literal["call.result"] = "call.result"
    call_id: str
    payload: Any = None


class ErrorPayload(FrameModel):
    code: str
    message: str
    retryable: bool = False
    details: dict[str, Any] | None = None


class CallErrorFrame(FrameModel):
    kind: Literal["call.error"] = "call.error"
    call_id: str
    error: ErrorPayload


class CallCancelFrame(FrameModel):
    kind: Literal["call.cancel"] = "call.cancel"
    call_id: str
    reason: str = "cancelled"


class StreamItemFrame(FrameModel):
    kind: Literal["stream.item"] = "stream.item"
    call_id: str
    direction: Direction
    sequence: int = Field(ge=0)
    payload: Any


class StreamEndFrame(FrameModel):
    kind: Literal["stream.end"] = "stream.end"
    call_id: str
    direction: Direction


class StreamWindowFrame(FrameModel):
    kind: Literal["stream.window"] = "stream.window"
    call_id: str
    direction: Direction
    credit: int = Field(gt=0)


class PingFrame(FrameModel):
    kind: Literal["ping"] = "ping"
    nonce: str


class PongFrame(FrameModel):
    kind: Literal["pong"] = "pong"
    nonce: str


Frame: TypeAlias = Annotated[
    HelloFrame
    | HelloAckFrame
    | ServerRegisterFrame
    | ServerRegisterAckFrame
    | CallOpenFrame
    | CallResultFrame
    | CallErrorFrame
    | CallCancelFrame
    | StreamItemFrame
    | StreamEndFrame
    | StreamWindowFrame
    | PingFrame
    | PongFrame,
    Field(discriminator="kind"),
]

_FRAME_ADAPTER = TypeAdapter(Frame)


def encode_frame(frame: Frame) -> str:
    return _FRAME_ADAPTER.dump_json(frame, by_alias=True).decode()


def decode_frame(data: str | bytes) -> Frame:
    try:
        return _FRAME_ADAPTER.validate_json(data)
    except ValidationError as exc:
        raise ProtocolError(f"Invalid frame: {exc}") from exc

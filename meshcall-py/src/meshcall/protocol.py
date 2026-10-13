from __future__ import annotations

import json
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
)

from meshcall.errors import ProtocolError
from meshcall.ir import BalancePolicy, StreamKind

PROTOCOL_VERSION = "meshcall/1"
Direction: TypeAlias = Literal["client", "server"]
Metadata: TypeAlias = dict[str, str]


class FrameModel(BaseModel):
    # meshcall/1 compatibility rule: unknown fields are ignored so that later
    # minor revisions can add optional fields without breaking older peers.
    model_config = ConfigDict(extra="ignore")


class _MetadataFrame(FrameModel):
    metadata: Metadata | None = None

    @field_validator("metadata")
    @classmethod
    def _non_empty_keys(cls, value: Metadata | None) -> Metadata | None:
        if value is not None and any(key == "" for key in value):
            raise ValueError("metadata keys must not be empty")
        return value


class HelloFrame(FrameModel):
    kind: Literal["hello"] = "hello"
    protocol: str = PROTOCOL_VERSION
    role: Literal["client", "server"]
    peer_id: str
    instance_id: str | None = None
    # Reserved for negotiating optional features without a protocol bump.
    capabilities: list[str] | None = None


class HelloAckFrame(FrameModel):
    kind: Literal["hello.ack"] = "hello.ack"
    protocol: str = PROTOCOL_VERSION
    connection_id: str


class RegisteredMethod(FrameModel):
    name: str
    stream: StreamKind
    balance: BalancePolicy


class RegisteredService(FrameModel):
    name: str
    methods: tuple[RegisteredMethod, ...]
    # The portable contract entry (schemas, descriptions, annotations) so the
    # Router can serve it to tooling; opaque to routing.
    contract: dict[str, Any] | None = None


class ServerRegisterFrame(FrameModel):
    kind: Literal["server.register"] = "server.register"
    instance_id: str
    services: tuple[RegisteredService, ...]
    # Calls this instance accepts concurrently across its services; absent = unlimited.
    max_inflight: int | None = Field(default=None, gt=0)


class ServerRegisterAckFrame(FrameModel):
    kind: Literal["server.register.ack"] = "server.register.ack"
    instance_id: str


class ServerDrainFrame(FrameModel):
    """Instance → Router: leave every pool, keep serving routed calls."""

    kind: Literal["server.drain"] = "server.drain"


class ServerDrainAckFrame(FrameModel):
    kind: Literal["server.drain.ack"] = "server.drain.ack"
    instance_id: str | None = None


class CallOpenFrame(_MetadataFrame):
    kind: Literal["call.open"] = "call.open"
    call_id: str
    service: str
    method: str
    payload: Any
    timeout_ms: int | None = Field(default=None, gt=0)
    idle_timeout_ms: int | None = Field(default=None, gt=0)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128)


class CallResultFrame(_MetadataFrame):
    kind: Literal["call.result"] = "call.result"
    call_id: str
    payload: Any = None


class ErrorPayload(FrameModel):
    code: str
    message: str
    retryable: bool = False
    details: dict[str, Any] | None = None


class CallErrorFrame(_MetadataFrame):
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
    | ServerDrainFrame
    | ServerDrainAckFrame
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
        raise ProtocolError(
            f"Invalid frame: {exc}", call_id=_salvage_call_id(data)
        ) from exc


def _salvage_call_id(data: str | bytes) -> str | None:
    """Best-effort ``call_id`` from a frame that failed validation."""
    try:
        value = json.loads(data)
    except ValueError:
        return None
    call_id = value.get("call_id") if isinstance(value, dict) else None
    return call_id if isinstance(call_id, str) and call_id else None

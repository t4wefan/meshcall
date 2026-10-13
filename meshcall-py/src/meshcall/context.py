"""Per-call context shared by handlers, interceptors, and generated clients."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from meshcall.logging import RpcLogger

DEFAULT_PING_INTERVAL = 15.0
DEFAULT_PING_TIMEOUT = 10.0
DEFAULT_IDLE_TIMEOUT = 45.0
RESERVED_METADATA_PREFIX = "meshcall-"


def client_deadline_grace(timeout: float) -> float:
    """Extra time the client waits past ``timeout`` for the peer's own
    ``deadline_exceeded`` before failing the call locally."""
    return max(1.0, timeout * 0.1)


def validate_metadata(metadata: Mapping[str, str] | None) -> dict[str, str] | None:
    if metadata is None:
        return None
    for key, value in metadata.items():
        if not isinstance(key, str) or key == "":
            raise ValueError("metadata keys must be non-empty strings")
        if key.startswith(RESERVED_METADATA_PREFIX):
            raise ValueError(f"metadata key {key!r} uses the reserved prefix")
        if not isinstance(value, str):
            raise TypeError(f"metadata value for {key!r} must be a string")
    return dict(metadata)


@dataclass(frozen=True)
class CallOptions:
    """Caller-side options for one RPC. All durations are seconds."""

    timeout: float | None = None
    idle_timeout: float | None = None
    metadata: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be positive")
        if self.idle_timeout is not None and self.idle_timeout <= 0:
            raise ValueError("idle_timeout must be positive")
        validate_metadata(self.metadata)


@dataclass
class RpcContext:
    """What a service handler can learn about, and attach to, the current call.

    Declare a parameter named ``ctx`` or ``context`` (or annotated with
    ``RpcContext``) on an RPC method to receive it.
    """

    call_id: str
    service: str
    method: str
    metadata: Mapping[str, str]
    logger: RpcLogger
    _deadline: float | None = field(default=None, repr=False)
    _trailers: dict[str, str] = field(default_factory=dict, repr=False)

    @property
    def deadline(self) -> float | None:
        """Absolute ``time.monotonic()`` value by which the call must finish."""
        return self._deadline

    def remaining(self) -> float | None:
        """Seconds left in the call's budget, or ``None`` when unlimited."""
        if self._deadline is None:
            return None
        return max(0.0, self._deadline - time.monotonic())

    def set_trailer(self, key: str, value: str) -> None:
        validate_metadata({key: value})
        self._trailers[key] = value

    @property
    def trailers(self) -> Mapping[str, str]:
        return MappingProxyType(self._trailers)

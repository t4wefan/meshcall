"""Per-call context shared by handlers, interceptors, and generated clients."""

from __future__ import annotations

import random
import time
from collections.abc import Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from meshcall.logging import RpcLogger

DEFAULT_PING_INTERVAL = 15.0
DEFAULT_PING_TIMEOUT = 10.0
DEFAULT_IDLE_TIMEOUT = 45.0
RESERVED_METADATA_PREFIX = "meshcall-"


def client_deadline_grace(timeout: float) -> float:
    """Extra time the client waits past ``timeout`` for the peer's own
    ``deadline_exceeded`` before failing the call locally."""
    return max(1.0, timeout * 0.1)


@dataclass(frozen=True)
class ReconnectPolicy:
    """Exponential backoff with jitter for re-establishing a lost connection.

    Applies to clients reconnecting to a server or Router and to Router-mode
    servers re-registering after the Router drops. ``max_attempts=None`` keeps
    trying until the owner stops or (for clients) the call's timeout expires.
    """

    enabled: bool = True
    initial_delay: float = 0.1
    max_delay: float = 30.0
    multiplier: float = 2.0
    jitter: float = 0.2
    max_attempts: int | None = None

    def __post_init__(self) -> None:
        if self.initial_delay <= 0 or self.max_delay < self.initial_delay:
            raise ValueError("initial_delay must be positive and <= max_delay")
        if self.multiplier < 1 or not 0 <= self.jitter <= 1:
            raise ValueError("multiplier must be >= 1 and jitter within [0, 1]")
        if self.max_attempts is not None and self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")

    def delays(self) -> Iterator[float]:
        """Yield successive sleep durations; exhausted after ``max_attempts``."""
        delay = self.initial_delay
        attempt = 0
        while self.max_attempts is None or attempt < self.max_attempts:
            attempt += 1
            spread = delay * self.jitter
            yield max(0.0, delay + random.uniform(-spread, spread))
            delay = min(self.max_delay, delay * self.multiplier)


DEFAULT_RECONNECT = ReconnectPolicy()
NO_RECONNECT = ReconnectPolicy(enabled=False)


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


MAX_IDEMPOTENCY_KEY_BYTES = 128


@dataclass(frozen=True)
class CallOptions:
    """Caller-side options for one RPC. All durations are seconds.

    ``idempotency_key`` lets the Router join or replay duplicate unary calls
    and lets the retry policy re-send a non-idempotent method safely.
    ``idempotent`` states the method's own semantics; generated clients set it
    from the contract so callers need not.
    """

    timeout: float | None = None
    idle_timeout: float | None = None
    metadata: Mapping[str, str] | None = None
    idempotency_key: str | None = None
    idempotent: bool = False
    #: Address one service instance by id; required for targeted services.
    instance: str | None = None

    def __post_init__(self) -> None:
        if self.instance is not None and not self.instance:
            raise ValueError("instance must not be empty")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be positive")
        if self.idle_timeout is not None and self.idle_timeout <= 0:
            raise ValueError("idle_timeout must be positive")
        validate_metadata(self.metadata)
        if self.idempotency_key is not None and not (
            0 < len(self.idempotency_key.encode()) <= MAX_IDEMPOTENCY_KEY_BYTES
        ):
            raise ValueError(
                f"idempotency_key must be 1..{MAX_IDEMPOTENCY_KEY_BYTES} bytes"
            )

    def replace(self, **changes: Any) -> CallOptions:
        from dataclasses import replace

        return replace(self, **changes)


@dataclass(frozen=True)
class RetryPolicy:
    """Client-side retry for failed unary calls.

    Only errors marked ``retryable`` are retried, and a method that is not
    declared ``idempotent`` is retried only when the call carries an
    ``idempotency_key``. Attempts share the call's ``timeout``; backoff uses
    exponential delays with jitter. Streams are never retried.
    """

    enabled: bool = True
    max_attempts: int = 3
    initial_delay: float = 0.1
    max_delay: float = 2.0
    multiplier: float = 2.0
    jitter: float = 0.2
    retry_codes: frozenset[str] = frozenset({"unavailable", "resource_exhausted"})

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.initial_delay <= 0 or self.max_delay < self.initial_delay:
            raise ValueError("initial_delay must be positive and <= max_delay")

    def delay(self, attempt: int) -> float:
        """Sleep before retry number ``attempt`` (1-based)."""
        base = min(
            self.max_delay, self.initial_delay * (self.multiplier ** (attempt - 1))
        )
        spread = base * self.jitter
        return max(0.0, base + random.uniform(-spread, spread))


DEFAULT_RETRY = RetryPolicy()
NO_RETRY = RetryPolicy(enabled=False)


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
    idempotency_key: str | None = None
    #: This server's instance id; what a targeted caller addressed.
    instance_id: str = ""
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


# -- interceptors -----------------------------------------------------------

ServerCallNext = Callable[[], Awaitable[Any]]
"""Runs the rest of the server chain: later interceptors, then the handler.

Awaiting it covers the whole call, including every streamed item, and returns
the handler's final result (``None`` for server streams). Exceptions raised by
the handler or by the runtime (validation, cancellation) propagate through it.
"""

ServerInterceptor = Callable[[RpcContext, Any, ServerCallNext], Awaitable[Any]]
"""``async def intercept(ctx, request, call_next) -> result``.

Server interceptors run on the service worker loop in the order given to
``RpcServer(interceptors=[...])``; the first one is outermost. ``request`` is
the validated request model. Raising ``MeshCallError`` short-circuits the call
with that error; the result an interceptor returns is validated like a
handler's.
"""


@dataclass
class ClientCallInfo:
    """What a client interceptor sees for one outgoing call.

    ``options`` may be replaced to add metadata or adjust timeouts before the
    call is sent. ``trailers`` is filled for unary and client-stream calls once
    ``call_next`` returns; streams expose trailers on the returned object.
    """

    service: str
    method: str
    stream: str
    options: CallOptions
    trailers: Mapping[str, str] = field(default_factory=dict)


ClientCallNext = Callable[[ClientCallInfo], Awaitable[Any]]
"""Runs the rest of the client chain and returns what the call returns: the
response for unary and client-stream calls, or the lazy stream object for
server-stream and duplex calls. It may be awaited more than once to retry."""

ClientInterceptor = Callable[[ClientCallInfo, ClientCallNext], Awaitable[Any]]
"""``async def intercept(info, call_next) -> result``; first interceptor is
outermost."""

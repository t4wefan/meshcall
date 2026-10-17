"""OpenTelemetry for MeshCall calls: spans, metrics and trace propagation.

Opt-in (``pip install meshcall[otel]``)::

    from meshcall.otel import client_interceptor, server_interceptor

    client = ChatClient(driver, interceptors=[client_interceptor()])
    server = RpcServer(services=[Chat()], driver=..., interceptors=[server_interceptor()])

Trace context travels as call metadata (``traceparent`` / ``tracestate``,
W3C), so a trace continues across languages and through the Router, which
forwards metadata untouched. Only ``opentelemetry-api`` is used: with no SDK
configured everything is a no-op.

Each call gets a span (``CLIENT`` / ``SERVER``) named ``service/method`` with
``rpc.system = "meshcall"``, ``rpc.service``, ``rpc.method`` and, on failure,
``rpc.meshcall.error_code``; plus ``meshcall.calls`` (counter) and
``meshcall.call.duration`` (histogram, seconds) by service, method, code
(``ok`` or the error code) and side. A server span covers the whole call,
streams included. A client span covers a unary or client-stream call; for a
server stream or duplex call it covers opening the call (the
stream's items are not awaited by the interceptor).
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from opentelemetry import metrics, propagate, trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from meshcall.context import (
    ClientCallInfo,
    ClientCallNext,
    ClientInterceptor,
    RpcContext,
    ServerCallNext,
    ServerInterceptor,
)
from meshcall.errors import MeshCallError

_SCOPE = "meshcall"


def _instruments(meter_provider: Any) -> tuple[Any, Any]:
    meter = metrics.get_meter(_SCOPE, meter_provider=meter_provider)
    calls = meter.create_counter(
        "meshcall.calls", unit="{call}", description="MeshCall calls by result"
    )
    duration = meter.create_histogram(
        "meshcall.call.duration", unit="s", description="MeshCall call duration"
    )
    return calls, duration


def _code(error: BaseException | None) -> str:
    if error is None:
        return "ok"
    if isinstance(error, MeshCallError):
        return error.code
    return "cancelled" if type(error).__name__ == "CancelledError" else "internal"


def _record(
    span: Any,
    instruments: tuple[Any, Any],
    attributes: dict[str, str],
    started: float,
    error: BaseException | None,
    side: str,
) -> None:
    code = _code(error)
    if error is not None:
        span.set_attribute("rpc.meshcall.error_code", code)
        span.set_status(Status(StatusCode.ERROR, str(error)))
    labels = {**attributes, "meshcall.code": code, "meshcall.side": side}
    instruments[0].add(1, labels)
    instruments[1].record(time.perf_counter() - started, labels)


def client_interceptor(
    *, tracer_provider: Any = None, meter_provider: Any = None
) -> ClientInterceptor:
    """Spans and metrics for outgoing calls; injects the trace context."""
    tracer = trace.get_tracer(_SCOPE, tracer_provider=tracer_provider)
    instruments = _instruments(meter_provider)

    async def intercept(info: ClientCallInfo, call_next: ClientCallNext) -> Any:
        attributes = {
            "rpc.system": "meshcall",
            "rpc.service": info.service,
            "rpc.method": info.method,
        }
        with tracer.start_as_current_span(
            f"{info.service}/{info.method}",
            kind=SpanKind.CLIENT,
            attributes=attributes,
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            carrier: dict[str, str] = dict(info.options.metadata or {})
            propagate.inject(carrier)
            info.options = replace(info.options, metadata=carrier)
            started = time.perf_counter()
            try:
                result = await call_next(info)
            except BaseException as exc:
                _record(span, instruments, attributes, started, exc, "client")
                raise
            _record(span, instruments, attributes, started, None, "client")
            return result

    return intercept


def server_interceptor(
    *, tracer_provider: Any = None, meter_provider: Any = None
) -> ServerInterceptor:
    """Spans and metrics for incoming calls, continuing the caller's trace."""
    tracer = trace.get_tracer(_SCOPE, tracer_provider=tracer_provider)
    instruments = _instruments(meter_provider)

    async def intercept(
        ctx: RpcContext, request: Any, call_next: ServerCallNext
    ) -> Any:
        attributes = {
            "rpc.system": "meshcall",
            "rpc.service": ctx.service,
            "rpc.method": ctx.method,
        }
        parent = propagate.extract(_carrier(ctx.metadata))
        with tracer.start_as_current_span(
            f"{ctx.service}/{ctx.method}",
            context=parent,
            kind=SpanKind.SERVER,
            attributes=attributes,
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            started = time.perf_counter()
            try:
                result = await call_next()
            except BaseException as exc:
                _record(span, instruments, attributes, started, exc, "server")
                raise
            _record(span, instruments, attributes, started, None, "server")
            return result

    return intercept


def _carrier(metadata: Mapping[str, str]) -> dict[str, str]:
    return dict(metadata)

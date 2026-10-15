"""OpenTelemetry interceptors: one trace across client and server, metrics."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from pydantic import BaseModel

from meshcall import MeshCallError, RpcContext, RpcServer, method, service
from meshcall.client import ClientBase
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
from meshcall.otel import client_interceptor, server_interceptor


class Echo(BaseModel):
    text: str
    traceparent: str = ""
    trace_id: str = ""


@service(name="test.v1.Traced", description="")
class Traced:
    @method(description="", read_only=True)
    async def echo(self, ctx: RpcContext, text: str) -> Echo:
        span = trace.get_current_span().get_span_context()
        return Echo(
            text=text,
            traceparent=ctx.metadata.get("traceparent", ""),
            trace_id=format(span.trace_id, "032x"),
        )

    @method(description="", read_only=True)
    async def fail(self) -> Echo:
        raise MeshCallError("app.nope", "no", details={})


class Request(BaseModel):
    text: str = ""


class TracedClient(ClientBase):
    async def echo(self, text: str) -> Echo:
        return await self.call_unary("test.v1.Traced", "echo", Request(text=text), Echo)

    async def fail(self) -> Echo:
        return await self.call_unary("test.v1.Traced", "fail", Request(), Echo)


@pytest.fixture
async def traced() -> AsyncIterator[
    tuple[TracedClient, InMemorySpanExporter, InMemoryMetricReader]
]:
    spans = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(spans))
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader])
    providers = {"tracer_provider": tracer_provider, "meter_provider": meter_provider}
    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(
        services=[Traced()],
        driver=driver,
        access_log=False,
        interceptors=[server_interceptor(**providers)],
    )
    await server.start()
    client = TracedClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}"),
        interceptors=[client_interceptor(**providers)],
    )
    try:
        yield client, spans, reader
    finally:
        await client.stop()
        await server.stop()


async def test_one_trace_spans_client_and_server(
    traced: tuple[TracedClient, InMemorySpanExporter, InMemoryMetricReader],
) -> None:
    client, spans, _ = traced
    echo = await client.echo("hi")
    finished = {span.kind: span for span in spans.get_finished_spans()}
    client_span, server_span = finished[SpanKind.CLIENT], finished[SpanKind.SERVER]
    client_context, server_context = client_span.context, server_span.context
    assert client_context is not None and server_context is not None
    trace_id = format(client_context.trace_id, "032x")
    assert echo.traceparent.split("-")[1] == trace_id
    assert echo.trace_id == trace_id  # the handler runs inside the server span
    assert server_context.trace_id == client_context.trace_id
    assert server_span.parent is not None
    assert server_span.parent.span_id == client_context.span_id
    assert server_span.name == "test.v1.Traced/echo"
    assert dict(server_span.attributes or {})["rpc.system"] == "meshcall"


async def test_errors_and_metrics(
    traced: tuple[TracedClient, InMemorySpanExporter, InMemoryMetricReader],
) -> None:
    client, spans, reader = traced
    await client.echo("a")
    with pytest.raises(MeshCallError):
        await client.fail()
    failed = [s for s in spans.get_finished_spans() if s.name.endswith("/fail")]
    codes = {str(dict(s.attributes or {})["rpc.meshcall.error_code"]) for s in failed}
    assert codes == {"app.nope"}
    data = reader.get_metrics_data()
    assert data is not None
    calls: dict[tuple[str, str, str], int] = {}
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                if metric.name != "meshcall.calls":
                    continue
                for point in metric.data.data_points:
                    assert isinstance(point, NumberDataPoint)
                    labels = dict(point.attributes or {})
                    key = (
                        str(labels["rpc.method"]),
                        str(labels["meshcall.code"]),
                        str(labels["meshcall.side"]),
                    )
                    calls[key] = int(point.value)
    assert calls[("echo", "ok", "client")] == 1
    assert calls[("echo", "ok", "server")] == 1
    assert calls[("fail", "app.nope", "server")] == 1

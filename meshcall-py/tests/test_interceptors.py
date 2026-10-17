"""Interceptor chains on both sides of a call."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
from pydantic import BaseModel

from meshcall import (
    ClientCallInfo,
    ClientCallNext,
    RpcContext,
    RpcServer,
    ServerCallNext,
    method,
    service,
)
from meshcall.client import ClientBase
from meshcall.context import CallOptions
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
from meshcall.errors import ErrorCode, MeshCallError
from meshcall.streams import RpcServerStream


class Request(BaseModel):
    value: int = 0


class Item(BaseModel):
    value: int


class Result(BaseModel):
    total: int
    seen: dict[str, str] = {}


@service(name="test.v1.InterceptService")
class InterceptService:
    @method()
    async def echo(self, ctx: RpcContext, value: int = 0) -> Result:
        ctx.set_trailer("handler", "ran")
        return Result(total=value, seen=dict(ctx.metadata))

    @method.server_stream()
    async def count(self, value: int) -> AsyncIterator[Item]:
        for index in range(value):
            yield Item(value=index)


class InterceptClient(ClientBase):
    service_name = "test.v1.InterceptService"

    async def echo(self, request: Request, **kwargs: Any) -> Result:
        return await self.call_unary(
            self.service_name, "echo", request, Result, **kwargs
        )

    def count(self, value: int, **kwargs: Any) -> RpcServerStream[Item]:
        class CountRequest(BaseModel):
            value: int

        return self.call_server_stream(
            self.service_name, "count", CountRequest(value=value), Item, **kwargs
        )


async def test_server_interceptors_order_short_circuit_and_wrap_streams() -> None:
    events: list[str] = []

    async def outer(ctx: RpcContext, request: Any, call_next: ServerCallNext) -> Any:
        events.append(f"outer:{ctx.method}:{ctx.metadata.get('who', '-')}")
        ctx.set_trailer("outer", "yes")
        result = await call_next()
        events.append("outer:done")
        return result

    async def guard(ctx: RpcContext, request: Any, call_next: ServerCallNext) -> Any:
        if ctx.metadata.get("who") == "intruder":
            raise MeshCallError(ErrorCode.PERMISSION_DENIED, "not you")
        events.append("guard:pass")
        return await call_next()

    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(
        services=[InterceptService()], driver=driver, interceptors=[outer, guard]
    )
    await server.start()
    client = InterceptClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}")
    )
    try:
        result = await client.echo(Request(value=1), metadata={"who": "friend"})
        assert result.total == 1 and result.seen == {"who": "friend"}
        assert events == ["outer:echo:friend", "guard:pass", "outer:done"]

        events.clear()
        with pytest.raises(MeshCallError) as error:
            await client.echo(Request(), metadata={"who": "intruder"})
        assert error.value.code == ErrorCode.PERMISSION_DENIED
        assert events == ["outer:echo:intruder"]

        # The chain spans the whole stream: outer:done fires after all items.
        events.clear()
        stream = client.count(3)
        assert [item.value async for item in stream] == [0, 1, 2]
        assert events == ["outer:count:-", "guard:pass", "outer:done"]
        assert dict(stream.trailers) == {"outer": "yes"}
    finally:
        await client.stop()
        await server.stop()


async def test_client_interceptors_propagate_metadata_and_see_trailers() -> None:
    seen: list[tuple[str, str, dict[str, str]]] = []

    async def tracing(info: ClientCallInfo, call_next: ClientCallNext) -> Any:
        info.options = CallOptions(
            info.options.timeout,
            info.options.idle_timeout,
            {**(info.options.metadata or {}), "traceparent": "00-trace"},
        )
        result = await call_next(info)
        seen.append((info.stream, info.method, dict(info.trailers)))
        return result

    attempts = 0

    async def retry_once(info: ClientCallInfo, call_next: ClientCallNext) -> Any:
        nonlocal attempts
        attempts += 1
        try:
            return await call_next(info)
        except MeshCallError as exc:
            if exc.code == ErrorCode.UNAVAILABLE and exc.retryable and attempts == 1:
                attempts += 1
                return await call_next(info)
            raise

    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(services=[InterceptService()], driver=driver)
    await server.start()
    client = InterceptClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}"),
        interceptors=[retry_once, tracing],
    )
    try:
        result = await client.echo(Request(value=2))
        assert result.seen == {"traceparent": "00-trace"}
        assert seen == [("unary", "echo", {"handler": "ran"})]
        stream = client.count(2)
        assert [item.value async for item in stream] == [0, 1]
        assert seen[-1][0:2] == ("server_stream", "count")
        assert attempts == 2
    finally:
        await client.stop()
        await server.stop()

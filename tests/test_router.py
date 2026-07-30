from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from pydantic import BaseModel
from test_direct import (
    NumberItem,
    NumberRequest,
    NumberResult,
    NumberService,
    NumberServiceClient,
)

from meshcall import Balance, RpcServer, WebSocketRouter, method, service
from meshcall.client import ClientBase
from meshcall.drivers import WebSocketClientDriver, WebSocketRouterServerDriver
from meshcall.errors import ErrorCode, MeshCallError


class RouteRequest(BaseModel):
    delay: float = 0
    session_id: str = "default"


class RouteResult(BaseModel):
    instance: str


@service(name="test.v1.RouteService", balance=Balance.round_robin())
class RouteServiceA:
    @staticmethod
    @method()
    async def round_robin(request: RouteRequest) -> RouteResult:
        await asyncio.sleep(request.delay)
        return RouteResult(instance="a")

    @staticmethod
    @method(balance=Balance.least_inflight())
    async def least_inflight(request: RouteRequest) -> RouteResult:
        await asyncio.sleep(request.delay)
        return RouteResult(instance="a")

    @staticmethod
    @method(balance=Balance.sticky(key="request.session_id"))
    async def sticky(request: RouteRequest) -> RouteResult:
        await asyncio.sleep(request.delay)
        return RouteResult(instance="a")


@service(name="test.v1.RouteService", balance=Balance.round_robin())
class RouteServiceB:
    @staticmethod
    @method()
    async def round_robin(request: RouteRequest) -> RouteResult:
        await asyncio.sleep(request.delay)
        return RouteResult(instance="b")

    @staticmethod
    @method(balance=Balance.least_inflight())
    async def least_inflight(request: RouteRequest) -> RouteResult:
        await asyncio.sleep(request.delay)
        return RouteResult(instance="b")

    @staticmethod
    @method(balance=Balance.sticky(key="request.session_id"))
    async def sticky(request: RouteRequest) -> RouteResult:
        await asyncio.sleep(request.delay)
        return RouteResult(instance="b")


class RouteServiceClient(ClientBase):
    service_name = "test.v1.RouteService"

    async def round_robin(self, request: RouteRequest) -> RouteResult:
        return await self._unary(
            self.service_name,
            "round_robin",
            request,
            RouteResult,
        )

    async def least_inflight(self, request: RouteRequest) -> RouteResult:
        return await self._unary(
            self.service_name,
            "least_inflight",
            request,
            RouteResult,
        )

    async def sticky(self, request: RouteRequest) -> RouteResult:
        return await self._unary(
            self.service_name,
            "sticky",
            request,
            RouteResult,
        )


async def test_router_balancing_and_stream_forwarding_over_tcp() -> None:
    router = WebSocketRouter(host="127.0.0.1", port=0)
    await router.start()
    uri = f"ws://127.0.0.1:{router.bound_port}"
    server_a = RpcServer(
        services=[RouteServiceA, NumberService],
        driver=WebSocketRouterServerDriver(uri, instance_id="a"),
    )
    server_b = RpcServer(
        services=[RouteServiceB, NumberService],
        driver=WebSocketRouterServerDriver(uri, instance_id="b"),
    )
    await server_a.start()
    await server_b.start()
    route_client = RouteServiceClient(WebSocketClientDriver(uri))
    number_client = NumberServiceClient(WebSocketClientDriver(uri))
    try:
        results = [
            (await route_client.round_robin(RouteRequest())).instance
            for _ in range(4)
        ]
        assert results == ["a", "b", "a", "b"]

        first = asyncio.create_task(
            route_client.least_inflight(RouteRequest(delay=0.05))
        )
        await asyncio.sleep(0.01)
        second = await route_client.least_inflight(RouteRequest())
        assert second.instance == "b"
        assert (await first).instance == "a"

        sticky_results = {
            (
                await route_client.sticky(
                    RouteRequest(session_id="session-123")
                )
            ).instance
            for _ in range(6)
        }
        assert len(sticky_results) == 1

        stream = number_client.download(NumberRequest(value=40))
        assert [item.value async for item in stream] == list(range(40))

        async def input_items() -> AsyncIterator[NumberItem]:
            for value in range(40):
                yield NumberItem(value=value)

        upload = await number_client.upload(NumberRequest(value=10), input_items())
        assert upload == NumberResult(total=790)

        channel = number_client.duplex(NumberRequest(value=10))
        for value in range(20):
            await channel.send(NumberItem(value=value))
        await channel.close_send()
        assert [item.value async for item in channel] == [
            value * 2 for value in range(20)
        ]
        assert await channel.result() == NumberResult(total=200)

        interrupted = asyncio.create_task(
            route_client.least_inflight(RouteRequest(delay=1))
        )
        await asyncio.sleep(0.01)
        await server_a.stop()
        with pytest.raises(MeshCallError) as error:
            await interrupted
        assert error.value.code == ErrorCode.UNAVAILABLE
    finally:
        await route_client.stop()
        await number_client.stop()
        await server_a.stop()
        await server_b.stop()
        await router.stop()


async def test_router_over_unix_websocket() -> None:
    socket_path = Path("/tmp") / f"meshcall-router-{uuid.uuid4().hex}.sock"
    router = WebSocketRouter(unix_path=socket_path)
    await router.start()
    server = RpcServer(
        services=[NumberService],
        driver=WebSocketRouterServerDriver(
            unix_path=socket_path,
            instance_id="unix-1",
        ),
    )
    await server.start()
    client = NumberServiceClient(WebSocketClientDriver(unix_path=socket_path))
    try:
        assert await client.unary(NumberRequest(value=12)) == NumberResult(total=12)
    finally:
        await client.stop()
        await server.stop()
        await router.stop()
    assert not socket_path.exists()

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
from meshcall.context import ReconnectPolicy
from meshcall.drivers import WebSocketClientDriver, WebSocketRouterServerDriver
from meshcall.errors import ErrorCode, MeshCallError


class RouteRequest(BaseModel):
    delay: float = 0
    session_id: str = "default"


class RouteResult(BaseModel):
    instance: str


@service(name="test.v1.RouteService", balance=Balance.round_robin())
class RouteServiceA:
    @method()
    async def round_robin(
        self, delay: float = 0, session_id: str = "default"
    ) -> RouteResult:
        await asyncio.sleep(delay)
        return RouteResult(instance="a")

    @method(balance=Balance.least_inflight())
    async def least_inflight(
        self, delay: float = 0, session_id: str = "default"
    ) -> RouteResult:
        await asyncio.sleep(delay)
        return RouteResult(instance="a")

    @method(balance=Balance.sticky(key="request.session_id"))
    async def sticky(
        self, delay: float = 0, session_id: str = "default"
    ) -> RouteResult:
        await asyncio.sleep(delay)
        return RouteResult(instance="a")


@service(name="test.v1.RouteService", balance=Balance.round_robin())
class RouteServiceB:
    @method()
    async def round_robin(
        self, delay: float = 0, session_id: str = "default"
    ) -> RouteResult:
        await asyncio.sleep(delay)
        return RouteResult(instance="b")

    @method(balance=Balance.least_inflight())
    async def least_inflight(
        self, delay: float = 0, session_id: str = "default"
    ) -> RouteResult:
        await asyncio.sleep(delay)
        return RouteResult(instance="b")

    @method(balance=Balance.sticky(key="request.session_id"))
    async def sticky(
        self, delay: float = 0, session_id: str = "default"
    ) -> RouteResult:
        await asyncio.sleep(delay)
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
            (await route_client.round_robin(RouteRequest())).instance for _ in range(4)
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
            (await route_client.sticky(RouteRequest(session_id="session-123"))).instance
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
            10 + value * 2 for value in range(20)
        ]

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


async def test_router_server_reregisters_after_router_restart() -> None:
    router = WebSocketRouter(host="127.0.0.1", port=0)
    await router.start()
    port = router.bound_port
    uri = f"ws://127.0.0.1:{port}"
    server_driver = WebSocketRouterServerDriver(
        uri,
        instance_id="phoenix",
        reconnect=ReconnectPolicy(initial_delay=0.05, max_delay=0.2),
    )
    server = RpcServer(services=[NumberService], driver=server_driver)
    await server.start()
    client = NumberServiceClient(
        WebSocketClientDriver(uri),
        reconnect=ReconnectPolicy(initial_delay=0.05, max_delay=0.2),
    )
    try:
        assert await client.unary(NumberRequest(value=1)) == NumberResult(total=1)
        await router.stop()
        for _ in range(50):
            if not server_driver.connected:
                break
            await asyncio.sleep(0.02)
        assert not server_driver.connected
        assert server.state.value == "running"

        restarted = WebSocketRouter(host="127.0.0.1", port=port)
        await restarted.start()
        try:
            for _ in range(100):
                if server_driver.connected:
                    break
                await asyncio.sleep(0.02)
            assert server_driver.connected
            assert await client.slow(NumberRequest(value=1), timeout=5) == NumberResult(
                total=1
            )
        finally:
            await restarted.stop()
    finally:
        await client.stop()
        await server.stop()


async def test_drain_leaves_the_pool_and_finishes_in_flight_calls() -> None:
    router = WebSocketRouter(host="127.0.0.1", port=0)
    await router.start()
    uri = f"ws://127.0.0.1:{router.bound_port}"
    server_a = RpcServer(
        services=[RouteServiceA],
        driver=WebSocketRouterServerDriver(uri, instance_id="a"),
    )
    server_b = RpcServer(
        services=[RouteServiceB],
        driver=WebSocketRouterServerDriver(uri, instance_id="b"),
    )
    await server_a.start()
    await server_b.start()
    client = RouteServiceClient(WebSocketClientDriver(uri))
    try:
        assert (await client.round_robin(RouteRequest())).instance == "a"
        # least_inflight with both idle picks the lowest instance id: a.
        pending = asyncio.create_task(client.least_inflight(RouteRequest(delay=0.4)))
        await asyncio.sleep(0.05)
        stopping = asyncio.create_task(server_a.stop(drain=True, drain_timeout=5))
        await asyncio.sleep(0.05)
        assert server_a.state.value == "draining"
        # New calls skip the draining instance...
        assert [
            (await client.round_robin(RouteRequest())).instance for _ in range(3)
        ] == ["b", "b", "b"]
        # ...while the call already on it completes without error.
        assert (await pending).instance == "a"
        await asyncio.wait_for(stopping, timeout=5)
        assert server_a.state.value == "idle"
        assert (await client.round_robin(RouteRequest())).instance == "b"
    finally:
        await client.stop()
        await server_b.stop()
        await router.stop()


async def test_multiple_routers_give_failover_without_coordination() -> None:
    routers = [WebSocketRouter(host="127.0.0.1", port=0) for _ in range(2)]
    for router in routers:
        await router.start()
    ports = [router.bound_port for router in routers]
    uris = [f"ws://127.0.0.1:{port}" for port in ports]
    policy = ReconnectPolicy(initial_delay=0.05, max_delay=0.2)
    server_driver = WebSocketRouterServerDriver(
        uris, instance_id="multi", reconnect=policy
    )
    server = RpcServer(services=[NumberService], driver=server_driver)
    await server.start()
    client_driver = WebSocketClientDriver(uris)
    client = NumberServiceClient(client_driver, reconnect=policy)
    try:
        assert set(server_driver.connected_endpoints) == set(uris)
        assert await client.unary(NumberRequest(value=1)) == NumberResult(total=1)
        assert client_driver.endpoint == uris[0]
        # Lose the Router the client is on: the next call fails over to the other.
        await routers[0].stop()
        assert await client.slow(NumberRequest(value=1), timeout=5) == NumberResult(
            total=1
        )
        assert client_driver.endpoint == uris[1]
        assert server_driver.connected_endpoints == (uris[1],)
        assert server.state.value == "running"
        # Bring the first Router back: the service re-registers there too.
        restarted = WebSocketRouter(host="127.0.0.1", port=ports[0])
        await restarted.start()
        try:
            for _ in range(100):
                if len(server_driver.connected_endpoints) == 2:
                    break
                await asyncio.sleep(0.02)
            assert set(server_driver.connected_endpoints) == set(uris)
        finally:
            await restarted.stop()
    finally:
        await client.stop()
        await server.stop()
        for router in routers[1:]:
            await router.stop()

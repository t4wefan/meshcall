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
    @method.unary()
    async def round_robin(self, request: RouteRequest) -> RouteResult:
        await asyncio.sleep(request.delay)
        return RouteResult(instance="a")

    @method.unary(balance=Balance.least_inflight())
    async def least_inflight(self, request: RouteRequest) -> RouteResult:
        await asyncio.sleep(request.delay)
        return RouteResult(instance="a")

    @method.unary(balance=Balance.sticky(key="request.session_id"))
    async def sticky(self, request: RouteRequest) -> RouteResult:
        await asyncio.sleep(request.delay)
        return RouteResult(instance="a")


@service(name="test.v1.RouteService", balance=Balance.round_robin())
class RouteServiceB:
    @method.unary()
    async def round_robin(self, request: RouteRequest) -> RouteResult:
        await asyncio.sleep(request.delay)
        return RouteResult(instance="b")

    @method.unary(balance=Balance.least_inflight())
    async def least_inflight(self, request: RouteRequest) -> RouteResult:
        await asyncio.sleep(request.delay)
        return RouteResult(instance="b")

    @method.unary(balance=Balance.sticky(key="request.session_id"))
    async def sticky(self, request: RouteRequest) -> RouteResult:
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


class UpgradeRequest(BaseModel):
    value: int = 0


class IncompatibleUpgradeRequest(BaseModel):
    value: str


class UpgradeResult(BaseModel):
    instance: str


@service(name="test.v1.UpgradeService")
class UpgradeServiceA:
    @method.unary(balance=Balance.round_robin())
    async def shared(self, request: UpgradeRequest) -> UpgradeResult:
        return UpgradeResult(instance="a")

    @method.unary(balance=Balance.disabled())
    async def exclusive(self, request: UpgradeRequest) -> UpgradeResult:
        return UpgradeResult(instance="a")


@service(name="test.v1.UpgradeService")
class UpgradeServiceB:
    @method.unary(balance=Balance.random())
    async def shared(self, request: UpgradeRequest) -> UpgradeResult:
        return UpgradeResult(instance="b")

    @method.unary(balance=Balance.disabled())
    async def exclusive(self, request: UpgradeRequest) -> UpgradeResult:
        return UpgradeResult(instance="b")

    @method.unary()
    async def added(self, request: UpgradeRequest) -> UpgradeResult:
        return UpgradeResult(instance="b")


@service(name="test.v1.UpgradeService")
class IncompatibleUpgradeService:
    @method.unary()
    async def shared(
        self,
        request: IncompatibleUpgradeRequest,
    ) -> UpgradeResult:
        return UpgradeResult(instance="incompatible")


@service(name="test.v1.UpgradeService")
class UpgradeServiceReplacement:
    @method.unary(balance=Balance.disabled())
    async def exclusive(self, request: UpgradeRequest) -> UpgradeResult:
        return UpgradeResult(instance="replacement")


class UpgradeServiceClient(ClientBase):
    service_name = "test.v1.UpgradeService"

    async def shared(self) -> UpgradeResult:
        return await self._unary(
            self.service_name,
            "shared",
            UpgradeRequest(),
            UpgradeResult,
        )

    async def exclusive(self) -> UpgradeResult:
        return await self._unary(
            self.service_name,
            "exclusive",
            UpgradeRequest(),
            UpgradeResult,
        )

    async def added(self) -> UpgradeResult:
        return await self._unary(
            self.service_name,
            "added",
            UpgradeRequest(),
            UpgradeResult,
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


async def test_router_method_registry_supports_rolling_upgrades() -> None:
    router = WebSocketRouter(host="127.0.0.1", port=0)
    await router.start()
    uri = f"ws://127.0.0.1:{router.bound_port}"
    driver_a = WebSocketRouterServerDriver(uri, instance_id="upgrade-a")
    driver_b = WebSocketRouterServerDriver(uri, instance_id="upgrade-b")
    driver_bad = WebSocketRouterServerDriver(uri, instance_id="upgrade-bad")
    driver_replacement = WebSocketRouterServerDriver(
        uri,
        instance_id="upgrade-replacement",
    )
    server_a = RpcServer(services=[UpgradeServiceA], driver=driver_a)
    server_b = RpcServer(services=[UpgradeServiceB], driver=driver_b)
    server_bad = RpcServer(
        services=[IncompatibleUpgradeService],
        driver=driver_bad,
    )
    replacement = RpcServer(
        services=[UpgradeServiceReplacement],
        driver=driver_replacement,
    )
    client = UpgradeServiceClient(WebSocketClientDriver(uri))
    await server_a.start()
    await server_b.start()
    await server_bad.start()
    try:
        results_b = {result.method: result for result in driver_b.registration_results}
        assert results_b["shared"].accepted
        assert not results_b["exclusive"].accepted
        assert results_b["exclusive"].reason == "balance_disabled"
        assert results_b["added"].accepted

        bad_result = driver_bad.registration_results[0]
        assert not bad_result.accepted
        assert bad_result.reason == "schema_mismatch"

        assert [(await client.shared()).instance for _ in range(4)] == [
            "a",
            "b",
            "a",
            "b",
        ]
        assert (await client.exclusive()).instance == "a"
        assert (await client.added()).instance == "b"

        await server_b.stop()
        with pytest.raises(MeshCallError) as unavailable:
            await client.added()
        assert unavailable.value.code == ErrorCode.UNAVAILABLE

        await server_a.stop()
        await replacement.start()
        replacement_result = driver_replacement.registration_results[0]
        assert replacement_result.accepted
        assert (await client.exclusive()).instance == "replacement"
    finally:
        await client.stop()
        await replacement.stop()
        await server_bad.stop()
        await server_b.stop()
        await server_a.stop()
        await router.stop()

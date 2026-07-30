from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterable, AsyncIterator
from pathlib import Path

import pytest
from pydantic import BaseModel

from meshcall import RpcDuplex, RpcInputStream, RpcServer, method, service
from meshcall.client import ClientBase
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
from meshcall.errors import DriverStateError, ErrorCode, MeshCallError
from meshcall.streams import RpcDuplexClient, RpcServerStream


class NumberRequest(BaseModel):
    value: int


class NumberItem(BaseModel):
    value: int


class NumberResult(BaseModel):
    total: int


handler_cancelled = asyncio.Event()


@service(name="test.v1.NumberService")
class NumberService:
    @method.unary().static
    async def unary(request: NumberRequest) -> NumberResult:
        return NumberResult(total=request.value)

    @method.unary().static
    async def slow(request: NumberRequest) -> NumberResult:
        await asyncio.sleep(request.value / 1000)
        return NumberResult(total=request.value)

    @method.unary().static
    async def cancellable(request: NumberRequest) -> NumberResult:
        try:
            await asyncio.sleep(request.value)
        except asyncio.CancelledError:
            handler_cancelled.set()
            raise
        return NumberResult(total=request.value)

    @method.server_stream().static
    async def download(request: NumberRequest) -> AsyncIterator[NumberItem]:
        for value in range(request.value):
            yield NumberItem(value=value)

    @method.client_stream().static
    async def upload(
        request: NumberRequest,
        items: RpcInputStream[NumberItem],
    ) -> NumberResult:
        total = request.value
        async for item in items:
            total += item.value
        return NumberResult(total=total)

    @method.duplex().static
    async def duplex(
        request: NumberRequest,
        channel: RpcDuplex[NumberItem, NumberItem],
    ) -> NumberResult:
        total = request.value
        async for item in channel:
            total += item.value
            await channel.send(NumberItem(value=item.value * 2))
        return NumberResult(total=total)


class NumberServiceClient(ClientBase):
    service_name = "test.v1.NumberService"

    async def unary(self, request: NumberRequest) -> NumberResult:
        return await self._unary(
            self.service_name,
            "unary",
            request,
            NumberResult,
        )

    async def slow(
        self,
        request: NumberRequest,
        *,
        timeout: float | None = None,
    ) -> NumberResult:
        return await self._unary(
            self.service_name,
            "slow",
            request,
            NumberResult,
            timeout=timeout,
        )

    async def cancellable(self, request: NumberRequest) -> NumberResult:
        return await self._unary(
            self.service_name,
            "cancellable",
            request,
            NumberResult,
        )

    def download(
        self,
        request: NumberRequest,
    ) -> RpcServerStream[NumberItem]:
        return self._server_stream(
            self.service_name,
            "download",
            request,
            NumberItem,
        )

    async def upload(
        self,
        request: NumberRequest,
        items: AsyncIterable[NumberItem],
    ) -> NumberResult:
        return await self._client_stream(
            self.service_name,
            "upload",
            request,
            items,
            NumberItem,
            NumberResult,
        )

    def duplex(
        self,
        request: NumberRequest,
    ) -> RpcDuplexClient[NumberItem, NumberItem, NumberResult]:
        return self._duplex(
            self.service_name,
            "duplex",
            request,
            NumberItem,
            NumberItem,
            NumberResult,
        )


async def test_all_call_shapes_over_tcp_websocket() -> None:
    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(services=[NumberService], driver=driver)
    await server.start()
    client = NumberServiceClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}")
    )
    try:
        assert await client.unary(NumberRequest(value=7)) == NumberResult(total=7)

        with pytest.raises(MeshCallError) as deadline_error:
            await client.slow(NumberRequest(value=100), timeout=0.01)
        assert deadline_error.value.code == ErrorCode.DEADLINE_EXCEEDED

        handler_cancelled.clear()
        cancelled_call = asyncio.create_task(
            client.cancellable(NumberRequest(value=10))
        )
        await asyncio.sleep(0.01)
        cancelled_call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled_call
        await asyncio.wait_for(handler_cancelled.wait(), timeout=1)

        stream = client.download(NumberRequest(value=40))
        assert [item.value async for item in stream] == list(range(40))

        async def input_items() -> AsyncIterator[NumberItem]:
            for value in range(40):
                yield NumberItem(value=value)

        assert await client.upload(
            NumberRequest(value=10),
            input_items(),
        ) == NumberResult(total=790)

        channel = client.duplex(NumberRequest(value=10))
        await channel.send(NumberItem(value=2))
        await channel.send(NumberItem(value=3))
        await channel.close_send()
        assert [item.value async for item in channel] == [4, 6]
        assert await channel.result() == NumberResult(total=15)
    finally:
        await client.stop()
        await server.stop()


async def test_unary_over_unix_websocket() -> None:
    socket_path = Path("/tmp") / f"meshcall-{uuid.uuid4().hex}.sock"
    driver = WebSocketDirectServerDriver(unix_path=socket_path)
    server = RpcServer(services=[NumberService], driver=driver)
    await server.start()
    client = NumberServiceClient(WebSocketClientDriver(unix_path=socket_path))
    try:
        result = await client.unary(NumberRequest(value=9))
        assert result == NumberResult(total=9)
    finally:
        await client.stop()
        await server.stop()
    assert not socket_path.exists()


async def test_server_driver_is_exclusive_and_reusable() -> None:
    driver = WebSocketDirectServerDriver()
    first = RpcServer(services=[NumberService], driver=driver)
    second = RpcServer(services=[NumberService], driver=driver)

    await first.start()
    try:
        try:
            await second.start()
        except DriverStateError as exc:
            assert "already bound" in str(exc)
        else:
            raise AssertionError("A running driver must reject a second server")
    finally:
        await first.stop()

    await second.start()
    await second.stop()

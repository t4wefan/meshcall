from __future__ import annotations

import asyncio
import sys
import threading
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


# Handlers run on the server's worker thread and loop, so the test's loop
# cannot wait on an asyncio.Event they set: set() from another thread does
# not wake it (the wait then ends only at its timeout, or never).
handler_cancelled = threading.Event()
handler_started = threading.Event()


@service(name="test.v1.NumberService")
class NumberService:
    @method()
    async def unary(self, value: int) -> NumberResult:
        return NumberResult(total=value)

    @method()
    async def slow(self, value: int) -> NumberResult:
        await asyncio.sleep(value / 1000)
        return NumberResult(total=value)

    @method()
    async def cancellable(self, value: int) -> NumberResult:
        handler_started.set()
        try:
            await asyncio.sleep(value)
        except asyncio.CancelledError:
            handler_cancelled.set()
            raise
        return NumberResult(total=value)

    @method.server_stream()
    async def download(self, value: int) -> AsyncIterator[NumberItem]:
        for current in range(value):
            yield NumberItem(value=current)

    @method.client_stream()
    async def upload(
        self,
        value: int,
        items: RpcInputStream[NumberItem],
    ) -> NumberResult:
        total = value
        async for item in items:
            total += item.value
        return NumberResult(total=total)

    @method.duplex(reason="doubles each item as it arrives")
    async def duplex(
        self,
        value: int,
        channel: RpcDuplex[NumberItem, NumberItem],
    ) -> None:
        async for item in channel:
            await channel.send(NumberItem(value=value + item.value * 2))


class NumberServiceClient(ClientBase):
    service_name = "test.v1.NumberService"

    async def unary(self, request: NumberRequest) -> NumberResult:
        return await self.call_unary(
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
        return await self.call_unary(
            self.service_name,
            "slow",
            request,
            NumberResult,
            timeout=timeout,
        )

    async def cancellable(self, request: NumberRequest) -> NumberResult:
        return await self.call_unary(
            self.service_name,
            "cancellable",
            request,
            NumberResult,
        )

    def download(
        self,
        request: NumberRequest,
    ) -> RpcServerStream[NumberItem]:
        return self.call_server_stream(
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
        return await self.call_client_stream(
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
    ) -> RpcDuplexClient[NumberItem, NumberItem]:
        return self.call_duplex(
            self.service_name,
            "duplex",
            request,
            NumberItem,
            NumberItem,
        )


class ExpandedUnaryRequest(BaseModel):
    left: int
    right: int = 1


class ExpandedDownloadRequest(BaseModel):
    start: int
    stop: int


class ExpandedUploadRequest(BaseModel):
    offset: int


class ExpandedDuplexRequest(BaseModel):
    offset: int


@service(name="test.v1.ExpandedService")
class ExpandedService:
    @method()
    async def unary(self, left: int, right: int = 1) -> NumberResult:
        return NumberResult(total=left + right)

    @method.server_stream()
    async def download(self, start: int, stop: int) -> AsyncIterator[NumberItem]:
        for value in range(start, stop):
            yield NumberItem(value=value)

    @method.client_stream()
    async def upload(
        self,
        offset: int,
        items: RpcInputStream[NumberItem],
    ) -> NumberResult:
        total = offset
        async for item in items:
            total += item.value
        return NumberResult(total=total)

    @method.duplex(reason="doubles each item as it arrives")
    async def duplex(
        self,
        offset: int,
        channel: RpcDuplex[NumberItem, NumberItem],
    ) -> None:
        async for item in channel:
            await channel.send(NumberItem(value=offset + item.value * 2))


class ExpandedServiceClient(ClientBase):
    service_name = "test.v1.ExpandedService"

    async def unary(self, left: int, right: int = 1) -> NumberResult:
        return await self.call_unary(
            self.service_name,
            "unary",
            ExpandedUnaryRequest(left=left, right=right),
            NumberResult,
        )

    def download(self, start: int, stop: int) -> RpcServerStream[NumberItem]:
        return self.call_server_stream(
            self.service_name,
            "download",
            ExpandedDownloadRequest(start=start, stop=stop),
            NumberItem,
        )

    async def upload(
        self,
        offset: int,
        items: AsyncIterable[NumberItem],
    ) -> NumberResult:
        return await self.call_client_stream(
            self.service_name,
            "upload",
            ExpandedUploadRequest(offset=offset),
            items,
            NumberItem,
            NumberResult,
        )

    def duplex(
        self,
        offset: int,
    ) -> RpcDuplexClient[NumberItem, NumberItem]:
        return self.call_duplex(
            self.service_name,
            "duplex",
            ExpandedDuplexRequest(offset=offset),
            NumberItem,
            NumberItem,
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
        handler_started.clear()
        cancelled_call = asyncio.create_task(
            client.cancellable(NumberRequest(value=10))
        )
        # Cancel once the call is running: a fixed sleep can be shorter than
        # one tick of Windows' 15.6 ms timer.
        assert await asyncio.to_thread(handler_started.wait, 5)
        cancelled_call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled_call
        assert await asyncio.to_thread(handler_cancelled.wait, 1)

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
        assert [item.value async for item in channel] == [14, 16]
    finally:
        await client.stop()
        await server.stop()


@pytest.mark.skipif(
    sys.platform == "win32", reason="Unix sockets are not used on Windows"
)
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


async def test_expanded_parameters_over_all_call_shapes() -> None:
    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(services=[ExpandedService], driver=driver)
    await server.start()
    client = ExpandedServiceClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}")
    )
    try:
        assert await client.unary(7) == NumberResult(total=8)

        stream = client.download(3, 7)
        assert [item.value async for item in stream] == [3, 4, 5, 6]

        async def input_items() -> AsyncIterator[NumberItem]:
            for value in (2, 3, 5):
                yield NumberItem(value=value)

        assert await client.upload(10, input_items()) == NumberResult(total=20)

        channel = client.duplex(10)
        await channel.send(NumberItem(value=2))
        await channel.send(NumberItem(value=3))
        await channel.close_send()
        assert [item.value async for item in channel] == [14, 16]
    finally:
        await client.stop()
        await server.stop()

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator

import pytest
from pydantic import BaseModel

from meshcall import RpcInputStream, RpcServer, method, service
from meshcall.client import ClientBase
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
from meshcall.errors import ErrorCode, MeshCallError
from meshcall.streams import RpcServerStream

LIMIT = 64 * 1024


class Size(BaseModel):
    bytes: int


class Text(BaseModel):
    text: str


@service(name="test.v1.Big")
class BigService:
    """Oversized frames."""

    @method(read_only=True)
    async def result(self, bytes: int) -> Text:
        """A result of bytes bytes."""
        return Text(text="x" * bytes)

    @method(read_only=True)
    async def echo(self, text: str) -> Text:
        """Return the text."""
        return Text(text=text)

    @method.server_stream(read_only=True)
    async def items(self, bytes: int) -> AsyncIterator[Text]:
        """One small item, then one of bytes bytes."""
        yield Text(text="small")
        yield Text(text="x" * bytes)

    @method.client_stream(idempotent=True)
    async def upload(self, bytes: int, items: RpcInputStream[Text]) -> Size:
        """Count the uploaded bytes."""
        total = 0
        async for item in items:
            total += len(item.text)
        return Size(bytes=total)

    @method(read_only=True)
    async def sleep(self, bytes: int) -> Size:
        """Sleep bytes milliseconds."""
        await asyncio.sleep(bytes / 1000)
        return Size(bytes=bytes)


class BigClient(ClientBase):
    service_name = "test.v1.Big"

    async def result(self, request: Size) -> Text:
        return await self.call_unary(self.service_name, "result", request, Text)

    async def echo(self, request: Text) -> Text:
        return await self.call_unary(self.service_name, "echo", request, Text)

    def items(self, request: Size) -> RpcServerStream[Text]:
        return self.call_server_stream(self.service_name, "items", request, Text)

    async def upload(self, request: Size, items: AsyncIterable[Text]) -> Size:
        return await self.call_client_stream(
            self.service_name, "upload", request, items, Text, Size
        )

    async def sleep(self, request: Size) -> Size:
        return await self.call_unary(self.service_name, "sleep", request, Size)


def _too_large(error: MeshCallError, source: str) -> bool:
    return (
        error.code == ErrorCode.RESOURCE_EXHAUSTED
        and not error.retryable
        and (error.details or {}).get("reason") == "frame_too_large"
        and (error.details or {}).get("source") == source
    )


async def test_frames_over_the_limit_fail_only_their_call() -> None:
    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0, max_frame_size=LIMIT)
    server = RpcServer(services=[BigService], driver=driver)
    await server.start()
    client = BigClient(
        WebSocketClientDriver(
            f"ws://127.0.0.1:{driver.bound_port}", max_frame_size=LIMIT
        )
    )
    try:
        slow = asyncio.create_task(client.sleep(Size(bytes=300)))
        await asyncio.sleep(0.05)

        with pytest.raises(MeshCallError) as result:
            await asyncio.wait_for(client.result(Size(bytes=2 * LIMIT)), 5)
        assert _too_large(result.value, "server"), result.value

        stream = client.items(Size(bytes=2 * LIMIT))
        received = []
        with pytest.raises(MeshCallError) as item:
            async for text in stream:
                received.append(text.text)
        assert received == ["small"]
        assert _too_large(item.value, "server"), item.value

        with pytest.raises(MeshCallError) as request:
            await client.echo(Text(text="x" * 2 * LIMIT))
        assert _too_large(request.value, "client"), request.value

        async def upload() -> AsyncIterator[Text]:
            yield Text(text="ok")
            yield Text(text="x" * 2 * LIMIT)

        with pytest.raises(MeshCallError) as input_item:
            await asyncio.wait_for(client.upload(Size(bytes=0), upload()), 5)
        assert _too_large(input_item.value, "client"), input_item.value

        assert await slow == Size(bytes=300)
        assert await client.result(Size(bytes=10)) == Text(text="x" * 10)
    finally:
        await client.stop()
        await server.stop()

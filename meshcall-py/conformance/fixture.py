"""Reference Python fixture for the conformance suite (see conformance/FIXTURE.md)."""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import AsyncIterator
from typing import cast

from pydantic import BaseModel

from meshcall import RpcContext, RpcDuplex, RpcInputStream, RpcServer, method, service
from meshcall.drivers import WebSocketRouterServerDriver
from meshcall.errors import MeshCallError


class Echo(BaseModel):
    value: int
    text: str


class Tag(BaseModel):
    tag: str


class Tags(BaseModel):
    tags: list[str]


class Index(BaseModel):
    index: int


class Value(BaseModel):
    value: int


class Total(BaseModel):
    total: int


class Metadata(BaseModel):
    metadata: dict[str, str]


class Ok(BaseModel):
    ok: bool = True


class Instance(BaseModel):
    instance: str


class Text(BaseModel):
    text: str


@service("conformance.v1.Targeted", targeted=True)
class Targeted:
    """A targeted service: every call must name the instance it is for."""

    @method(read_only=True)
    async def whoami(self, ctx: RpcContext) -> Instance:
        """Return the id of the instance that served the call."""
        return Instance(instance=ctx.instance_id)


@service("conformance.v1.Fixture")
class Fixture:
    """Deterministic behaviours the conformance suite drives through the Router."""

    def __init__(self) -> None:
        self.cancelled: list[str] = []
        self.invoked: list[str] = []
        self.server: RpcServer | None = None

    @method(read_only=True)
    async def echo(self, value: int, text: str) -> Echo:
        """Return the input unchanged."""
        return Echo(value=value, text=text)

    @method(idempotent=True)
    async def fail(self, code: str, message: str, retryable: bool = False) -> Ok:
        """Raise an application error with the given code."""
        raise MeshCallError(code, message, retryable=retryable, details={"from": "fixture"})

    @method(read_only=True)
    async def bad_response(self) -> Ok:
        """Return a response that breaks this method's own contract."""
        return cast(Ok, {"ok": "not-a-bool"})

    @method(idempotent=True)
    async def slow(self, ms: int, tag: str = "") -> Tag:
        """Sleep ``ms`` milliseconds; record ``tag`` if cancelled meanwhile."""
        self.invoked.append(tag)
        try:
            await asyncio.sleep(ms / 1000)
        except asyncio.CancelledError:
            self.cancelled.append(tag)
            raise
        return Tag(tag=tag)

    @method(read_only=True)
    async def cancelled_tags(self) -> Tags:
        """Tags of slow calls that observed cancellation."""
        return Tags(tags=list(self.cancelled))

    @method(read_only=True)
    async def invoked_tags(self) -> Tags:
        """Tags of every slow call that started, in order."""
        return Tags(tags=list(self.invoked))

    @method.server_stream(read_only=True)
    async def count(self, n: int, gap_ms: int = 0) -> AsyncIterator[Index]:
        """Yield 0..n-1 with ``gap_ms`` between items."""
        for index in range(n):
            if gap_ms:
                await asyncio.sleep(gap_ms / 1000)
            yield Index(index=index)

    @method.client_stream(idempotent=True)
    async def sum(self, items: RpcInputStream[Value], offset: int = 0) -> Total:
        """Add every streamed value to ``offset``."""
        total = offset
        async for item in items:
            total += item.value
        return Total(total=total)

    @method(read_only=True)
    async def metadata_echo(self, ctx: RpcContext) -> Metadata:
        """Return the call metadata and set trailers describing it."""
        ctx.set_trailer("echoed", "yes")
        ctx.set_trailer("count", str(len(ctx.metadata)))
        return Metadata(metadata=dict(ctx.metadata))

    @method.duplex(reason="conformance: answers interleave with input")
    async def chat(self, channel: RpcDuplex[Text, Text], prefix: str = "") -> None:
        """Echo each input with ``prefix``; send ``done:<inputs>`` when input ends."""
        count = 0
        async for item in channel:
            count += 1
            await channel.send(Text(text=prefix + item.text))
        await channel.send(Text(text=f"done:{count}"))

    @method.duplex(reason="conformance: output ends before input")
    async def announce(self, channel: RpcDuplex[Text, Text]) -> None:
        """Send ``hello``, close the output, then read the input to its end."""
        await channel.send(Text(text="hello"))
        await channel.close_send()
        async for _ in channel:
            pass

    @method(destructive=True)
    async def drain_self(self, delay_ms: int = 0) -> Ok:
        """Drain and stop this instance after ``delay_ms``."""

        async def later() -> None:
            await asyncio.sleep(delay_ms / 1000)
            assert self.server is not None
            await self.server.stop(drain=True, drain_timeout=30)

        asyncio.get_running_loop().create_task(later())
        return Ok()


async def main() -> None:
    fixture = Fixture()
    ping = float(os.environ.get("MESHCALL_PING_INTERVAL_MS", "15000")) / 1000
    server = RpcServer(
        services=[fixture, Targeted],
        driver=WebSocketRouterServerDriver(
            os.environ["MESHCALL_ROUTER_URL"],
            instance_id=os.environ.get("MESHCALL_CONFORMANCE_INSTANCE", "fixture-1"),
            ping_interval=ping,
            ping_timeout=max(1.0, ping * 2),
        ),
        access_log=False,
    )
    fixture.server = server
    await server.start()
    print("fixture registered", file=sys.stderr, flush=True)
    await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())

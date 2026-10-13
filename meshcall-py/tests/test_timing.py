"""Protocol-level behaviour: deadlines, idle timeouts, keepalive, metadata."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
from pydantic import BaseModel
from websockets.asyncio.server import ServerConnection, serve

from meshcall import RpcContext, RpcInputStream, RpcServer, method, service
from meshcall.client import ClientBase
from meshcall.context import NO_RECONNECT, ReconnectPolicy
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
from meshcall.errors import ErrorCode, MeshCallError
from meshcall.protocol import (
    PROTOCOL_VERSION,
    CallOpenFrame,
    HelloAckFrame,
    decode_frame,
    encode_frame,
)
from meshcall.streams import RpcServerStream


class Request(BaseModel):
    value: int = 0


class Item(BaseModel):
    value: int


class Result(BaseModel):
    total: int
    echoed: dict[str, str] = {}


@service(name="test.v1.TimingService")
class TimingService:
    @method()
    async def echo_metadata(self, ctx: RpcContext, value: int = 0) -> Result:
        ctx.set_trailer("handled-by", "python")
        ctx.set_trailer("remaining-known", str(ctx.remaining() is not None))
        return Result(total=value, echoed=dict(ctx.metadata))

    @method.server_stream()
    async def trickle(self, value: int, gap_ms: int) -> AsyncIterator[Item]:
        for index in range(value):
            await asyncio.sleep(gap_ms / 1000)
            yield Item(value=index)

    @method.client_stream()
    async def collect(self, items: RpcInputStream[Item]) -> Result:
        total = 0
        async for item in items:
            total += item.value
        return Result(total=total)


class TimingClient(ClientBase):
    service_name = "test.v1.TimingService"

    async def echo_metadata(
        self,
        request: Request,
        *,
        timeout: float | None = None,
        metadata: dict[str, str] | None = None,
    ) -> Result:
        return await self._unary(
            self.service_name,
            "echo_metadata",
            request,
            Result,
            timeout=timeout,
            metadata=metadata,
        )

    def trickle(
        self,
        value: int,
        gap_ms: int,
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
    ) -> RpcServerStream[Item]:
        class TrickleRequest(BaseModel):
            value: int
            gap_ms: int

        return self._server_stream(
            self.service_name,
            "trickle",
            TrickleRequest(value=value, gap_ms=gap_ms),
            Item,
            timeout=timeout,
            idle_timeout=idle_timeout,
        )

    async def collect(
        self,
        items: AsyncIterator[Item],
        *,
        idle_timeout: float | None = None,
    ) -> Result:
        return await self._client_stream(
            self.service_name,
            "collect",
            Request(),
            items,
            Item,
            Result,
            idle_timeout=idle_timeout,
        )


async def _running(
    *, idle_timeout: float | None = None
) -> tuple[RpcServer, WebSocketDirectServerDriver]:
    driver = WebSocketDirectServerDriver(
        host="127.0.0.1", port=0, idle_timeout=idle_timeout
    )
    server = RpcServer(services=[TimingService()], driver=driver)
    await server.start()
    return server, driver


async def test_metadata_context_and_trailers_round_trip() -> None:
    server, driver = await _running()
    client = TimingClient(WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}"))
    try:
        result = await client.echo_metadata(
            Request(value=3),
            timeout=5,
            metadata={"traceparent": "00-abc", "tenant": "t1"},
        )
        assert result == Result(
            total=3, echoed={"traceparent": "00-abc", "tenant": "t1"}
        )
        with pytest.raises(ValueError, match="reserved"):
            await client.echo_metadata(Request(), metadata={"meshcall-x": "1"})
    finally:
        await client.stop()
        await server.stop()


async def test_stream_trailers_are_exposed_after_termination() -> None:
    server, driver = await _running()
    client = TimingClient(WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}"))
    try:
        stream = client.trickle(2, 1)
        assert [item.value async for item in stream] == [0, 1]
        # trickle sets no trailers; the property must still be safe to read.
        assert dict(stream.trailers) == {}
    finally:
        await client.stop()
        await server.stop()


async def test_server_idle_timeout_on_client_stream() -> None:
    server, driver = await _running()
    client = TimingClient(WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}"))

    async def stalled() -> AsyncIterator[Item]:
        yield Item(value=1)
        await asyncio.sleep(10)
        yield Item(value=2)

    try:
        with pytest.raises(MeshCallError) as error:
            await client.collect(stalled(), idle_timeout=0.05)
        assert error.value.code == ErrorCode.DEADLINE_EXCEEDED
        assert error.value.details == {"source": "server", "reason": "idle"}
    finally:
        await client.stop()
        await server.stop()


async def test_client_idle_timeout_on_server_stream() -> None:
    server, driver = await _running()
    client = TimingClient(WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}"))
    try:
        received: list[int] = []
        with pytest.raises(MeshCallError) as error:
            async for item in client.trickle(5, 200, idle_timeout=0.05):
                received.append(item.value)
        assert received == []
        assert error.value.details == {"source": "client", "reason": "idle"}
        # A stream whose gaps fit inside the idle timeout completes normally.
        assert [item.value async for item in client.trickle(3, 1, idle_timeout=1)] == [
            0,
            1,
            2,
        ]
    finally:
        await client.stop()
        await server.stop()


async def test_client_deadline_fires_when_peer_never_answers() -> None:
    """A fake endpoint that accepts the handshake and then stays silent."""
    opened: list[CallOpenFrame] = []

    async def handler(websocket: ServerConnection) -> None:
        hello = decode_frame(await websocket.recv())
        assert hello.kind == "hello"
        await websocket.send(encode_frame(HelloAckFrame(connection_id="fake")))
        async for message in websocket:
            frame = decode_frame(message)
            if isinstance(frame, CallOpenFrame):
                opened.append(frame)

    async with serve(handler, "127.0.0.1", 0) as fake:
        port = fake.sockets[0].getsockname()[1]
        client = TimingClient(
            WebSocketClientDriver(f"ws://127.0.0.1:{port}"), deadline_grace=0.05
        )
        try:
            started = asyncio.get_running_loop().time()
            with pytest.raises(MeshCallError) as error:
                await client.echo_metadata(Request(), timeout=0.1)
            elapsed = asyncio.get_running_loop().time() - started
            assert error.value.code == ErrorCode.DEADLINE_EXCEEDED
            assert error.value.details == {"source": "client"}
            assert 0.1 <= elapsed < 1
            assert opened and opened[0].timeout_ms == 100
            assert not client._calls
        finally:
            await client.stop()


async def test_server_closes_idle_connection_and_client_keepalive_prevents_it() -> None:
    server, driver = await _running(idle_timeout=1)
    silent = WebSocketClientDriver(
        f"ws://127.0.0.1:{driver.bound_port}", ping_interval=None
    )
    chatty = WebSocketClientDriver(
        f"ws://127.0.0.1:{driver.bound_port}", ping_interval=0.2, ping_timeout=0.5
    )
    silent_client = TimingClient(silent)
    chatty_client = TimingClient(chatty)
    try:
        await silent_client.start()
        await chatty_client.start()
        await asyncio.sleep(1.5)
        # The server dropped the silent peer; its receive loop has ended and
        # the client forgot the dead connection.
        assert silent_client._receive_task is None
        assert silent_client._connection is None
        # The next call reconnects transparently.
        assert await silent_client.echo_metadata(Request(value=5)) == Result(total=5)
        assert await chatty_client.echo_metadata(Request(value=1)) == Result(total=1)
    finally:
        await silent_client.stop()
        await chatty_client.stop()
        await server.stop()


async def test_client_reconnects_after_server_restart_and_honours_timeout() -> None:
    server, driver = await _running()
    port = driver.bound_port
    client = TimingClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{port}"),
        reconnect=ReconnectPolicy(initial_delay=0.05, max_delay=0.2),
    )
    try:
        assert await client.echo_metadata(Request(value=1)) == Result(total=1)
        await server.stop()
        # In-flight and immediate calls fail as retryable unavailable while the
        # server is down; the reconnect loop keeps trying within the timeout.
        with pytest.raises(MeshCallError) as error:
            await client.echo_metadata(Request(value=2), timeout=0.3)
        assert error.value.code in (ErrorCode.UNAVAILABLE, ErrorCode.DEADLINE_EXCEEDED)
        # Bring the server back on the same port: the next call succeeds.
        restarted = RpcServer(
            services=[TimingService()],
            driver=WebSocketDirectServerDriver(host="127.0.0.1", port=port),
        )
        await restarted.start()
        try:
            assert await client.echo_metadata(Request(value=3), timeout=5) == Result(
                total=3
            )
        finally:
            await restarted.stop()
    finally:
        await client.stop()


async def test_reconnect_disabled_fails_fast() -> None:
    server, driver = await _running()
    client = TimingClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}"),
        reconnect=NO_RECONNECT,
    )
    try:
        await client.echo_metadata(Request())
        await server.stop()
        with pytest.raises(MeshCallError) as error:
            await client.echo_metadata(Request(), timeout=5)
        assert error.value.code == ErrorCode.UNAVAILABLE
        assert error.value.retryable
    finally:
        await client.stop()


async def test_malformed_frame_fails_only_its_call() -> None:
    """A frame with a call_id but an invalid shape must not drop the connection."""
    server, driver = await _running()
    client = TimingClient(WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}"))
    try:
        await client.start()
        connection = client._connection
        assert connection is not None
        # Open a slow stream, then inject a bad frame for it straight onto the socket.
        stream = client.trickle(5, 500)
        await asyncio.sleep(0.05)
        (call_id,) = client._calls
        await connection._websocket.send(
            json.dumps(
                {
                    "kind": "stream.window",
                    "call_id": call_id,
                    "direction": "server",
                    "credit": -1,
                }
            )
        )
        with pytest.raises(MeshCallError) as error:
            async for _ in stream:
                pass
        assert error.value.code == ErrorCode.PROTOCOL_ERROR
        # Other calls on the same connection keep working.
        assert await client.echo_metadata(Request(value=9)) == Result(total=9)
        assert client._connection is connection
    finally:
        await client.stop()
        await server.stop()


def test_unknown_frame_fields_are_ignored_but_unknown_kinds_rejected() -> None:
    frame = decode_frame(
        json.dumps(
            {
                "kind": "call.open",
                "call_id": "x",
                "service": "s",
                "method": "m",
                "payload": {},
                "timeout_ms": 10,
                "metadata": {"k": "v"},
                "future_field": {"nested": True},
            }
        )
    )
    assert isinstance(frame, CallOpenFrame)
    assert frame.timeout_ms == 10 and frame.metadata == {"k": "v"}
    assert PROTOCOL_VERSION == "meshcall/1"
    with pytest.raises(MeshCallError):
        decode_frame(json.dumps({"kind": "call.future", "call_id": "x"}))
    with pytest.raises(MeshCallError):
        decode_frame(
            json.dumps(
                {
                    "kind": "call.open",
                    "call_id": "x",
                    "service": "s",
                    "method": "m",
                    "payload": {},
                    "timeout_ms": 0,
                }
            )
        )


async def test_direct_server_drain_finishes_streams_and_refuses_new_connections() -> (
    None
):
    server, driver = await _running()
    port = driver.bound_port
    client = TimingClient(WebSocketClientDriver(f"ws://127.0.0.1:{port}"))
    try:
        stream = client.trickle(5, 40)
        first = await anext(stream)
        assert first.value == 0
        stopping = asyncio.create_task(server.stop(drain=True, drain_timeout=5))
        await asyncio.sleep(0.05)
        assert server.state.value == "draining"
        # The listener is closed: a new connection cannot be made...
        late = TimingClient(
            WebSocketClientDriver(f"ws://127.0.0.1:{port}"), reconnect=NO_RECONNECT
        )
        with pytest.raises(MeshCallError):
            await late.echo_metadata(Request(), timeout=1)
        await late.stop()
        # ...but the stream already running completes in full.
        assert [item.value async for item in stream] == [1, 2, 3, 4]
        await asyncio.wait_for(stopping, timeout=5)
        assert server.state.value == "idle"
    finally:
        await client.stop()
        if server.state.value != "idle":
            await server.stop()


class FlakyRequest(BaseModel):
    fail_times: int = 1


class Attempts(BaseModel):
    attempts: int


@service("test.v1.FlakyService")
class FlakyService:
    """Fails with a retryable error a configurable number of times per tag."""

    def __init__(self) -> None:
        self.seen: dict[str, int] = {}

    @method()
    async def flaky(self, tag: str, fail_times: int, ctx: RpcContext) -> Attempts:
        """Fail ``fail_times`` times for ``tag`` with retryable unavailable, then succeed."""
        count = self.seen.get(tag, 0) + 1
        self.seen[tag] = count
        if count <= fail_times:
            raise MeshCallError(
                ErrorCode.UNAVAILABLE,
                "try again",
                retryable=True,
                details={"key": ctx.idempotency_key},
            )
        return Attempts(attempts=count)


class FlakyClient(ClientBase):
    service_name = "test.v1.FlakyService"

    async def flaky(self, tag: str, fail_times: int, **kwargs: Any) -> Attempts:
        class Req(BaseModel):
            tag: str
            fail_times: int

        return await self.call_unary(
            self.service_name,
            "flaky",
            Req(tag=tag, fail_times=fail_times),
            Attempts,
            **kwargs,
        )


async def test_retry_policy_retries_only_when_safe() -> None:
    from meshcall.context import RetryPolicy

    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(services=[FlakyService()], driver=driver)
    await server.start()
    client = FlakyClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}"),
        retry=RetryPolicy(max_attempts=3, initial_delay=0.01, max_delay=0.02),
    )
    try:
        # Not idempotent and no key: the first failure is final.
        with pytest.raises(MeshCallError) as error:
            await client.flaky("plain", 1)
        assert error.value.code == ErrorCode.UNAVAILABLE
        assert "attempts" not in (error.value.details or {})

        # Declared idempotent (what generated clients pass): retried.
        assert (await client.flaky("idem", 2, idempotent=True)).attempts == 3

        # A key makes a non-idempotent method retryable and reaches the handler.
        assert (await client.flaky("keyed", 1, idempotency_key="k-1")).attempts == 2

        # Exhausting attempts reports how many were made.
        with pytest.raises(MeshCallError) as error:
            await client.flaky("hopeless", 10, idempotent=True)
        assert error.value.details == {"key": None, "attempts": 3}

        # Attempts share the call's budget.
        with pytest.raises(MeshCallError) as error:
            await client.flaky("budget", 10, idempotent=True, timeout=0.015)
        assert error.value.code == ErrorCode.UNAVAILABLE
    finally:
        await client.stop()
        await server.stop()


async def test_max_inflight_is_enforced_locally_in_direct_mode() -> None:
    from meshcall.context import NO_RETRY

    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(services=[TimingService()], driver=driver, max_inflight=1)
    await server.start()
    client = TimingClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}"), retry=NO_RETRY
    )
    try:
        stream = client.trickle(3, 200)
        await anext(stream)
        with pytest.raises(MeshCallError) as error:
            await client.echo_metadata(Request())
        assert error.value.code == ErrorCode.RESOURCE_EXHAUSTED
        assert error.value.retryable
        assert error.value.details == {"source": "server", "reason": "overloaded"}
        assert [item.value async for item in stream] == [1, 2]
        assert (await client.echo_metadata(Request(value=4))).total == 4
    finally:
        await client.stop()
        await server.stop()

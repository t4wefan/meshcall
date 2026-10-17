"""Subscriptions: resumable server streams (docs/protocol.md → Subscriptions)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
from pydantic import BaseModel

from meshcall import (
    ContractError,
    MeshCallError,
    ReconnectPolicy,
    RpcServer,
    Topic,
    get_service_contract,
    method,
    service,
)
from meshcall.client import ClientBase
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
from meshcall.errors import ErrorCode
from meshcall.streams import RpcServerStream


class Tick(BaseModel):
    seq: int


class TicksRequest(BaseModel):
    since: int | None = None


@service(name="test.v1.Feed", description="Subscription fixtures.")
class Feed:
    def __init__(self) -> None:
        self.topic: Topic[Tick] = Topic(history=1000, cursor=lambda tick: tick.seq)
        self.opened: list[int | None] = []
        self.fail_every = 0

    @method.subscription(cursor="seq", resume="since", read_only=True)
    async def ticks(self, since: int | None = None) -> AsyncIterator[Tick]:
        """Every tick after ``since``."""
        self.opened.append(since)
        sent = 0
        async for tick in self.topic.subscribe(after=since):
            yield tick
            sent += 1
            if self.fail_every and sent == self.fail_every:
                raise MeshCallError(ErrorCode.UNAVAILABLE, "flap", retryable=True)

    @method.subscription(read_only=True)
    async def broken(self) -> AsyncIterator[Tick]:
        """Fails for good after one item."""
        yield Tick(seq=0)
        raise MeshCallError("app.gone", "no more", retryable=False)


class FeedClient(ClientBase):
    service_name = "test.v1.Feed"

    def ticks(self, since: int | None = None) -> RpcServerStream[Tick]:
        return self.call_subscription(
            self.service_name,
            "ticks",
            TicksRequest(since=since),
            Tick,
            cursor="seq",
            resume="since",
        )

    def broken(self) -> RpcServerStream[Tick]:
        return self.call_subscription(self.service_name, "broken", TicksRequest(), Tick)


@pytest.fixture
async def feed() -> AsyncIterator[tuple[Feed, FeedClient]]:
    instance = Feed()
    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(services=[instance], driver=driver, access_log=False)
    await server.start()
    client = FeedClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}"),
        reconnect=ReconnectPolicy(initial_delay=0.01, max_delay=0.05),
    )
    try:
        yield instance, client
    finally:
        await client.stop()
        await server.stop()


async def take(stream: RpcServerStream[Tick], n: int) -> list[int]:
    return [(await anext(stream)).seq for _ in range(n)]


async def publish_when_subscribed(instance: Feed, items: range, count: int = 1) -> None:
    while instance.topic.subscribers < count:
        await asyncio.sleep(0.005)
    for seq in items:
        instance.topic.publish(Tick(seq=seq))


def test_contract_declares_the_subscription() -> None:
    ticks = get_service_contract(Feed).methods[0]
    assert ticks.stream == "server_stream"
    assert ticks.subscription is not None
    assert (ticks.subscription.cursor, ticks.subscription.resume) == ("seq", "since")
    broken = get_service_contract(Feed).methods[1]
    assert broken.subscription is not None and broken.subscription.cursor is None


def test_subscription_rules_are_checked() -> None:
    with pytest.raises(ContractError, match="together"):
        method.subscription(cursor="seq")
    with pytest.raises(ContractError, match="not a field"):

        @service(name="test.v1.BadCursor")
        class BadCursor:
            @method.subscription(cursor="nope", resume="since")
            async def ticks(self, since: int | None = None) -> AsyncIterator[Tick]:
                """Ticks."""
                yield Tick(seq=0)

        del BadCursor
    with pytest.raises(ContractError, match="must be optional"):

        @service(name="test.v1.RequiredResume")
        class RequiredResume:
            @method.subscription(cursor="seq", resume="since")
            async def ticks(self, since: int) -> AsyncIterator[Tick]:
                """Ticks."""
                yield Tick(seq=0)

        del RequiredResume


async def test_resumes_after_a_retryable_failure(
    feed: tuple[Feed, FeedClient],
) -> None:
    instance, client = feed
    instance.fail_every = 3
    async with client.ticks() as stream:
        publisher = asyncio.create_task(publish_when_subscribed(instance, range(10)))
        assert await take(stream, 10) == list(range(10))
        await publisher
    # Reopened after items 2, 5 and 8, each time after the last cursor.
    assert instance.opened == [None, 2, 5, 8]


async def test_resumes_after_the_connection_drops(
    feed: tuple[Feed, FeedClient],
) -> None:
    instance, client = feed
    for seq in range(3):
        instance.topic.publish(Tick(seq=seq))
    async with client.ticks(since=-1) as stream:
        assert await take(stream, 3) == [0, 1, 2]
        await client.driver.close()  # every call on the connection fails
        instance.topic.publish(Tick(seq=3))
        assert await take(stream, 1) == [3]
    assert instance.opened == [-1, 2]


async def test_non_retryable_errors_end_the_subscription(
    feed: tuple[Feed, FeedClient],
) -> None:
    _, client = feed
    stream = client.broken()
    assert await take(stream, 1) == [0]
    with pytest.raises(MeshCallError) as error:
        await anext(stream)
    assert error.value.code == "app.gone"
    with pytest.raises(StopAsyncIteration):
        await anext(stream)


async def test_topic_cuts_off_slow_subscribers_and_expired_cursors() -> None:
    topic: Topic[Tick] = Topic(history=2, cursor=lambda tick: tick.seq, queue_size=2)
    slow = topic.subscribe()
    for seq in range(5):
        topic.publish(Tick(seq=seq))
    assert [(await anext(slow)).seq for _ in range(2)] == [0, 1]
    with pytest.raises(MeshCallError) as error:
        await anext(slow)
    assert error.value.code == ErrorCode.RESOURCE_EXHAUSTED and error.value.retryable
    assert topic.subscribers == 0
    # History keeps 3 and 4: after=2 replays both, after=1 is too old.
    replay = topic.subscribe(after=2)
    assert [(await anext(replay)).seq for _ in range(2)] == [3, 4]
    with pytest.raises(MeshCallError) as error:
        topic.subscribe(after=1)
    assert error.value.details == {"source": "server", "reason": "cursor_expired"}
    topic.close()
    with pytest.raises(StopAsyncIteration):
        await anext(replay)

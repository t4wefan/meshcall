"""In-process fan-out for subscriptions (docs/protocol.md → Subscriptions).

A ``Topic`` hands every published item to every current subscriber of this
process. A subscription handler typically does::

    @method.subscription(cursor="seq", resume="since")
    async def events(self, since: int | None = None) -> AsyncIterator[Event]:
        async for event in self.topic.subscribe(after=since):
            yield event

Fan-out across instances is not a Topic's job: route subscribers to the
instance that produces the events (a targeted service) or bridge an external
broker into each instance's Topic.
"""

from __future__ import annotations

import asyncio
import threading
from collections import deque
from collections.abc import AsyncIterator, Callable
from typing import Any, Generic, TypeVar

from meshcall.errors import ErrorCode, MeshCallError

T = TypeVar("T")

_CLOSED = object()


class _Subscriber(Generic[T]):
    def __init__(self, size: int, loop: asyncio.AbstractEventLoop) -> None:
        self.queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=size)
        self.loop = loop
        self.failure: MeshCallError | None = None


class Topic(Generic[T]):
    """Publish items to every subscriber in this process.

    Each subscriber has a bounded queue (``queue_size``). One that falls that
    far behind is cut off with retryable ``resource_exhausted`` (``reason:
    "slow_subscriber"``) so a slow client cannot hold memory for everyone;
    its client resubscribes after its cursor. With ``history`` and a
    ``cursor`` function, the last ``history`` items are kept so
    ``subscribe(after=c)`` first replays the buffered items whose cursor is
    greater than ``c``; a cursor older than the buffer is refused with
    non-retryable ``invalid_argument`` (``reason: "cursor_expired"``) rather
    than silently skipping items. Cursors must be ordered.

    ``publish`` may be called from any thread or event loop: Python servers
    run handlers on their own worker loop, while events usually originate in
    application code elsewhere. Each item is handed to a subscriber on the
    loop that subscriber iterates in.
    """

    def __init__(
        self,
        *,
        history: int = 0,
        cursor: Callable[[T], Any] | None = None,
        queue_size: int = 1024,
    ) -> None:
        if history < 0 or queue_size < 1:
            raise ValueError("history must be >= 0 and queue_size positive")
        if history and cursor is None:
            raise ValueError("history= needs cursor= to know what was seen")
        self._cursor = cursor
        self._history: deque[T] = deque(maxlen=history or None)
        self._keep = history
        self._evicted: Any = None  # cursor of the newest item no longer kept
        self._subscribers: set[_Subscriber[T]] = set()
        self._queue_size = queue_size
        self._closed = False
        self._lock = threading.Lock()

    @property
    def subscribers(self) -> int:
        return len(self._subscribers)

    def publish(self, item: T) -> None:
        """Hand ``item`` to every subscriber; never blocks."""
        with self._lock:
            if self._closed:
                raise RuntimeError("Topic is closed")
            if self._keep:
                if len(self._history) == self._keep:
                    assert self._cursor is not None
                    self._evicted = self._cursor(self._history[0])
                self._history.append(item)
            subscribers = tuple(self._subscribers)
        for subscriber in subscribers:
            self._hand(subscriber, item)

    def _hand(self, subscriber: _Subscriber[T], item: Any) -> None:
        try:
            current = asyncio.get_running_loop()
        except RuntimeError:
            current = None
        if current is subscriber.loop:
            self._deliver(subscriber, item)
            return
        try:
            subscriber.loop.call_soon_threadsafe(self._deliver, subscriber, item)
        except RuntimeError:  # that loop has closed: nobody is listening
            with self._lock:
                self._subscribers.discard(subscriber)

    def _deliver(self, subscriber: _Subscriber[T], item: Any) -> None:
        """Runs on the subscriber's loop."""
        if subscriber.failure is not None:
            return
        try:
            subscriber.queue.put_nowait(item)
        except asyncio.QueueFull:
            with self._lock:
                self._subscribers.discard(subscriber)
            if item is _CLOSED:
                subscriber.queue.get_nowait()
                subscriber.queue.put_nowait(_CLOSED)
                return
            subscriber.failure = MeshCallError(
                ErrorCode.RESOURCE_EXHAUSTED,
                "Subscriber fell too far behind",
                retryable=True,
                details={"source": "server", "reason": "slow_subscriber"},
            )

    def subscribe(self, after: Any = None) -> AsyncIterator[T]:
        """Items published from now on, preceded by buffered ones after ``after``.

        The subscription is registered when this is called, not when
        iteration starts, so nothing published in between is missed.
        """
        subscriber: _Subscriber[T] = _Subscriber(
            self._queue_size, asyncio.get_running_loop()
        )
        with self._lock:
            if self._closed:
                raise RuntimeError("Topic is closed")
            if after is not None and self._keep:
                assert self._cursor is not None
                if self._evicted is not None and after < self._evicted:
                    raise MeshCallError(
                        ErrorCode.INVALID_ARGUMENT,
                        "The cursor is older than this topic's history",
                        details={"source": "server", "reason": "cursor_expired"},
                    )
                for item in self._history:
                    if self._cursor(item) > after:
                        subscriber.queue.put_nowait(item)
            self._subscribers.add(subscriber)
        return self._iterate(subscriber)

    async def _iterate(self, subscriber: _Subscriber[T]) -> AsyncIterator[T]:
        try:
            while True:
                if subscriber.queue.empty() and subscriber.failure is not None:
                    raise subscriber.failure
                item = await subscriber.queue.get()
                if item is _CLOSED:
                    return
                yield item
        finally:
            with self._lock:
                self._subscribers.discard(subscriber)

    def close(self) -> None:
        """End every subscription normally; later publishes fail."""
        with self._lock:
            self._closed = True
            subscribers = tuple(self._subscribers)
            self._subscribers.clear()
        for subscriber in subscribers:
            self._hand(subscriber, _CLOSED)

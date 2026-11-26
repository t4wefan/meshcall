"""A Mesh over several independent Routers (docs/protocol.md → Several Routers).

One connection per Router, opened when first needed; each call goes to the
Router where its instance was last reached, or to the Routers in turn, moving
on only when a Router did not route the call.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Mapping,
    Sequence,
)
from dataclasses import dataclass, field
from typing import Any, Self, TypeVar

from pydantic import BaseModel

from meshcall.client import ClientBase, _retry_interceptor
from meshcall.context import (
    DEFAULT_RECONNECT,
    DEFAULT_RETRY,
    CallOptions,
    ClientCallInfo,
    ReconnectPolicy,
    RetryPolicy,
)
from meshcall.errors import (
    DeadlineExceededError,
    MeshCallError,
    ProtocolError,
    UnavailableError,
)
from meshcall.streams import RpcDuplexClient, RpcServerStream

T = TypeVar("T")

#: The Router's own services, which only one Router can answer.
ROUTER_SERVICE_PREFIX = "meshcall.router."

#: What a Router answers for a call it did not route, the answer worth
#: reporting first when none routed it: a draining Router may take the call
#: again soon, and one that serves the service but not the instance says more
#: than one that lacks the service.
_NOT_ROUTED = ("router_draining", "instance_unknown", "no_instances")


def router_did_not_route(error: BaseException) -> bool:
    """The Router answered without routing the call, so it reached no handler
    and another Router may take it."""
    return (
        isinstance(error, MeshCallError)
        and error.source == "router"
        and error.reason in _NOT_ROUTED
    )


def _rank(error: MeshCallError) -> int:
    return _NOT_ROUTED.index(error.reason or "")


@dataclass
class _State:
    connections: list[ClientBase | None]
    #: Instance id → index of the Router a call to it last succeeded on.
    located: dict[str, int] = field(default_factory=dict)
    #: Index of the Router that served the last call; tried first.
    cursor: int = 0
    ever_connected: bool = False
    closed: bool = False


def _call_kwargs(options: CallOptions) -> dict[str, Any]:
    return {
        "timeout": options.timeout,
        "idle_timeout": options.idle_timeout,
        "metadata": options.metadata,
        "idempotency_key": options.idempotency_key,
        "idempotent": options.idempotent,
        "instance": options.instance,
    }


class RouterSet:
    """A ``MeshCallRpc`` over several Routers that know nothing of each other.

    Each ``open(router)`` client must try only that Router, must not wait
    between connection attempts and must not retry calls: the set moves on to
    the next Router instead, backs off (``reconnect``) only once none could be
    reached, and retries unary calls (``retry``) across all of them.
    """

    def __init__(
        self,
        routers: Sequence[str],
        open: Callable[[str], ClientBase],
        *,
        reconnect: ReconnectPolicy = DEFAULT_RECONNECT,
        retry: RetryPolicy = DEFAULT_RETRY,
        timeout: float | None = None,
    ) -> None:
        if len(routers) < 2:
            raise ValueError("A RouterSet needs at least two Routers")
        if len(set(routers)) != len(routers):
            raise ValueError("A Router is listed twice")
        #: The Routers as listed: how sources and results name them.
        self.routers = tuple(routers)
        self._open = open
        self._reconnect = reconnect
        self._retry = retry
        self._state = _State(connections=[None] * len(routers))
        self._options = CallOptions(timeout=timeout)
        self._owner = True

    @property
    def is_connected(self) -> bool:
        return any(
            c is not None and c._root._connection is not None
            for c in self._state.connections
        )

    def at(self, router: str) -> ClientBase:
        """The client of one of the Routers, opened when first needed."""
        if router not in self.routers:
            raise ValueError(
                f"{router!r} is not one of this Mesh's Routers: {', '.join(self.routers)}"
            )
        return self._connection(self.routers.index(router))

    def with_options(
        self,
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        instance: str | None = None,
    ) -> Self:
        clone = object.__new__(type(self))
        clone.__dict__.update(self.__dict__)
        clone._owner = False
        clone._options = self._merge(
            CallOptions(
                timeout=timeout,
                idle_timeout=idle_timeout,
                metadata=metadata,
                idempotency_key=idempotency_key,
                instance=instance,
            )
        )
        return clone

    def _merge(self, extra: CallOptions) -> CallOptions:
        base = self._options
        if base.metadata and extra.metadata:
            metadata: Mapping[str, str] | None = {**base.metadata, **extra.metadata}
        else:
            metadata = extra.metadata if extra.metadata is not None else base.metadata
        return CallOptions(
            timeout=extra.timeout if extra.timeout is not None else base.timeout,
            idle_timeout=(
                extra.idle_timeout
                if extra.idle_timeout is not None
                else base.idle_timeout
            ),
            metadata=metadata,
            idempotency_key=(
                extra.idempotency_key
                if extra.idempotency_key is not None
                else base.idempotency_key
            ),
            idempotent=extra.idempotent or base.idempotent,
            instance=extra.instance if extra.instance is not None else base.instance,
        )

    async def start(self) -> None:
        """Connect to one Router, the first that answers."""

        async def connected(_: ClientBase, __: CallOptions, ___: str) -> None:
            return None

        await self.route(None, self._options, connected)

    async def stop(self) -> None:
        if not self._owner:
            return
        self._state.closed = True
        clients = [c for c in self._state.connections if c is not None]
        self._state.connections = [None] * len(self.routers)
        self._state.located.clear()
        for client in clients:
            with contextlib.suppress(Exception):
                await client.stop()

    async def close(self) -> None:
        await self.stop()

    def _prepare(self, service: str, **options: Any) -> CallOptions:
        if service.startswith(ROUTER_SERVICE_PREFIX):
            raise TypeError(
                f"{service} belongs to one Router and this Mesh has several: pick it "
                f"with mesh.at(router) ({', '.join(self.routers)})"
            )
        return self._merge(CallOptions(**options))

    async def call_unary(
        self,
        service: str,
        method: str,
        request: BaseModel,
        response_type: type[BaseModel],
        **options: Any,
    ) -> Any:
        merged = self._prepare(service, **options)

        async def routed(info: ClientCallInfo) -> Any:
            return await self.route(
                info.options.instance,
                info.options,
                lambda client, attempt, _: client.call_unary(
                    service, method, request, response_type, **_call_kwargs(attempt)
                ),
            )

        info = ClientCallInfo(service, method, "unary", merged)
        if not self._retry.enabled:
            return await routed(info)
        return await _retry_interceptor(self._retry)(info, routed)

    def call_server_stream(
        self,
        service: str,
        method: str,
        request: BaseModel,
        item_type: type[BaseModel | str | bytes],
        **options: Any,
    ) -> RpcServerStream[Any]:
        merged = self._prepare(service, **options)

        async def attempt(client: ClientBase, call: CallOptions, _: str) -> _Opened:
            stream = client.call_server_stream(
                service, method, request, item_type, **_call_kwargs(call)
            )
            return stream, await _first(stream)

        return _RoutedStream(lambda: self.route(merged.instance, merged, attempt))

    async def call_client_stream(
        self,
        service: str,
        method: str,
        request: BaseModel,
        items: AsyncIterable[Any],
        input_type: type[BaseModel | str | bytes],
        response_type: type[BaseModel],
        **options: Any,
    ) -> Any:
        merged = self._prepare(service, **options)
        replay = _ReplayableInput(items)
        try:
            return await self.route(
                merged.instance,
                merged,
                lambda client, call, _: client.call_client_stream(
                    service,
                    method,
                    request,
                    replay.attempt(),
                    input_type,
                    response_type,
                    **_call_kwargs(call),
                ),
            )
        finally:
            await replay.close()

    def call_duplex(
        self,
        service: str,
        method: str,
        request: BaseModel,
        input_type: type[BaseModel | str | bytes],
        output_type: type[BaseModel | str | bytes],
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        instance: str | None = None,
    ) -> RpcDuplexClient[Any, Any]:
        merged = self._prepare(
            service,
            timeout=timeout,
            idle_timeout=idle_timeout,
            metadata=metadata,
            instance=instance,
        )

        def start(
            probe: Callable[[RpcDuplexClient[Any, Any]], Awaitable[_Opened]],
        ) -> Awaitable[_Opened]:
            return self.route(
                merged.instance,
                merged,
                lambda client, call, _: probe(
                    client.call_duplex(
                        service,
                        method,
                        request,
                        input_type,
                        output_type,
                        timeout=call.timeout,
                        idle_timeout=call.idle_timeout,
                        metadata=call.metadata,
                        instance=call.instance,
                    )
                ),
            )

        return _RoutedDuplex(start)

    def _connection(self, index: int) -> ClientBase:
        if self._state.closed:
            raise UnavailableError("Mesh is closed", details={"source": "client"})
        client = self._state.connections[index]
        if client is None:
            client = self._open(self.routers[index])
            self._state.connections[index] = client
        return client

    def _order(self, instance: str | None) -> list[int]:
        """Where the instance was last reached, then from the last good one on."""
        count = len(self.routers)
        order = [(self._state.cursor + i) % count for i in range(count)]
        located = self._state.located.get(instance) if instance is not None else None
        if located is None:
            return order
        return [located, *(i for i in order if i != located)]

    async def route(
        self,
        instance: str | None,
        options: CallOptions,
        attempt: Callable[[ClientBase, CallOptions, str], Awaitable[T]],
    ) -> T:
        """Run ``attempt`` on each Router in turn until one routes the call.

        A Router that cannot be reached, or answers that it did not route the
        call, is skipped; any other outcome is the call's. When no Router
        routed it and one could not be reached, wait per the reconnect policy
        and go round again within the call's timeout.
        """
        options = self._merge(options)
        started = time.monotonic()

        def remaining() -> float | None:
            if options.timeout is None:
                return None
            return options.timeout - (time.monotonic() - started)

        delays = (
            self._reconnect.delays()
            if self._reconnect.enabled and self._state.ever_connected
            else iter(())
        )
        while True:
            unreachable: MeshCallError | None = None
            not_routed: MeshCallError | None = None
            for index in self._order(instance):
                left = remaining()
                if left is not None and left <= 0:
                    raise DeadlineExceededError(
                        "Call timed out", details={"source": "client"}
                    )
                client = self._connection(index)
                try:
                    await asyncio.wait_for(client.start(), left)
                    self._state.ever_connected = True
                except TimeoutError as exc:
                    raise DeadlineExceededError(
                        "Call timed out", details={"source": "client"}
                    ) from exc
                except MeshCallError as exc:
                    if isinstance(exc, DeadlineExceededError):
                        raise
                    unreachable = unreachable or exc
                    continue
                call = options if left is None else options.replace(timeout=left)
                try:
                    result = await attempt(client, call, self.routers[index])
                except MeshCallError as exc:
                    # Lost, draining or gone from there: find it again next time.
                    if (
                        instance is not None
                        and isinstance(exc, UnavailableError)
                        and self._state.located.get(instance) == index
                    ):
                        del self._state.located[instance]
                    if not router_did_not_route(exc):
                        raise
                    if not_routed is None or _rank(exc) < _rank(not_routed):
                        not_routed = exc
                    continue
                self._state.cursor = index
                if instance is not None:
                    self._state.located[instance] = index
                return result
            # Every Router answered that it did not route the call: that is the answer.
            if unreachable is None:
                assert not_routed is not None
                raise not_routed
            delay = next(delays, None)
            left = remaining()
            if (
                delay is None
                or self._state.closed
                or (left is not None and delay >= left)
            ):
                raise unreachable
            await asyncio.sleep(delay)


class _End:
    """The stream's first result was its end."""


_END = _End()

_Opened = tuple[RpcServerStream[Any], "asyncio.Future[Any]"]


async def _first(stream: RpcServerStream[Any]) -> asyncio.Future[Any]:
    """The stream's first result as a done future; the stream is cancelled if it fails."""
    future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
    try:
        future.set_result(await anext(stream))
    except StopAsyncIteration:
        future.set_result(_END)
    except BaseException:
        with contextlib.suppress(Exception):
            await stream.cancel()
        raise
    return future


class _RoutedStream(RpcServerStream[Any]):
    """A server stream opened on the first Router that routes it. Whether it
    was routed is known from its first result, which is held for the caller."""

    def __init__(self, start: Callable[[], Awaitable[_Opened]]) -> None:
        self._start = start
        self._stream: RpcServerStream[Any] | None = None
        self._first: asyncio.Future[Any] | None = None
        self._opening: asyncio.Task[None] | None = None
        self._cancelled: str | None = None

    async def _open(self) -> None:
        stream, first = await self._start()
        self._stream, self._first = stream, first
        if self._cancelled is not None:
            await stream.cancel(self._cancelled)

    def _opened(self) -> asyncio.Task[None]:
        if self._opening is None:
            self._opening = asyncio.ensure_future(self._open())
        return self._opening

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Any:
        await self._opened()
        first, self._first = self._first, None
        if first is not None:
            value = await first
            if value is _END:
                raise StopAsyncIteration
            return value
        assert self._stream is not None
        return await anext(self._stream)

    @property
    def trailers(self) -> Mapping[str, str]:
        return self._stream.trailers if self._stream is not None else {}

    async def cancel(self, reason: str = "client_closed") -> None:
        if self._stream is None:
            self._cancelled = reason
            return
        await self._stream.cancel(reason)


class _ReplayableInput:
    """The caller's items for a client stream that may be opened on more than
    one Router. A runtime pulls the next item only after sending the previous
    one, and sends nothing before the server grants credit; a Router that did
    not route the call granted none, so only the item last pulled can be
    unsent, and it is offered again to the next Router."""

    _NOTHING = object()

    def __init__(self, items: AsyncIterable[Any]) -> None:
        self._source = aiter(items)
        self._held: Any = self._NOTHING
        self._done = False

    async def attempt(self) -> AsyncIterator[Any]:
        if self._held is not self._NOTHING:
            yield self._held
        while not self._done:
            try:
                item = await anext(self._source)
            except StopAsyncIteration:
                self._done = True
                return
            self._held = item
            yield item

    async def close(self) -> None:
        aclose = getattr(self._source, "aclose", None)
        if not self._done and aclose is not None:
            await aclose()


@dataclass(eq=False)
class _PendingSend:
    item: Any
    future: asyncio.Future[None]
    #: Sending it on the current attempt failed with this.
    failed: BaseException | None = None


class _RoutedDuplex(_RoutedStream, RpcDuplexClient[Any, Any]):
    """A duplex call opened on the first Router that routes it.

    The call is routed once a send completes (the server granted input credit)
    or it produces a result; until then the caller's items are kept and, when
    a Router did not route the call, sent again to the next one. A send
    returns once its item went out on the call that was routed.
    """

    def __init__(
        self,
        start: Callable[
            [Callable[[RpcDuplexClient[Any, Any]], Awaitable[_Opened]]],
            Awaitable[_Opened],
        ],
    ) -> None:
        self._current: RpcDuplexClient[Any, Any] | None = None
        self._routed: RpcDuplexClient[Any, Any] | None = None
        self._failure: BaseException | None = None
        self._unsent: list[_PendingSend] = []
        self._sent_on: asyncio.Future[None] | None = None
        self._half_closing: asyncio.Task[None] | None = None
        #: The call whose input was half-closed: once per call.
        self._closed_on: RpcDuplexClient[Any, Any] | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        super().__init__(lambda: self._route(start))
        # Opened at once, as a duplex call is; failures surface from
        # iteration and send().
        self._opened().add_done_callback(_retrieve)

    async def _route(
        self,
        start: Callable[
            [Callable[[RpcDuplexClient[Any, Any]], Awaitable[_Opened]]],
            Awaitable[_Opened],
        ],
    ) -> _Opened:
        try:
            opened = await start(self._probe)
        except BaseException as exc:
            self._failure = exc
            for pending in list(self._unsent):
                self._settle(pending, exc)
            raise
        self._routed = opened[0]  # type: ignore[assignment]
        for pending in list(self._unsent):
            if pending.failed is not None:
                self._settle(pending, pending.failed)
        return opened

    async def _probe(self, call: RpcDuplexClient[Any, Any]) -> _Opened:
        """Open the call on one Router: send what is kept, and wait until it
        is routed or fails."""
        self._current = call
        for pending in self._unsent:
            self._forward(call, pending)
        if self._half_closing is not None and not self._unsent:
            self._spawn(self._close_on(call))
        first: asyncio.Future[Any] = asyncio.ensure_future(_next_or_end(call))
        sent: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._sent_on = sent
        try:
            await asyncio.wait({first, sent}, return_when=asyncio.FIRST_COMPLETED)
        except BaseException:
            first.cancel()
            raise
        if first.done() and first.exception() is not None:
            error = first.exception()
            with contextlib.suppress(Exception):
                await call.cancel()
            assert error is not None
            raise error
        first.add_done_callback(_retrieve)
        return call, first

    def _spawn(self, coroutine: Awaitable[Any]) -> None:
        task = asyncio.ensure_future(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        task.add_done_callback(_retrieve)

    def _forward(self, call: RpcDuplexClient[Any, Any], pending: _PendingSend) -> None:
        pending.failed = None

        def done(task: asyncio.Task[None]) -> None:
            if self._current is not call:
                return
            error = None if task.cancelled() else task.exception()
            if task.cancelled():
                error = asyncio.CancelledError()
            if error is None:
                self._settle(pending)
                # Credit was granted: the call is routed.
                if self._sent_on is not None and not self._sent_on.done():
                    self._sent_on.set_result(None)
            elif self._routed is call:
                self._settle(pending, error)
            else:
                pending.failed = error  # decided once the attempt is

        task = asyncio.ensure_future(call.send(pending.item))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        task.add_done_callback(done)

    def _settle(
        self, pending: _PendingSend, error: BaseException | None = None
    ) -> None:
        if pending not in self._unsent:
            return
        self._unsent.remove(pending)
        if pending.future.done():
            return
        if error is None:
            pending.future.set_result(None)
        else:
            pending.future.set_exception(error)

    async def send(self, item: Any) -> None:
        if self._half_closing is not None:
            raise ProtocolError("Input stream is closed")
        if self._routed is not None:
            await self._routed.send(item)
            return
        if self._failure is not None:
            raise self._failure
        pending = _PendingSend(item, asyncio.get_running_loop().create_future())
        self._unsent.append(pending)
        if self._current is not None:
            self._forward(self._current, pending)
        await pending.future

    async def close_send(self) -> None:
        """Half-close after the items sent before it: they settle once the call is routed."""
        if self._half_closing is None:
            self._half_closing = asyncio.ensure_future(self._close_send())
        await self._half_closing

    async def _close_send(self) -> None:
        await asyncio.gather(
            *(pending.future for pending in self._unsent), return_exceptions=True
        )
        # Not routed yet: the server may be waiting for the end of the input.
        if self._routed is None and self._current is not None:
            self._spawn(self._close_on(self._current))
        with contextlib.suppress(Exception):
            await self._opened()
        if self._routed is not None:
            await self._close_on(self._routed)

    async def _close_on(self, call: RpcDuplexClient[Any, Any]) -> None:
        if self._closed_on is call:
            return
        self._closed_on = call
        await call.close_send()


async def _next_or_end(stream: RpcServerStream[Any]) -> Any:
    try:
        return await anext(stream)
    except StopAsyncIteration:
        return _END


def _retrieve(future: asyncio.Future[Any]) -> None:
    if not future.cancelled():
        future.exception()

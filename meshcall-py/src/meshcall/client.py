from __future__ import annotations

import asyncio
import contextlib
import copy
import inspect
import time
import uuid
from collections.abc import AsyncIterable, Generator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Self

from pydantic import BaseModel, TypeAdapter, ValidationError

from meshcall.context import (
    DEFAULT_RECONNECT,
    DEFAULT_RETRY,
    CallOptions,
    ClientCallInfo,
    ClientCallNext,
    ClientInterceptor,
    ReconnectPolicy,
    RetryPolicy,
    client_deadline_grace,
)
from meshcall.driver import ClientDriver
from meshcall.errors import (
    CallCancelledError,
    DeadlineExceededError,
    MeshCallError,
    ProtocolError,
    UnavailableError,
    violations_of,
)
from meshcall.flow import CreditWindow
from meshcall.protocol import (
    CallCancelFrame,
    CallErrorFrame,
    CallOpenFrame,
    CallResultFrame,
    Frame,
    StreamEndFrame,
    StreamItemFrame,
    StreamWindowFrame,
)
from meshcall.server import INITIAL_STREAM_CREDIT
from meshcall.streams import RpcDuplexClient, RpcServerStream
from meshcall.transport import (
    FrameConnection,
    is_frame_too_large,
)

if TYPE_CHECKING:
    from meshcall.router import Mesh

_STREAM_END = object()


class ClientBase:
    """Runtime base class for low-level and manual service clients.

    ``deadline_grace`` is how long past a call's ``timeout`` the client keeps
    waiting for the peer's own ``deadline_exceeded`` before failing the call
    locally; ``None`` uses ``max(1s, 10% of timeout)``.

    ``reconnect`` controls how a lost connection is re-established. Calls in
    flight when the connection drops fail with retryable ``unavailable``; the
    next call reconnects lazily with backoff, waiting at most its own
    ``timeout``.
    """

    def __init__(
        self,
        driver: ClientDriver | Mesh | ClientBase,
        *,
        deadline_grace: float | None = None,
        reconnect: ReconnectPolicy = DEFAULT_RECONNECT,
        interceptors: Sequence[ClientInterceptor] = (),
        retry: RetryPolicy = DEFAULT_RETRY,
    ) -> None:
        self._call_options = CallOptions()
        if isinstance(driver, ClientBase):
            self._root: ClientBase = driver._root
            self._owns_root = False
            self.driver: ClientDriver = self._root.driver
            self.deadline_grace = (
                deadline_grace
                if deadline_grace is not None
                else self._root.deadline_grace
            )
            self.reconnect = self._root.reconnect
            self.retry = self._root.retry
            self.interceptors = self._root.interceptors
        elif hasattr(driver, "_ensure_client"):
            peer: ClientBase = driver._ensure_client()  # type: ignore[union-attr]
            self._root = peer._root
            self._owns_root = False
            self._call_options = peer._call_options
            self.driver = self._root.driver
            self.deadline_grace = (
                deadline_grace
                if deadline_grace is not None
                else self._root.deadline_grace
            )
            self.reconnect = self._root.reconnect
            self.retry = self._root.retry
            self.interceptors = self._root.interceptors
        else:
            self._root = self
            self._owns_root = True
            self.driver = driver  # type: ignore[assignment]
            self.deadline_grace = deadline_grace
            self.reconnect = reconnect
            self.retry = retry
            self.interceptors = tuple(interceptors)
        self._connection: FrameConnection | None = None
        self._calls: dict[str, _ClientCall] = {}
        self._state_lock = asyncio.Lock()
        self._receive_task: asyncio.Task[None] | None = None
        self._ever_connected = False

    def with_options(
        self,
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        instance: str | None = None,
    ) -> Self:
        """Return a shallow clone with per-call options applied."""
        clone = copy.copy(self)
        clone._root = self._root
        clone._owns_root = False
        clone._call_options = self._resolve_options(
            timeout=timeout,
            idle_timeout=idle_timeout,
            metadata=metadata,
            idempotency_key=idempotency_key,
            instance=instance,
        )
        return clone

    def _resolve_options(
        self,
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        idempotent: bool = False,
        instance: str | None = None,
    ) -> CallOptions:
        base = self._call_options
        merged_metadata: Mapping[str, str] | None
        if base.metadata and metadata:
            merged_metadata = {**base.metadata, **metadata}
        elif metadata is not None:
            merged_metadata = metadata
        else:
            merged_metadata = base.metadata
        return CallOptions(
            timeout=timeout if timeout is not None else base.timeout,
            idle_timeout=(
                idle_timeout if idle_timeout is not None else base.idle_timeout
            ),
            metadata=merged_metadata,
            idempotency_key=(
                idempotency_key if idempotency_key is not None else base.idempotency_key
            ),
            idempotent=idempotent or base.idempotent,
            instance=instance if instance is not None else base.instance,
        )

    async def start(self) -> None:
        """Connect if not connected, retrying with backoff after a loss."""
        if self._root is not self:
            await self._root.start()
            return
        async with self._state_lock:
            if self._connection is not None:
                return
            delays = (
                self.reconnect.delays()
                if self.reconnect.enabled and self._ever_connected
                else iter(())
            )
            while True:
                try:
                    self._connection = await self.driver.connect()
                    break
                except (OSError, ConnectionError, MeshCallError) as exc:
                    delay = next(delays, None)
                    if delay is None:
                        if isinstance(exc, MeshCallError):
                            raise
                        raise UnavailableError(
                            f"Could not connect: {exc}",
                            retryable=True,
                        ) from exc
                    await self.driver.close()
                    await asyncio.sleep(delay)
            self._ever_connected = True
            self._receive_task = asyncio.create_task(
                self._receive_loop(self._connection),
                name="meshcall-client-receiver",
            )

    async def stop(self) -> None:
        if self._root is not self:
            if self._owns_root:
                await self._root.stop()
            return
        async with self._state_lock:
            receive_task, self._receive_task = self._receive_task, None
            self._connection = None
            await self.driver.close()
            if receive_task is not None:
                receive_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await receive_task
            calls = tuple(self._calls.values())
            self._calls.clear()
            for call in calls:
                await call.fail(UnavailableError("Client is stopped"))

    async def close(self) -> None:
        """Close the client connection (alias for stop)."""
        await self.stop()

    def __await__(self) -> Generator[Any, None, Self]:
        async def _connect() -> Self:
            await self.start()
            return self

        return _connect().__await__()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.stop()

    async def call_unary(
        self,
        service: str,
        method: str,
        request: BaseModel,
        response_type: type[BaseModel],
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        idempotent: bool = False,
        instance: str | None = None,
    ) -> Any:
        root = self._root
        info = ClientCallInfo(
            service,
            method,
            "unary",
            self._resolve_options(
                timeout=timeout,
                idle_timeout=idle_timeout,
                metadata=metadata,
                idempotency_key=idempotency_key,
                idempotent=idempotent,
                instance=instance,
            ),
        )

        async def innermost(info: ClientCallInfo) -> Any:
            call = await root._open_call(
                info.service,
                info.method,
                request,
                response_type=response_type,
                options=info.options,
            )
            try:
                result = await call.result()
            except asyncio.CancelledError:
                await asyncio.shield(call.cancel("caller_cancelled"))
                raise
            finally:
                info.trailers = call.trailers
            return result

        return await root._run_chain(info, innermost)

    def call_server_stream(
        self,
        service: str,
        method: str,
        request: BaseModel,
        item_type: type[BaseModel | str | bytes],
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        idempotent: bool = False,
        instance: str | None = None,
    ) -> RpcServerStream[Any]:
        root = self._root
        info = ClientCallInfo(
            service,
            method,
            "server_stream",
            self._resolve_options(
                timeout=timeout,
                idle_timeout=idle_timeout,
                metadata=metadata,
                idempotency_key=idempotency_key,
                idempotent=idempotent,
                instance=instance,
            ),
        )

        async def innermost(info: ClientCallInfo) -> _ClientServerStream:
            return _ClientServerStream(
                _observed(
                    asyncio.create_task(
                        root._open_call(
                            info.service,
                            info.method,
                            request,
                            output_type=item_type,
                            options=info.options,
                        )
                    )
                )
            )

        async def open() -> _ClientCall:
            stream = await root._run_chain(info, innermost)
            return await _stream_open_task(stream)

        return _ClientServerStream(_observed(asyncio.create_task(open())))

    async def call_client_stream(
        self,
        service: str,
        method: str,
        request: BaseModel,
        items: AsyncIterable[Any],
        input_type: type[BaseModel | str | bytes],
        response_type: type[BaseModel],
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        idempotent: bool = False,
        instance: str | None = None,
    ) -> Any:
        root = self._root
        info = ClientCallInfo(
            service,
            method,
            "client_stream",
            self._resolve_options(
                timeout=timeout,
                idle_timeout=idle_timeout,
                metadata=metadata,
                idempotency_key=idempotency_key,
                idempotent=idempotent,
                instance=instance,
            ),
        )

        async def innermost(info: ClientCallInfo) -> Any:
            call = await root._open_call(
                info.service,
                info.method,
                request,
                response_type=response_type,
                input_type=input_type,
                options=info.options,
            )
            pump = asyncio.create_task(
                root._pump_input(call, items),
                name=f"meshcall-input-{call.call_id}",
            )
            try:
                result = await call.result()
            except asyncio.CancelledError:
                await asyncio.shield(call.cancel("caller_cancelled"))
                raise
            finally:
                info.trailers = call.trailers
                if not pump.done():
                    pump.cancel()
                with contextlib.suppress(asyncio.CancelledError, MeshCallError):
                    await pump
            return result

        return await root._run_chain(info, innermost)

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
        """Open a duplex call. It is never retried and takes no idempotency key."""
        root = self._root
        resolved = self._resolve_options(
            timeout=timeout,
            idle_timeout=idle_timeout,
            metadata=metadata,
            instance=instance,
        )
        info = ClientCallInfo(
            service,
            method,
            "duplex",
            CallOptions(
                resolved.timeout,
                resolved.idle_timeout,
                resolved.metadata,
                None,
                False,
                resolved.instance,
            ),
        )

        async def innermost(info: ClientCallInfo) -> _ClientDuplex:
            return _ClientDuplex(
                _observed(
                    asyncio.create_task(
                        root._open_call(
                            info.service,
                            info.method,
                            request,
                            input_type=input_type,
                            output_type=output_type,
                            options=info.options,
                        )
                    )
                )
            )

        async def open() -> _ClientCall:
            stream = await root._run_chain(info, innermost)
            return await _stream_open_task(stream)

        return _ClientDuplex(_observed(asyncio.create_task(open())))

    def _run_chain(self, info: ClientCallInfo, innermost: ClientCallNext) -> Any:
        """Invoke the interceptor chain; the first interceptor is outermost.

        The built-in retry policy wraps everything so user interceptors see
        each attempt."""
        call_next = innermost
        for interceptor in reversed(self.interceptors):
            call_next = _bind_client_interceptor(interceptor, call_next)
        if self.retry.enabled and info.stream == "unary":
            call_next = _bind_client_interceptor(
                _retry_interceptor(self.retry), call_next
            )
        return call_next(info)

    async def _open_call(
        self,
        service: str,
        method: str,
        request: BaseModel,
        *,
        response_type: type[BaseModel] | None = None,
        input_type: type[BaseModel | str | bytes] | None = None,
        output_type: type[BaseModel | str | bytes] | None = None,
        options: CallOptions,
    ) -> _ClientCall:
        # The local budget covers connecting and the handshake as well as the
        # call, and runs slightly longer than timeout so the peer's own
        # deadline_exceeded normally wins.
        budget: float | None = None
        if options.timeout is not None:
            grace = (
                self.deadline_grace
                if self.deadline_grace is not None
                else client_deadline_grace(options.timeout)
            )
            budget = options.timeout + grace
        started = time.monotonic()
        try:
            async with asyncio.timeout(budget):
                await self.start()
        except TimeoutError as exc:
            raise _local_deadline_error() from exc
        connection = self._connection
        if connection is None:
            raise UnavailableError("Client is not connected")
        call_id = uuid.uuid4().hex
        call = _ClientCall(
            client=self,
            connection=connection,
            call_id=call_id,
            response_type=response_type,
            input_type=input_type,
            output_type=output_type,
            idle_timeout=options.idle_timeout,
        )
        self._calls[call_id] = call
        if budget is not None:
            call.arm_deadline(max(0.0, budget - (time.monotonic() - started)))
        try:
            await connection.send(
                CallOpenFrame(
                    call_id=call_id,
                    service=service,
                    method=method,
                    payload=request.model_dump(
                        mode="json", by_alias=True, exclude_unset=True
                    ),
                    timeout_ms=(
                        max(1, round(options.timeout * 1000))
                        if options.timeout is not None
                        else None
                    ),
                    idle_timeout_ms=(
                        max(1, round(options.idle_timeout * 1000))
                        if options.idle_timeout is not None
                        else None
                    ),
                    metadata=dict(options.metadata) if options.metadata else None,
                    idempotency_key=options.idempotency_key,
                    instance_id=options.instance,
                )
            )
            if output_type is not None:
                call.output_allowance = INITIAL_STREAM_CREDIT
                await connection.send(
                    StreamWindowFrame(
                        call_id=call_id,
                        direction="server",
                        credit=INITIAL_STREAM_CREDIT,
                    )
                )
                call.arm_idle()
        except ConnectionError as exc:
            call.disarm_timers()
            self._calls.pop(call_id, None)
            raise UnavailableError("Connection was lost", retryable=True) from exc
        except BaseException:
            call.disarm_timers()
            self._calls.pop(call_id, None)
            raise
        return call

    async def _pump_input(
        self,
        call: _ClientCall,
        items: AsyncIterable[Any],
    ) -> None:
        try:
            async for item in items:
                await call.send(item)
            await call.close_send()
        except asyncio.CancelledError:
            raise
        except Exception:
            await call.cancel("input_producer_failed")
            raise

    async def _receive_loop(self, connection: FrameConnection) -> None:
        failure: MeshCallError | None = None
        try:
            while True:
                try:
                    frame = await connection.receive()
                except ProtocolError as exc:
                    # A malformed frame that still names a call fails only that
                    # call; one we cannot attribute poisons the connection.
                    call = (
                        self._calls.pop(exc.call_id, None)
                        if exc.call_id is not None
                        else None
                    )
                    if call is None:
                        raise
                    await call.fail(exc)
                    continue
                await self._dispatch(frame)
        except asyncio.CancelledError:
            raise
        except ConnectionError:
            failure = UnavailableError(
                "Connection was lost",
                retryable=True,
            )
        except Exception as exc:  # noqa: BLE001
            failure = exc if isinstance(exc, MeshCallError) else ProtocolError(str(exc))
        finally:
            if failure is not None:
                # Forget the dead connection so the next call reconnects.
                if self._connection is connection:
                    self._connection = None
                    self._receive_task = None
                    with contextlib.suppress(Exception):
                        await self.driver.close()
                calls = tuple(self._calls.values())
                self._calls.clear()
                for call in calls:
                    await call.fail(failure)

    async def _dispatch(self, frame: Frame) -> None:
        call_id = getattr(frame, "call_id", None)
        if not isinstance(call_id, str):
            return
        call = self._calls.get(call_id)
        if call is None:
            return
        if isinstance(frame, StreamWindowFrame):
            await call.receive_window(frame)
        elif isinstance(frame, StreamItemFrame):
            await call.receive_item(frame)
        elif isinstance(frame, StreamEndFrame):
            await call.receive_end(frame)
        elif isinstance(frame, CallResultFrame):
            await call.receive_result(frame)
            self._calls.pop(call.call_id, None)
        elif isinstance(frame, CallErrorFrame):
            await call.receive_error(frame)
            self._calls.pop(call.call_id, None)


def _retry_interceptor(policy: RetryPolicy) -> ClientInterceptor:
    async def retry(info: ClientCallInfo, call_next: ClientCallNext) -> Any:
        options = info.options
        may_retry = options.idempotent or options.idempotency_key is not None
        deadline = (
            time.monotonic() + options.timeout if options.timeout is not None else None
        )
        attempt = 1
        while True:
            try:
                return await call_next(info)
            except MeshCallError as exc:
                if (
                    not may_retry
                    or not exc.retryable
                    or exc.code not in policy.retry_codes
                    or attempt >= policy.max_attempts
                ):
                    if attempt > 1:
                        exc.details = {**(exc.details or {}), "attempts": attempt}
                    raise
                delay = policy.delay(attempt)
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= delay:
                        exc.details = {**(exc.details or {}), "attempts": attempt}
                        raise
                    # The next attempt gets whatever budget is left.
                    info.options = options.replace(timeout=remaining - delay)
                await asyncio.sleep(delay)
                attempt += 1

    return retry


def _bind_client_interceptor(
    interceptor: ClientInterceptor, call_next: ClientCallNext
) -> ClientCallNext:
    async def bound(info: ClientCallInfo) -> Any:
        return await interceptor(info, call_next)

    return bound


async def _stream_open_task(stream: Any) -> _ClientCall:
    """Resolve the ``_ClientCall`` behind whatever an interceptor returned."""
    if not isinstance(stream, (_ClientServerStream, _ClientDuplex)):
        raise TypeError(
            "Client interceptors must return the stream produced by call_next"
        )
    return await stream._open_task


def _observed(task: asyncio.Task[_ClientCall]) -> asyncio.Task[_ClientCall]:
    """Mark a lazily consumed open task's failure as retrieved.

    The caller sees the error when it first iterates; if it never does, this
    stops asyncio from reporting the exception as never retrieved.
    """

    def retrieve(done: asyncio.Task[_ClientCall]) -> None:
        if not done.cancelled():
            done.exception()

    task.add_done_callback(retrieve)
    return task


def _local_deadline_error(reason: str | None = None) -> MeshCallError:
    details: dict[str, Any] = {"source": "client"}
    if reason is not None:
        details["reason"] = reason
    return DeadlineExceededError(
        "Call deadline exceeded" if reason is None else "Output stream idle timeout",
        details=details,
    )


class _ClientCall:
    def __init__(
        self,
        *,
        client: ClientBase,
        connection: FrameConnection,
        call_id: str,
        response_type: type[BaseModel] | None,
        input_type: type[BaseModel | str | bytes] | None,
        output_type: type[BaseModel | str | bytes] | None,
        idle_timeout: float | None = None,
    ) -> None:
        self.client = client
        self.connection = connection
        self.call_id = call_id
        self.response_type = response_type
        self.input_type = input_type
        self.output_type = output_type
        self.idle_timeout = idle_timeout
        self.send_credit = CreditWindow()
        self.loop = asyncio.get_running_loop()
        self.result_future: asyncio.Future[Any] = self.loop.create_future()
        self.output_queue: asyncio.Queue[Any] = asyncio.Queue()
        self.input_sequence = 0
        self.output_sequence = 0
        self.output_allowance = 0
        self.input_closed = False
        self.output_closed = False
        self.terminal = False
        self.trailers: Mapping[str, str] = {}
        self._deadline_handle: asyncio.TimerHandle | None = None
        self._idle_handle: asyncio.TimerHandle | None = None

    # -- local timers -------------------------------------------------------

    def arm_deadline(self, delay: float) -> None:
        self._deadline_handle = self.loop.call_later(delay, self._on_deadline)

    def arm_idle(self) -> None:
        """(Re)start the gap timer for the server's output stream."""
        if self._idle_handle is not None:
            self._idle_handle.cancel()
            self._idle_handle = None
        if self.idle_timeout is None or self.output_closed or self.terminal:
            return
        self._idle_handle = self.loop.call_later(self.idle_timeout, self._on_idle)

    def disarm_timers(self) -> None:
        for attribute in ("_deadline_handle", "_idle_handle"):
            handle: asyncio.TimerHandle | None = getattr(self, attribute)
            if handle is not None:
                handle.cancel()
                setattr(self, attribute, None)

    def _on_deadline(self) -> None:
        self._deadline_handle = None
        self.loop.create_task(self._fail_locally(_local_deadline_error()))

    def _on_idle(self) -> None:
        self._idle_handle = None
        self.loop.create_task(self._fail_locally(_local_deadline_error("idle")))

    async def _fail_locally(self, error: MeshCallError) -> None:
        if self.terminal:
            return
        # Late frames for this call are dropped once it leaves the registry.
        self.client._calls.pop(self.call_id, None)
        await self.fail(error)
        with contextlib.suppress(Exception):
            await self.connection.send(
                CallCancelFrame(call_id=self.call_id, reason=str(error.code))
            )

    # -- call surface --------------------------------------------------------

    async def send(self, item: Any) -> None:
        if self.input_closed:
            raise ProtocolError("Input stream is closed")
        await self.send_credit.acquire()
        if self.terminal:
            raise CallCancelledError("Call is closed")
        if self.input_type is not None and self.input_type is not BaseModel:
            if inspect.isclass(self.input_type) and issubclass(
                self.input_type, BaseModel
            ):
                validated = self.input_type.model_validate(item)
                payload = validated.model_dump(mode="json", by_alias=True)
            else:
                adapter = TypeAdapter(self.input_type)
                validated = adapter.validate_python(item)
                payload = adapter.dump_python(validated, mode="json", by_alias=True)
        elif isinstance(item, BaseModel):
            payload = item.model_dump(mode="json", by_alias=True)
        else:
            payload = item
        try:
            await self.connection.send(
                StreamItemFrame(
                    call_id=self.call_id,
                    direction="client",
                    sequence=self.input_sequence,
                    payload=payload,
                )
            )
        except MeshCallError as exc:
            if is_frame_too_large(exc):
                # The call is open at the peer: end it there too.
                await self._fail_locally(exc)
            raise
        self.input_sequence += 1

    async def close_send(self) -> None:
        if not self.input_closed:
            self.input_closed = True
            await self.connection.send(
                StreamEndFrame(call_id=self.call_id, direction="client")
            )

    async def next_output(self) -> Any:
        value = await self.output_queue.get()
        if value is _STREAM_END:
            raise StopAsyncIteration
        if isinstance(value, BaseException):
            raise value
        if not self.terminal:
            self.output_allowance += 1
            try:
                await self.connection.send(
                    StreamWindowFrame(
                        call_id=self.call_id,
                        direction="server",
                        credit=1,
                    )
                )
            except ConnectionError as exc:
                error = UnavailableError("Connection was lost", retryable=True)
                await self.fail(error)
                raise error from exc
        return value

    async def result(self) -> Any:
        return await self.result_future

    async def cancel(self, reason: str) -> None:
        if not self.terminal:
            await self.connection.send(
                CallCancelFrame(call_id=self.call_id, reason=reason)
            )

    async def receive_window(self, frame: StreamWindowFrame) -> None:
        if frame.direction != "client" or self.input_type is None:
            await self.fail(ProtocolError("Invalid input stream window"))
            return
        await self.send_credit.grant(frame.credit)

    async def receive_item(self, frame: StreamItemFrame) -> None:
        if (
            frame.direction != "server"
            or self.output_type is None
            or self.output_closed
        ):
            await self.fail(ProtocolError("Invalid output stream item"))
            return
        if self.output_allowance <= 0:
            await self.fail(ProtocolError("Output stream exceeded granted credit"))
            return
        if frame.sequence != self.output_sequence:
            await self.fail(
                ProtocolError(
                    f"Expected server sequence {self.output_sequence}, "
                    f"got {frame.sequence}"
                )
            )
            return
        self.output_allowance -= 1
        self.output_sequence += 1
        self.arm_idle()
        try:
            if inspect.isclass(self.output_type) and issubclass(
                self.output_type, BaseModel
            ):
                item = self.output_type.model_validate(frame.payload)
            else:
                item = TypeAdapter(self.output_type).validate_python(frame.payload)
        except ValidationError as exc:
            await self.fail(
                ProtocolError(
                    "Invalid output stream item",
                    details={
                        "source": "client",
                        "reason": "invalid_output_item",
                        "violations": [v.as_dict() for v in violations_of(exc)],
                    },
                )
            )
            return
        self.output_queue.put_nowait(item)

    async def receive_end(self, frame: StreamEndFrame) -> None:
        if frame.direction != "server" or self.output_type is None:
            await self.fail(ProtocolError("Invalid output stream end"))
            return
        if not self.output_closed:
            self.output_closed = True
            if self._idle_handle is not None:
                self._idle_handle.cancel()
                self._idle_handle = None
            # The consumer's iteration ends when the terminal frame (with
            # trailers, or an error raised from the iteration) arrives.

    async def receive_result(self, frame: CallResultFrame) -> None:
        if self.terminal:
            return
        self.terminal = True
        self.disarm_timers()
        self.trailers = dict(frame.metadata or {})
        try:
            if self.response_type is None:
                result = None
            elif inspect.isclass(self.response_type) and issubclass(
                self.response_type, BaseModel
            ):
                result = self.response_type.model_validate(frame.payload)
            else:
                result = TypeAdapter(self.response_type).validate_python(frame.payload)
        except ValidationError as exc:
            await self.fail(
                ProtocolError(
                    "Invalid response",
                    details={
                        "source": "client",
                        "reason": "invalid_response",
                        "violations": [v.as_dict() for v in violations_of(exc)],
                    },
                )
            )
            return
        if not self.result_future.done():
            self.result_future.set_result(result)
        if self.output_type is not None:
            self.output_queue.put_nowait(_STREAM_END)
        await self.send_credit.close(CallCancelledError("Call is complete"))

    async def receive_error(self, frame: CallErrorFrame) -> None:
        self.trailers = dict(frame.metadata or {})
        await self.fail(
            MeshCallError.from_code(
                frame.error.code,
                frame.error.message,
                retryable=frame.error.retryable,
                details=frame.error.details,
            )
        )

    async def fail(self, error: BaseException) -> None:
        if self.terminal and self.result_future.done():
            return
        self.terminal = True
        self.disarm_timers()
        if not self.result_future.done():
            self.result_future.set_exception(error)
            if self.output_type is not None:
                # Stream consumers receive the error through output_queue. They
                # need not separately await the internal terminal result future.
                self.result_future.exception()
        self.output_queue.put_nowait(error)
        await self.send_credit.close(error)


class _ClientServerStream(RpcServerStream[BaseModel]):
    def __init__(self, open_task: asyncio.Task[_ClientCall]) -> None:
        self._open_task = open_task

    def __aiter__(self) -> _ClientServerStream:
        return self

    async def __anext__(self) -> BaseModel:
        call = await self._open_task
        return await call.next_output()

    @property
    def trailers(self) -> Mapping[str, str]:
        """Server trailers; populated once the stream has terminated."""
        if not self._open_task.done() or self._open_task.cancelled():
            return {}
        if self._open_task.exception() is not None:
            return {}
        return self._open_task.result().trailers

    async def cancel(self, reason: str = "client_closed") -> None:
        call = await self._open_task
        await call.cancel(reason)


class _ClientDuplex(_ClientServerStream, RpcDuplexClient[BaseModel, BaseModel]):
    async def send(self, item: BaseModel) -> None:
        call = await self._open_task
        await call.send(item)

    async def close_send(self) -> None:
        call = await self._open_task
        await call.close_send()

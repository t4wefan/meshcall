from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterable
from typing import Any, Self

from pydantic import BaseModel, ValidationError

from meshcall.driver import ClientDriver
from meshcall.errors import ErrorCode, MeshCallError, ProtocolError
from meshcall.flow import CreditWindow
from meshcall.protocol import (
    CallCancelFrame,
    CallErrorFrame,
    CallOpenFrame,
    CallResultFrame,
    Frame,
    PingFrame,
    PongFrame,
    StreamEndFrame,
    StreamItemFrame,
    StreamWindowFrame,
)
from meshcall.server import INITIAL_STREAM_CREDIT
from meshcall.streams import RpcDuplexClient, RpcServerStream
from meshcall.transport import FrameConnection

_STREAM_END = object()


class ClientBase:
    """Runtime base class inherited by generated service clients."""

    def __init__(self, driver: ClientDriver) -> None:
        self.driver = driver
        self._connection: FrameConnection | None = None
        self._calls: dict[str, _ClientCall] = {}
        self._state_lock = asyncio.Lock()
        self._receive_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        async with self._state_lock:
            if self._connection is not None:
                return
            self._connection = await self.driver.connect()
            self._receive_task = asyncio.create_task(
                self._receive_loop(),
                name="meshcall-client-receiver",
            )

    async def stop(self) -> None:
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
                await call.fail(
                    MeshCallError(ErrorCode.UNAVAILABLE, "Client is stopped")
                )

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.stop()

    async def _unary(
        self,
        service: str,
        method: str,
        request: BaseModel,
        response_type: type[BaseModel],
        *,
        timeout: float | None = None,
    ) -> Any:
        call = await self._open_call(
            service,
            method,
            request,
            response_type=response_type,
            timeout=timeout,
        )
        try:
            return await call.result()
        except asyncio.CancelledError:
            await asyncio.shield(call.cancel("caller_cancelled"))
            raise

    def _server_stream(
        self,
        service: str,
        method: str,
        request: BaseModel,
        item_type: type[BaseModel],
        *,
        timeout: float | None = None,
    ) -> RpcServerStream[Any]:
        return _ClientServerStream(
            asyncio.create_task(
                self._open_call(
                    service,
                    method,
                    request,
                    output_type=item_type,
                    timeout=timeout,
                )
            )
        )

    async def _client_stream(
        self,
        service: str,
        method: str,
        request: BaseModel,
        items: AsyncIterable[BaseModel],
        input_type: type[BaseModel],
        response_type: type[BaseModel],
        *,
        timeout: float | None = None,
    ) -> Any:
        call = await self._open_call(
            service,
            method,
            request,
            response_type=response_type,
            input_type=input_type,
            timeout=timeout,
        )
        pump = asyncio.create_task(
            self._pump_input(call, items),
            name=f"meshcall-input-{call.call_id}",
        )
        try:
            return await call.result()
        except asyncio.CancelledError:
            await asyncio.shield(call.cancel("caller_cancelled"))
            raise
        finally:
            if not pump.done():
                pump.cancel()
            with contextlib.suppress(asyncio.CancelledError, MeshCallError):
                await pump

    def _duplex(
        self,
        service: str,
        method: str,
        request: BaseModel,
        input_type: type[BaseModel],
        output_type: type[BaseModel],
        result_type: type[BaseModel] | None,
        *,
        timeout: float | None = None,
    ) -> RpcDuplexClient[Any, Any, Any]:
        return _ClientDuplex(
            asyncio.create_task(
                self._open_call(
                    service,
                    method,
                    request,
                    response_type=result_type,
                    input_type=input_type,
                    output_type=output_type,
                    timeout=timeout,
                )
            )
        )

    async def _open_call(
        self,
        service: str,
        method: str,
        request: BaseModel,
        *,
        response_type: type[BaseModel] | None = None,
        input_type: type[BaseModel] | None = None,
        output_type: type[BaseModel] | None = None,
        timeout: float | None = None,
    ) -> _ClientCall:
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        await self.start()
        connection = self._connection
        if connection is None:
            raise MeshCallError(ErrorCode.UNAVAILABLE, "Client is not connected")
        call_id = uuid.uuid4().hex
        call = _ClientCall(
            client=self,
            connection=connection,
            call_id=call_id,
            response_type=response_type,
            input_type=input_type,
            output_type=output_type,
        )
        self._calls[call_id] = call
        try:
            await connection.send(
                CallOpenFrame(
                    call_id=call_id,
                    service=service,
                    method=method,
                    payload=request.model_dump(
                        mode="json", by_alias=True, exclude_unset=True
                    ),
                    deadline_unix_ms=(
                        int((time.time() + timeout) * 1000)
                        if timeout is not None
                        else None
                    ),
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
        except BaseException:
            self._calls.pop(call_id, None)
            raise
        return call

    async def _pump_input(
        self,
        call: _ClientCall,
        items: AsyncIterable[BaseModel],
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

    async def _receive_loop(self) -> None:
        connection = self._connection
        if connection is None:
            return
        failure: MeshCallError | None = None
        try:
            while True:
                frame = await connection.receive()
                if isinstance(frame, PingFrame):
                    await connection.send(PongFrame(nonce=frame.nonce))
                    continue
                await self._dispatch(frame)
        except asyncio.CancelledError:
            raise
        except ConnectionError:
            failure = MeshCallError(
                ErrorCode.UNAVAILABLE,
                "Connection was lost",
                retryable=True,
            )
        except Exception as exc:  # noqa: BLE001
            failure = ProtocolError(str(exc))
        finally:
            if failure is not None:
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


class _ClientCall:
    def __init__(
        self,
        *,
        client: ClientBase,
        connection: FrameConnection,
        call_id: str,
        response_type: type[BaseModel] | None,
        input_type: type[BaseModel] | None,
        output_type: type[BaseModel] | None,
    ) -> None:
        self.client = client
        self.connection = connection
        self.call_id = call_id
        self.response_type = response_type
        self.input_type = input_type
        self.output_type = output_type
        self.send_credit = CreditWindow()
        self.result_future: asyncio.Future[Any] = (
            asyncio.get_running_loop().create_future()
        )
        self.output_queue: asyncio.Queue[Any] = asyncio.Queue()
        self.input_sequence = 0
        self.output_sequence = 0
        self.output_allowance = 0
        self.input_closed = False
        self.output_closed = False
        self.terminal = False

    async def send(self, item: BaseModel) -> None:
        if self.input_closed:
            raise ProtocolError("Input stream is closed")
        await self.send_credit.acquire()
        if self.terminal:
            raise MeshCallError(ErrorCode.CANCELLED, "Call is closed")
        validated = (
            self.input_type.model_validate(item)
            if self.input_type is not None and self.input_type is not BaseModel
            else item
        )
        await self.connection.send(
            StreamItemFrame(
                call_id=self.call_id,
                direction="client",
                sequence=self.input_sequence,
                payload=validated.model_dump(mode="json", by_alias=True),
            )
        )
        self.input_sequence += 1

    async def close_send(self) -> None:
        if not self.input_closed:
            self.input_closed = True
            await self.connection.send(
                StreamEndFrame(call_id=self.call_id, direction="client")
            )

    async def next_output(self) -> BaseModel:
        value = await self.output_queue.get()
        if value is _STREAM_END:
            raise StopAsyncIteration
        if isinstance(value, BaseException):
            raise value
        if not self.terminal:
            self.output_allowance += 1
            await self.connection.send(
                StreamWindowFrame(
                    call_id=self.call_id,
                    direction="server",
                    credit=1,
                )
            )
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
        try:
            item = self.output_type.model_validate(frame.payload)
        except ValidationError as exc:
            await self.fail(ProtocolError(f"Invalid output item: {exc}"))
            return
        self.output_queue.put_nowait(item)

    async def receive_end(self, frame: StreamEndFrame) -> None:
        if frame.direction != "server" or self.output_type is None:
            await self.fail(ProtocolError("Invalid output stream end"))
            return
        if not self.output_closed:
            self.output_closed = True
            self.output_queue.put_nowait(_STREAM_END)

    async def receive_result(self, frame: CallResultFrame) -> None:
        if self.terminal:
            return
        self.terminal = True
        try:
            result = (
                self.response_type.model_validate(frame.payload)
                if self.response_type is not None
                else None
            )
        except ValidationError as exc:
            await self.fail(ProtocolError(f"Invalid call result: {exc}"))
            return
        if not self.result_future.done():
            self.result_future.set_result(result)
        await self.send_credit.close(
            MeshCallError(ErrorCode.CANCELLED, "Call is complete")
        )

    async def receive_error(self, frame: CallErrorFrame) -> None:
        await self.fail(
            MeshCallError(
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

    async def cancel(self, reason: str = "client_closed") -> None:
        call = await self._open_task
        await call.cancel(reason)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.cancel()


class _ClientDuplex(RpcDuplexClient[BaseModel, BaseModel, Any]):
    def __init__(self, open_task: asyncio.Task[_ClientCall]) -> None:
        self._open_task = open_task

    def __aiter__(self) -> _ClientDuplex:
        return self

    async def __anext__(self) -> BaseModel:
        call = await self._open_task
        return await call.next_output()

    async def send(self, item: BaseModel) -> None:
        call = await self._open_task
        await call.send(item)

    async def close_send(self) -> None:
        call = await self._open_task
        await call.close_send()

    async def result(self) -> Any:
        call = await self._open_task
        return await call.result()

    async def cancel(self, reason: str = "client_closed") -> None:
        call = await self._open_task
        await call.cancel(reason)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.cancel()

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from dataclasses import dataclass
from typing import Protocol

from websockets.exceptions import ConnectionClosed

from meshcall.protocol import (
    CallCancelFrame,
    Frame,
    PingFrame,
    PongFrame,
    StreamWindowFrame,
    decode_frame,
    encode_frame,
)


class WebSocketLike(Protocol):
    async def send(self, message: str | bytes) -> None: ...

    async def recv(self, decode: bool | None = None) -> str | bytes: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...


class FrameConnection:
    def __init__(self, websocket: WebSocketLike, *, max_frame_size: int) -> None:
        self._websocket = websocket
        self._max_frame_size = max_frame_size
        self._control_queue: deque[_PendingFrame] = deque()
        self._call_queues: dict[str, deque[_PendingFrame]] = {}
        self._call_order: deque[str] = deque()
        self._send_ready = asyncio.Event()
        self._sender_task: asyncio.Task[None] | None = None
        self._closed: BaseException | None = None

    async def send(self, frame: Frame) -> None:
        if self._closed is not None:
            raise self._closed
        encoded = encode_frame(frame)
        if len(encoded.encode()) > self._max_frame_size:
            raise ValueError(
                f"Encoded frame exceeds {self._max_frame_size} byte limit"
            )
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        pending = _PendingFrame(encoded=encoded, future=future)
        call_id = getattr(frame, "call_id", None)
        if _is_control_frame(frame) or not isinstance(call_id, str):
            self._control_queue.append(pending)
        else:
            queue = self._call_queues.get(call_id)
            if queue is None:
                queue = deque()
                self._call_queues[call_id] = queue
                self._call_order.append(call_id)
            queue.append(pending)
        if self._sender_task is None:
            self._sender_task = asyncio.create_task(
                self._send_loop(),
                name="meshcall-frame-sender",
            )
        self._send_ready.set()
        await future

    async def receive(self) -> Frame:
        try:
            data = await self._websocket.recv()
        except ConnectionClosed as exc:
            raise ConnectionError("WebSocket connection is closed") from exc
        return decode_frame(data)

    async def close(self) -> None:
        if self._closed is None:
            self._closed = ConnectionError("WebSocket connection is closed")
        await self._websocket.close()
        if self._sender_task is not None:
            sender_task, self._sender_task = self._sender_task, None
            sender_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sender_task
        self._fail_pending(self._closed)

    async def _send_loop(self) -> None:
        while True:
            await self._send_ready.wait()
            pending = self._next_pending()
            if pending is None:
                self._send_ready.clear()
                continue
            try:
                await self._websocket.send(pending.encoded)
            except ConnectionClosed:
                error = ConnectionError("WebSocket connection is closed")
                self._closed = error
                if not pending.future.done():
                    pending.future.set_exception(error)
                self._fail_pending(error)
                return
            except Exception as exc:  # noqa: BLE001
                self._closed = exc
                if not pending.future.done():
                    pending.future.set_exception(exc)
                self._fail_pending(exc)
                return
            if not pending.future.done():
                pending.future.set_result(None)

    def _next_pending(self) -> _PendingFrame | None:
        if self._control_queue:
            return self._control_queue.popleft()
        while self._call_order:
            call_id = self._call_order.popleft()
            queue = self._call_queues.get(call_id)
            if not queue:
                self._call_queues.pop(call_id, None)
                continue
            pending = queue.popleft()
            if queue:
                self._call_order.append(call_id)
            else:
                self._call_queues.pop(call_id, None)
            return pending
        return None

    def _fail_pending(self, error: BaseException) -> None:
        pending_frames = list(self._control_queue)
        self._control_queue.clear()
        for queue in self._call_queues.values():
            pending_frames.extend(queue)
        self._call_queues.clear()
        self._call_order.clear()
        for pending in pending_frames:
            if not pending.future.done():
                pending.future.set_exception(error)


@dataclass(frozen=True)
class _PendingFrame:
    encoded: str
    future: asyncio.Future[None]


def _is_control_frame(frame: Frame) -> bool:
    return isinstance(
        frame,
        (CallCancelFrame, StreamWindowFrame, PingFrame, PongFrame),
    )

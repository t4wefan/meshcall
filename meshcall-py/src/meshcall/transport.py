from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Literal, Protocol

from websockets.exceptions import ConnectionClosed

from meshcall.errors import ResourceExhaustedError
from meshcall.protocol import (
    CallCancelFrame,
    Frame,
    PingFrame,
    PongFrame,
    ServerDrainAckFrame,
    StreamWindowFrame,
    decode_frame,
    encode_frame,
)

#: Largest encoded frame, sent or received, unless configured smaller; also
#: the Router's ceiling.
DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024


def is_frame_too_large(error: BaseException) -> bool:
    """Whether ``error`` is a frame that ``FrameConnection.send`` did not send."""
    return (
        isinstance(error, ResourceExhaustedError)
        and (error.details or {}).get("reason") == "frame_too_large"
    )


class WebSocketLike(Protocol):
    async def send(self, message: str | bytes) -> None: ...

    async def recv(self, decode: bool | None = None) -> str | bytes: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...


class FrameConnection:
    """Frame ordering, fairness, and liveness over one WebSocket.

    Protocol ``ping``/``pong`` frames are handled here so that neither the
    client nor the server runtime sees them. A connecting peer passes
    ``ping_interval``/``ping_timeout`` and this connection keeps itself alive;
    an accepting peer passes ``idle_timeout`` and this connection drops a peer
    that stays silent for that long.
    """

    def __init__(
        self,
        websocket: WebSocketLike,
        *,
        max_frame_size: int,
        source: Literal["client", "server"],
        ping_interval: float | None = None,
        ping_timeout: float | None = None,
        idle_timeout: float | None = None,
    ) -> None:
        self._websocket = websocket
        self._max_frame_size = max_frame_size
        self._source = source
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._idle_timeout = idle_timeout
        self._control_queue: deque[_PendingFrame] = deque()
        self._call_queues: dict[str, deque[_PendingFrame]] = {}
        self._call_order: deque[str] = deque()
        self._send_ready = asyncio.Event()
        self._sender_task: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self._pending_pong: asyncio.Future[None] | None = None
        self._pending_nonce: str | None = None
        self._drain_ack: asyncio.Future[None] | None = None
        self._closed: BaseException | None = None

    async def send(self, frame: Frame) -> None:
        if self._closed is not None:
            raise self._closed
        encoded = encode_frame(frame)
        size = len(encoded.encode())
        if size > self._max_frame_size:
            # The peer would close the connection on receiving it; only this
            # frame's call fails (docs/protocol.md → Frame size).
            raise ResourceExhaustedError(
                f"Not sent: {frame.kind} is {size} bytes, limit {self._max_frame_size}",
                details={"source": self._source, "reason": "frame_too_large"},
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
        """Return the next non-keepalive frame.

        Raises ``ConnectionError`` when the socket closes, when the peer stays
        silent past ``idle_timeout``, or when a keepalive ping goes unanswered.
        """
        self.start_keepalive()
        while True:
            try:
                if self._idle_timeout is None:
                    data = await self._websocket.recv()
                else:
                    async with asyncio.timeout(self._idle_timeout):
                        data = await self._websocket.recv()
            except ConnectionClosed as exc:
                raise self._mark_closed("WebSocket connection is closed") from exc
            except TimeoutError as exc:
                error = self._mark_closed("Peer was idle for too long")
                await self._websocket.close(1001, "idle timeout")
                raise error from exc
            frame = decode_frame(data)
            if isinstance(frame, PingFrame):
                with contextlib.suppress(ConnectionError):
                    await self.send(PongFrame(nonce=frame.nonce))
                continue
            if isinstance(frame, ServerDrainAckFrame):
                if self._drain_ack is not None and not self._drain_ack.done():
                    self._drain_ack.set_result(None)
                continue
            if isinstance(frame, PongFrame):
                if (
                    self._pending_pong is not None
                    and frame.nonce == self._pending_nonce
                    and not self._pending_pong.done()
                ):
                    self._pending_pong.set_result(None)
                continue
            return frame

    def drain_ack(self) -> asyncio.Future[None]:
        """Future resolved when the Router acknowledges ``server.drain``."""
        if self._drain_ack is None:
            self._drain_ack = asyncio.get_running_loop().create_future()
        return self._drain_ack

    def start_keepalive(self) -> None:
        """Begin pinging the peer. Called automatically by ``receive``."""
        if self._ping_interval is None or self._keepalive_task is not None:
            return
        if self._closed is not None:
            return
        self._keepalive_task = asyncio.create_task(
            self._keepalive_loop(),
            name="meshcall-keepalive",
        )

    async def close(self) -> None:
        if self._closed is None:
            self._closed = ConnectionError("WebSocket connection is closed")
        await self._websocket.close()
        for attribute in ("_sender_task", "_keepalive_task"):
            task: asyncio.Task[None] | None = getattr(self, attribute)
            if task is not None:
                setattr(self, attribute, None)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._fail_pending(self._closed)
        if self._drain_ack is not None and not self._drain_ack.done():
            self._drain_ack.set_exception(self._closed)

    def _mark_closed(self, message: str) -> BaseException:
        if self._closed is None:
            self._closed = ConnectionError(message)
            self._fail_pending(self._closed)
            if self._drain_ack is not None and not self._drain_ack.done():
                self._drain_ack.set_exception(self._closed)
        return self._closed

    async def _keepalive_loop(self) -> None:
        assert self._ping_interval is not None
        timeout = self._ping_timeout if self._ping_timeout is not None else 10.0
        loop = asyncio.get_running_loop()
        while self._closed is None:
            await asyncio.sleep(self._ping_interval)
            if self._closed is not None:
                return
            nonce = uuid.uuid4().hex
            self._pending_nonce = nonce
            self._pending_pong = loop.create_future()
            try:
                await self.send(PingFrame(nonce=nonce))
                async with asyncio.timeout(timeout):
                    await self._pending_pong
            except (TimeoutError, ConnectionError):
                self._mark_closed("Peer did not answer keepalive ping")
                # Closing the socket wakes the receive loop with ConnectionClosed.
                with contextlib.suppress(Exception):
                    await self._websocket.close(1001, "keepalive timeout")
                return
            finally:
                self._pending_pong = None
                self._pending_nonce = None

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

from __future__ import annotations

import asyncio
from typing import Protocol

from websockets.exceptions import ConnectionClosed

from meshcall.protocol import Frame, decode_frame, encode_frame


class WebSocketLike(Protocol):
    async def send(self, message: str | bytes) -> None: ...

    async def recv(self, decode: bool | None = None) -> str | bytes: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...


class FrameConnection:
    def __init__(self, websocket: WebSocketLike, *, max_frame_size: int) -> None:
        self._websocket = websocket
        self._max_frame_size = max_frame_size
        self._send_lock = asyncio.Lock()

    async def send(self, frame: Frame) -> None:
        encoded = encode_frame(frame)
        if len(encoded.encode()) > self._max_frame_size:
            raise ValueError(
                f"Encoded frame exceeds {self._max_frame_size} byte limit"
            )
        try:
            async with self._send_lock:
                await self._websocket.send(encoded)
        except ConnectionClosed as exc:
            raise ConnectionError("WebSocket connection is closed") from exc

    async def receive(self) -> Frame:
        try:
            data = await self._websocket.recv()
        except ConnectionClosed as exc:
            raise ConnectionError("WebSocket connection is closed") from exc
        return decode_frame(data)

    async def close(self) -> None:
        await self._websocket.close()


from __future__ import annotations

import asyncio

import pytest

from meshcall.protocol import StreamItemFrame, StreamWindowFrame, decode_frame
from meshcall.transport import FrameConnection


class RecordingWebSocket:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(self, message: str | bytes) -> None:
        await asyncio.sleep(0)
        assert isinstance(message, str)
        self.messages.append(message)

    async def recv(self, decode: bool | None = None) -> str | bytes:
        raise NotImplementedError

    async def close(self, code: int = 1000, reason: str = "") -> None:
        return None


async def test_sends_control_first_and_round_robins_calls() -> None:
    websocket = RecordingWebSocket()
    connection = FrameConnection(websocket, max_frame_size=1024 * 1024, source="client")

    frames = [
        StreamItemFrame(
            call_id="a",
            direction="client",
            sequence=0,
            payload={},
        ),
        StreamItemFrame(
            call_id="a",
            direction="client",
            sequence=1,
            payload={},
        ),
        StreamItemFrame(
            call_id="b",
            direction="client",
            sequence=0,
            payload={},
        ),
        StreamItemFrame(
            call_id="b",
            direction="client",
            sequence=1,
            payload={},
        ),
        StreamWindowFrame(call_id="a", direction="server", credit=1),
    ]
    tasks = [asyncio.create_task(connection.send(frame)) for frame in frames]
    await asyncio.gather(*tasks)

    sent = [decode_frame(message) for message in websocket.messages]
    assert isinstance(sent[0], StreamWindowFrame)
    items = [frame for frame in sent[1:] if isinstance(frame, StreamItemFrame)]
    assert len(items) == 4
    assert [frame.call_id for frame in items] == ["a", "b", "a", "b"]
    await connection.close()


class StalledWebSocket(RecordingWebSocket):
    """A socket whose send never completes, like one the peer just dropped."""

    def __init__(self) -> None:
        super().__init__()
        self.sending = asyncio.Event()

    async def send(self, message: str | bytes) -> None:
        self.sending.set()
        await asyncio.Event().wait()


async def test_close_fails_the_frame_being_sent() -> None:
    # The sender has taken the frame off its queue when close() cancels it;
    # the caller of send() used to wait for it forever (a Router dropping a
    # client mid-stream hung `async for` on the call).
    websocket = StalledWebSocket()
    connection = FrameConnection(websocket, max_frame_size=1024 * 1024, source="client")
    sending = asyncio.create_task(
        connection.send(StreamWindowFrame(call_id="a", direction="server", credit=1))
    )
    await websocket.sending.wait()
    await connection.close()
    with pytest.raises(ConnectionError):
        await asyncio.wait_for(sending, 1)

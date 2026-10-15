"""The local transport: Unix sockets, and named pipes on Windows (only there).

asyncio has no public stream API for pipes, so the Proactor event loop's pipe
support (Windows' default loop) is attached to websockets the way websockets
attaches TCP: the client overrides how the connection's transport is opened,
the server builds the websockets ``Server`` around ``start_serving_pipe``.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable, Callable
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.asyncio.server import Server, ServerConnection
from websockets.server import ServerProtocol

from meshcall.errors import DriverStateError

PIPE_PREFIX = "\\\\.\\pipe\\"
WINDOWS = sys.platform == "win32"


def pipe_address(name: str) -> str:
    """``NAME`` or ``\\\\.\\pipe\\NAME`` → the pipe's full address."""
    if not WINDOWS:
        raise ValueError("Named pipes are Windows-only; use unix_path= here")
    name = name.removeprefix(PIPE_PREFIX)
    if not name or len(name) > 256 or "\\" in name:
        raise ValueError(f"Invalid pipe name {name!r}: 1-256 characters, no backslash")
    return PIPE_PREFIX + name


def check_unix_supported() -> None:
    if WINDOWS:
        raise ValueError("Unix sockets are not supported on Windows; use pipe= instead")


def _loop_method(name: str) -> Callable[..., Awaitable[Any]]:
    method = getattr(asyncio.get_running_loop(), name, None)
    if method is None:
        raise DriverStateError(
            "Named pipes need asyncio's Proactor event loop (the Windows default)"
        )
    return method


class pipe_connect(connect):
    """``websockets.asyncio.client.connect`` over a named pipe."""

    def __init__(self, address: str, **kwargs: Any) -> None:
        super().__init__("ws://localhost/", **kwargs)
        self.pipe_address = address

    async def open_tcp_connection(self) -> ClientConnection:
        create = _loop_method("create_pipe_connection")
        _, connection = await create(
            lambda: self.factory(self.ws_uri), self.pipe_address
        )
        # Unlike create_connection, create_pipe_connection returns before the
        # transport calls connection_made (it is scheduled with call_soon), and
        # the handshake that follows needs the transport attached.
        while not hasattr(connection, "transport"):
            await asyncio.sleep(0)
        return connection


class _PipeListener:
    """The part of ``asyncio.Server`` that websockets' ``Server`` uses."""

    def __init__(self, loop: asyncio.AbstractEventLoop, pipes: list[Any]) -> None:
        self._loop = loop
        self._pipes = pipes
        self._serving = True

    @property
    def sockets(self) -> tuple[()]:
        return ()

    def is_serving(self) -> bool:
        return self._serving

    def get_loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def close(self) -> None:
        self._serving = False
        for pipe in self._pipes:
            pipe.close()

    async def wait_closed(self) -> None:
        # Pipe connections are tracked by websockets' Server, which waits for
        # their handlers after this returns.
        return


async def pipe_serve(
    handler: Callable[[ServerConnection], Awaitable[None]],
    address: str,
    *,
    max_size: int,
) -> Server:
    """``websockets.asyncio.server.serve`` on a named pipe."""
    start = _loop_method("start_serving_pipe")
    server = Server(handler)

    def factory() -> ServerConnection:
        return ServerConnection(
            ServerProtocol(max_size=max_size, logger=server.logger), server
        )

    pipes = await start(factory, address)
    server.wrap(_PipeListener(asyncio.get_running_loop(), pipes))  # pyright: ignore[reportArgumentType]
    return server

from __future__ import annotations

import asyncio
import contextlib
import uuid
from pathlib import Path
from stat import S_ISSOCK
from typing import TYPE_CHECKING

from websockets.asyncio.client import ClientConnection, connect, unix_connect
from websockets.asyncio.server import Server, ServerConnection, serve, unix_serve

from meshcall.driver import ClientDriver, DriverBinding, ServerDriver
from meshcall.errors import DriverStateError, ProtocolError
from meshcall.protocol import (
    PROTOCOL_VERSION,
    HelloAckFrame,
    HelloFrame,
    ServerRegisterAckFrame,
    ServerRegisterFrame,
)
from meshcall.router_auth import RouterCredentials
from meshcall.transport import FrameConnection

if TYPE_CHECKING:
    from meshcall.server import ServerRuntime

MAX_UNIX_SOCKET_PATH_BYTES = 103


class WebSocketClientDriver(ClientDriver):
    def __init__(
        self,
        uri: str | None = None,
        *,
        unix_path: str | Path | None = None,
        peer_id: str | None = None,
        max_frame_size: int = 1024 * 1024,
        auth: RouterCredentials | None = None,
    ) -> None:
        if (uri is None) == (unix_path is None):
            raise ValueError("Set exactly one of uri or unix_path")
        self.uri = uri
        self.unix_path = Path(unix_path) if unix_path is not None else None
        self.peer_id = peer_id or uuid.uuid4().hex
        self.max_frame_size = max_frame_size
        self.auth = auth
        self._connection: FrameConnection | None = None

    async def connect(self) -> FrameConnection:
        if self._connection is not None:
            return self._connection
        websocket = await _connect_endpoint(
            uri=self.uri,
            unix_path=self.unix_path,
            max_frame_size=self.max_frame_size,
            auth=self.auth,
        )
        connection = FrameConnection(websocket, max_frame_size=self.max_frame_size)
        await connection.send(HelloFrame(role="client", peer_id=self.peer_id))
        response = await connection.receive()
        if not isinstance(response, HelloAckFrame):
            await connection.close()
            raise ProtocolError("Expected hello.ack from WebSocket endpoint")
        if response.protocol != PROTOCOL_VERSION:
            await connection.close()
            raise ProtocolError(f"Unsupported protocol: {response.protocol}")
        self._connection = connection
        return connection

    async def close(self) -> None:
        if self._connection is not None:
            connection, self._connection = self._connection, None
            await connection.close()


class WebSocketDirectServerDriver(ServerDriver):
    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        *,
        unix_path: str | Path | None = None,
        max_frame_size: int = 1024 * 1024,
    ) -> None:
        super().__init__()
        if unix_path is not None and (host is not None or port is not None):
            raise ValueError("Unix socket cannot be combined with host or port")
        self.host = host or "127.0.0.1"
        self.port = 0 if port is None else port
        self.unix_path = Path(unix_path) if unix_path is not None else None
        self.max_frame_size = max_frame_size
        self._server: Server | None = None
        self._connections: set[FrameConnection] = set()

    @property
    def bound_port(self) -> int:
        if self.unix_path is not None:
            raise DriverStateError("Unix socket listener does not have a TCP port")
        if self._server is None or not self._server.sockets:
            raise DriverStateError("WebSocket server is not running")
        return int(self._server.sockets[0].getsockname()[1])

    async def start(
        self,
        binding: DriverBinding,
        runtime: ServerRuntime,
    ) -> None:
        self.check_binding(binding)

        async def handler(websocket: ServerConnection) -> None:
            connection = FrameConnection(
                websocket,
                max_frame_size=self.max_frame_size,
            )
            self._connections.add(connection)
            try:
                hello = await connection.receive()
                if not isinstance(hello, HelloFrame) or hello.role != "client":
                    raise ProtocolError("Direct server expects a client hello")
                if hello.protocol != PROTOCOL_VERSION:
                    raise ProtocolError(f"Unsupported protocol: {hello.protocol}")
                await connection.send(
                    HelloAckFrame(connection_id=uuid.uuid4().hex)
                )
                await runtime.serve_connection(connection)
            finally:
                self._connections.discard(connection)
                await connection.close()

        if self.unix_path is not None:
            if len(str(self.unix_path).encode()) > MAX_UNIX_SOCKET_PATH_BYTES:
                raise DriverStateError(
                    "Unix socket path exceeds the portable 103-byte limit"
                )
            if self.unix_path.exists():
                raise DriverStateError(
                    f"Unix socket path already exists: {self.unix_path}"
                )
            self.unix_path.parent.mkdir(parents=True, exist_ok=True)
            self._server = await unix_serve(
                handler,
                str(self.unix_path),
                max_size=self.max_frame_size,
            )
        else:
            self._server = await serve(
                handler,
                self.host,
                self.port,
                max_size=self.max_frame_size,
            )

    async def stop(self, binding: DriverBinding) -> None:
        self.check_binding(binding)
        if self._server is not None:
            server, self._server = self._server, None
            server.close()
            await server.wait_closed()
        connections = tuple(self._connections)
        self._connections.clear()
        for connection in connections:
            await connection.close()
        if (
            self.unix_path is not None
            and self.unix_path.exists()
            and S_ISSOCK(self.unix_path.stat().st_mode)
        ):
            self.unix_path.unlink()


class WebSocketRouterServerDriver(ServerDriver):
    def __init__(
        self,
        uri: str | None = None,
        *,
        unix_path: str | Path | None = None,
        instance_id: str | None = None,
        max_frame_size: int = 1024 * 1024,
        auth: RouterCredentials | None = None,
    ) -> None:
        super().__init__()
        if (uri is None) == (unix_path is None):
            raise ValueError("Set exactly one of uri or unix_path")
        self.uri = uri
        self.unix_path = Path(unix_path) if unix_path is not None else None
        self.instance_id = instance_id or uuid.uuid4().hex
        self.max_frame_size = max_frame_size
        self.auth = auth
        self._connection: FrameConnection | None = None
        self._runtime_task: asyncio.Task[None] | None = None

    async def start(
        self,
        binding: DriverBinding,
        runtime: ServerRuntime,
    ) -> None:
        self.check_binding(binding)
        websocket = await _connect_endpoint(
            uri=self.uri,
            unix_path=self.unix_path,
            max_frame_size=self.max_frame_size,
            auth=self.auth,
        )
        connection = FrameConnection(websocket, max_frame_size=self.max_frame_size)
        try:
            await connection.send(
                HelloFrame(
                    role="server",
                    peer_id=self.instance_id,
                    instance_id=self.instance_id,
                )
            )
            hello_ack = await connection.receive()
            if not isinstance(hello_ack, HelloAckFrame):
                raise ProtocolError("Expected hello.ack from WebSocket Router")
            await connection.send(
                ServerRegisterFrame(
                    instance_id=self.instance_id,
                    services=runtime.registration(),
                )
            )
            register_ack = await connection.receive()
            if (
                not isinstance(register_ack, ServerRegisterAckFrame)
                or register_ack.instance_id != self.instance_id
            ):
                raise ProtocolError("Expected matching server.register.ack")
        except BaseException:
            await connection.close()
            raise
        self._connection = connection
        self._runtime_task = asyncio.create_task(
            runtime.serve_connection(connection),
            name=f"meshcall-router-server-{self.instance_id}",
        )

    async def stop(self, binding: DriverBinding) -> None:
        self.check_binding(binding)
        connection, self._connection = self._connection, None
        runtime_task, self._runtime_task = self._runtime_task, None
        if connection is not None:
            await connection.close()
        if runtime_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await runtime_task


async def _connect_endpoint(
    *,
    uri: str | None,
    unix_path: Path | None,
    max_frame_size: int,
    auth: RouterCredentials | None = None,
) -> ClientConnection:
    headers = {"Authorization": auth.authorization_header()} if auth is not None else None
    if unix_path is not None:
        return await unix_connect(
            str(unix_path),
            uri="ws://localhost/",
            max_size=max_frame_size,
            additional_headers=headers,
        )
    if uri is None:
        raise DriverStateError("WebSocket URI is not configured")
    return await connect(uri, max_size=max_frame_size, additional_headers=headers)

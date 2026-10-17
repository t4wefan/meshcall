from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import Sequence
from pathlib import Path
from stat import S_ISSOCK
from typing import TYPE_CHECKING

from websockets.asyncio.client import ClientConnection, connect, unix_connect
from websockets.asyncio.server import Server, ServerConnection, serve, unix_serve

from meshcall.context import (
    DEFAULT_IDLE_TIMEOUT,
    DEFAULT_PING_INTERVAL,
    DEFAULT_PING_TIMEOUT,
    DEFAULT_RECONNECT,
    ReconnectPolicy,
)
from meshcall.driver import ClientDriver, DriverBinding, ServerDriver
from meshcall.drivers.local import (
    check_unix_supported,
    pipe_address,
    pipe_connect,
    pipe_serve,
)
from meshcall.errors import DriverStateError, MeshCallError, ProtocolError
from meshcall.protocol import (
    PROTOCOL_VERSION,
    HelloAckFrame,
    HelloFrame,
    ServerDrainFrame,
    ServerRegisterAckFrame,
    ServerRegisterFrame,
)
from meshcall.router_auth import RouterCredentials
from meshcall.transport import DEFAULT_MAX_FRAME_SIZE, FrameConnection

if TYPE_CHECKING:
    from meshcall.server import ServerRuntime

MAX_UNIX_SOCKET_PATH_BYTES = 103


def _local_endpoint(
    uri: object, unix_path: str | Path | None, pipe: str | None
) -> tuple[Path | None, str | None]:
    """Validate the choice of exactly one endpoint; return (unix path, pipe address)."""
    if sum(value is not None for value in (uri, unix_path, pipe)) != 1:
        raise ValueError("Set exactly one of uri, unix_path or pipe")
    if unix_path is not None:
        check_unix_supported()
    return (
        Path(unix_path) if unix_path is not None else None,
        pipe_address(pipe) if pipe is not None else None,
    )


class WebSocketClientDriver(ClientDriver):
    """Connect to a direct server or a Router.

    ``uri`` may be a list: endpoints are tried in order starting from the last
    one that worked, so several independent Routers give failover without any
    Router-side coordination.
    """

    def __init__(
        self,
        uri: str | Sequence[str] | None = None,
        *,
        unix_path: str | Path | None = None,
        pipe: str | None = None,
        peer_id: str | None = None,
        max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
        auth: RouterCredentials | None = None,
        ping_interval: float | None = DEFAULT_PING_INTERVAL,
        ping_timeout: float = DEFAULT_PING_TIMEOUT,
    ) -> None:
        self.unix_path, self.pipe = _local_endpoint(uri, unix_path, pipe)
        self.uris: tuple[str, ...] = (
            () if uri is None else ((uri,) if isinstance(uri, str) else tuple(uri))
        )
        if uri is not None and not self.uris:
            raise ValueError("uri list must not be empty")
        self.uri = self.uris[0] if self.uris else None
        self._cursor = 0
        self.peer_id = peer_id or uuid.uuid4().hex
        self.max_frame_size = max_frame_size
        self.auth = auth
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self._connection: FrameConnection | None = None

    @property
    def endpoint(self) -> str | None:
        """The URI of the current (or most recently used) connection."""
        return self.uris[self._cursor % len(self.uris)] if self.uris else None

    async def connect(self) -> FrameConnection:
        if self._connection is not None:
            return self._connection
        if not self.uris:  # a Unix socket or a named pipe
            self._connection = await self._connect_one(None)
            return self._connection
        # One pass over the endpoints, starting from the last good one.
        last: BaseException | None = None
        for offset in range(len(self.uris)):
            index = (self._cursor + offset) % len(self.uris)
            try:
                connection = await self._connect_one(self.uris[index])
            except (OSError, ConnectionError, MeshCallError) as exc:
                last = exc
                continue
            self._cursor = index
            self._connection = connection
            return connection
        assert last is not None
        raise last

    async def _connect_one(self, uri: str | None) -> FrameConnection:
        websocket = await _connect_endpoint(
            uri=uri,
            unix_path=self.unix_path,
            pipe=self.pipe,
            max_frame_size=self.max_frame_size,
            auth=self.auth,
        )
        connection = FrameConnection(
            websocket,
            max_frame_size=self.max_frame_size,
            source="client",
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout,
        )
        try:
            await connection.send(HelloFrame(role="client", peer_id=self.peer_id))
            response = await connection.receive()
            if not isinstance(response, HelloAckFrame):
                raise ProtocolError("Expected hello.ack from WebSocket endpoint")
            if response.protocol != PROTOCOL_VERSION:
                raise ProtocolError(f"Unsupported protocol: {response.protocol}")
        except BaseException:
            await connection.close()
            raise
        return connection

    async def close(self) -> None:
        if self._connection is not None:
            connection, self._connection = self._connection, None
            await connection.close()
            # The next connect() starts from the following endpoint: a
            # connection that just died is the least likely to come back first.
            if len(self.uris) > 1:
                self._cursor = (self._cursor + 1) % len(self.uris)


class WebSocketDirectServerDriver(ServerDriver):
    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        *,
        unix_path: str | Path | None = None,
        pipe: str | None = None,
        max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
        idle_timeout: float | None = DEFAULT_IDLE_TIMEOUT,
        instance_id: str | None = None,
    ) -> None:
        super().__init__(instance_id=instance_id)
        if (unix_path is not None or pipe is not None) and (
            host is not None or port is not None
        ):
            raise ValueError(
                "A Unix socket or named pipe cannot be combined with host or port"
            )
        self.host = host or "127.0.0.1"
        self.port = 0 if port is None else port
        if unix_path is not None or pipe is not None:
            self.unix_path, self.pipe = _local_endpoint(None, unix_path, pipe)
        else:
            self.unix_path, self.pipe = None, None
        self.max_frame_size = max_frame_size
        self.idle_timeout = idle_timeout
        self._server: Server | None = None
        self._connections: set[FrameConnection] = set()

    @property
    def bound_port(self) -> int:
        if self.unix_path is not None or self.pipe is not None:
            raise DriverStateError("A local listener does not have a TCP port")
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
                source="server",
                idle_timeout=self.idle_timeout,
            )
            self._connections.add(connection)
            try:
                hello = await connection.receive()
                if not isinstance(hello, HelloFrame) or hello.role != "client":
                    raise ProtocolError("Direct server expects a client hello")
                if hello.protocol != PROTOCOL_VERSION:
                    raise ProtocolError(f"Unsupported protocol: {hello.protocol}")
                await connection.send(HelloAckFrame(connection_id=uuid.uuid4().hex))
                await runtime.serve_connection(connection)
            finally:
                self._connections.discard(connection)
                await connection.close()

        if self.pipe is not None:
            self._server = await pipe_serve(
                handler, self.pipe, max_size=self.max_frame_size
            )
        elif self.unix_path is not None:
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

    async def drain(
        self,
        binding: DriverBinding,
        runtime: ServerRuntime,
        *,
        timeout: float,
    ) -> None:
        self.check_binding(binding)
        if self._server is not None:
            # Stop accepting; existing connections keep running their calls.
            self._server.close(close_connections=False)
        await runtime.wait_idle(timeout)

    async def stop(self, binding: DriverBinding) -> None:
        self.check_binding(binding)
        server, self._server = self._server, None
        if server is not None:
            server.close(close_connections=False)
        # Close our connections ourselves (a drain may have already closed the
        # listener, in which case wait_closed would block on them forever).
        connections = tuple(self._connections)
        self._connections.clear()
        for connection in connections:
            await connection.close()
        if server is not None:
            await server.wait_closed()
        if (
            self.unix_path is not None
            and self.unix_path.exists()
            and S_ISSOCK(self.unix_path.stat().st_mode)
        ):
            self.unix_path.unlink()


class WebSocketRouterServerDriver(ServerDriver):
    """Register a service instance with one or more Routers.

    With several URIs the instance keeps one registration per Router, each
    with its own reconnect loop, so clients can fail over between Routers
    without any Router-side coordination. ``start()`` succeeds as soon as one
    Router accepted the registration; the others keep retrying.
    """

    def __init__(
        self,
        uri: str | Sequence[str] | None = None,
        *,
        unix_path: str | Path | None = None,
        pipe: str | None = None,
        instance_id: str | None = None,
        max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
        auth: RouterCredentials | None = None,
        ping_interval: float | None = DEFAULT_PING_INTERVAL,
        ping_timeout: float = DEFAULT_PING_TIMEOUT,
        reconnect: ReconnectPolicy = DEFAULT_RECONNECT,
    ) -> None:
        super().__init__(instance_id=instance_id)
        self.unix_path, self.pipe = _local_endpoint(uri, unix_path, pipe)
        uris = () if uri is None else ((uri,) if isinstance(uri, str) else tuple(uri))
        if uri is not None and not uris:
            raise ValueError("uri list must not be empty")
        self.uri = uris[0] if uris else None
        self.max_frame_size = max_frame_size
        self.auth = auth
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.reconnect = reconnect
        endpoints: list[str | None] = list(uris) if uris else [None]
        self._links = [_RouterLink(self, endpoint) for endpoint in endpoints]
        self._draining = False

    @property
    def connected(self) -> bool:
        """Whether at least one Router currently holds this instance's registration."""
        return any(link.connection is not None for link in self._links)

    @property
    def connected_endpoints(self) -> tuple[str | None, ...]:
        return tuple(
            link.endpoint for link in self._links if link.connection is not None
        )

    async def start(
        self,
        binding: DriverBinding,
        runtime: ServerRuntime,
    ) -> None:
        self.check_binding(binding)
        results = await asyncio.gather(
            *(link.register(runtime) for link in self._links), return_exceptions=True
        )
        failures = [r for r in results if isinstance(r, BaseException)]
        if len(failures) == len(self._links):
            raise failures[0]
        for link, result in zip(self._links, results, strict=True):
            if isinstance(result, BaseException):
                runtime.logger.warning(  # noqa: PLE1205
                    "Router {} rejected or refused instance {} at start ({}); retrying in the background",
                    link.endpoint,
                    self.instance_id,
                    result,
                )
            link.task = asyncio.create_task(
                link.supervise(
                    runtime, initial_failure=isinstance(result, BaseException)
                ),
                name=f"meshcall-router-server-{self.instance_id}",
            )

    async def drain(
        self,
        binding: DriverBinding,
        runtime: ServerRuntime,
        *,
        timeout: float,
    ) -> None:
        self.check_binding(binding)
        self._draining = True
        started = time.monotonic()
        acks = [
            link.send_drain() for link in self._links if link.connection is not None
        ]
        if acks:
            async with asyncio.timeout(timeout):
                await asyncio.gather(*acks, return_exceptions=True)
        remaining = max(0.0, timeout - (time.monotonic() - started))
        await runtime.wait_idle(remaining)

    async def stop(self, binding: DriverBinding) -> None:
        self.check_binding(binding)
        await asyncio.gather(*(link.close() for link in self._links))


class _RouterLink:
    """One registration with one Router, supervised independently."""

    def __init__(
        self, driver: WebSocketRouterServerDriver, endpoint: str | None
    ) -> None:
        self.driver = driver
        self.endpoint = endpoint
        self.connection: FrameConnection | None = None
        self.task: asyncio.Task[None] | None = None

    async def register(self, runtime: ServerRuntime) -> None:
        self.connection = await self._open(runtime)

    async def _open(self, runtime: ServerRuntime) -> FrameConnection:
        driver = self.driver
        websocket = await _connect_endpoint(
            uri=self.endpoint,
            unix_path=driver.unix_path,
            pipe=driver.pipe,
            max_frame_size=driver.max_frame_size,
            auth=driver.auth,
        )
        connection = FrameConnection(
            websocket,
            max_frame_size=driver.max_frame_size,
            source="server",
            ping_interval=driver.ping_interval,
            ping_timeout=driver.ping_timeout,
        )
        try:
            await connection.send(
                HelloFrame(
                    role="server",
                    peer_id=driver.instance_id,
                    instance_id=driver.instance_id,
                )
            )
            hello_ack = await connection.receive()
            if not isinstance(hello_ack, HelloAckFrame):
                raise ProtocolError("Expected hello.ack from WebSocket Router")
            if hello_ack.protocol != PROTOCOL_VERSION:
                raise ProtocolError(f"Unsupported protocol: {hello_ack.protocol}")
            await connection.send(
                ServerRegisterFrame(
                    instance_id=driver.instance_id,
                    services=runtime.registration(),
                    max_inflight=runtime.max_inflight,
                )
            )
            register_ack = await connection.receive()
            if (
                not isinstance(register_ack, ServerRegisterAckFrame)
                or register_ack.instance_id != driver.instance_id
            ):
                raise ProtocolError("Expected matching server.register.ack")
        except BaseException:
            await connection.close()
            raise
        return connection

    async def send_drain(self) -> None:
        connection = self.connection
        if connection is None:
            return
        ack = connection.drain_ack()
        await connection.send(ServerDrainFrame())
        await ack

    async def supervise(self, runtime: ServerRuntime, *, initial_failure: bool) -> None:
        """Serve this Router's connection and re-register after it drops."""
        driver = self.driver
        where = (
            f"instance {driver.instance_id} at "
            f"{self.endpoint or driver.unix_path or driver.pipe}"
        )
        while True:
            if self.connection is None:
                if driver._draining or not driver.reconnect.enabled:
                    if not initial_failure:
                        runtime.logger.warning(  # noqa: PLE1205
                            "Router connection lost for {}; reconnect disabled", where
                        )
                    return
                if not initial_failure:
                    runtime.logger.warning(  # noqa: PLE1205
                        "Router connection lost for {}; reconnecting", where
                    )
                initial_failure = False
                for delay in driver.reconnect.delays():
                    await asyncio.sleep(delay)
                    try:
                        self.connection = await self._open(runtime)
                    except (OSError, ConnectionError, MeshCallError) as exc:
                        runtime.logger.warning(  # noqa: PLE1205
                            "Router re-registration failed for {}: {}", where, exc
                        )
                        continue
                    runtime.logger.info(  # noqa: PLE1205
                        "Router connection restored for {}", where
                    )
                    break
                else:
                    runtime.logger.error(  # noqa: PLE1205
                        "Giving up on Router reconnection for {}", where
                    )
                    return
            connection = self.connection
            assert connection is not None
            await runtime.serve_connection(connection)
            if self.connection is not connection:
                return  # stop() took the connection away
            self.connection = None
            await connection.close()  # releases the sender task of the dead socket

    async def close(self) -> None:
        connection, self.connection = self.connection, None
        task, self.task = self.task, None
        if connection is not None:
            await connection.close()
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


async def _connect_endpoint(
    *,
    uri: str | None,
    unix_path: Path | None,
    pipe: str | None = None,
    max_frame_size: int,
    auth: RouterCredentials | None = None,
) -> ClientConnection:
    headers = (
        {"Authorization": auth.authorization_header()} if auth is not None else None
    )
    if pipe is not None:
        return await pipe_connect(
            pipe, max_size=max_frame_size, additional_headers=headers
        )
    if unix_path is not None:
        return await unix_connect(
            str(unix_path),
            uri="ws://localhost/",
            max_size=max_frame_size,
            additional_headers=headers,
        )
    if uri is None:
        raise DriverStateError("WebSocket URI is not configured")
    if uri.startswith("pipe://"):
        return await pipe_connect(
            pipe_address(uri.removeprefix("pipe://")),
            max_size=max_frame_size,
            additional_headers=headers,
        )
    if uri.startswith("unix://"):
        check_unix_supported()
        return await unix_connect(
            uri.removeprefix("unix://"),
            uri="ws://localhost/",
            max_size=max_frame_size,
            additional_headers=headers,
        )
    return await connect(uri, max_size=max_frame_size, additional_headers=headers)

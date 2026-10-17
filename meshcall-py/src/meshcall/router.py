from __future__ import annotations

import asyncio
import contextlib
import copy
import inspect
import json
import os
import signal
from collections.abc import Generator, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self
from urllib.parse import urlsplit

from meshcall.client import ClientBase
from meshcall.context import (
    DEFAULT_PING_INTERVAL,
    DEFAULT_PING_TIMEOUT,
    DEFAULT_RECONNECT,
    DEFAULT_RETRY,
    ClientInterceptor,
    ReconnectPolicy,
    RetryPolicy,
    ServerInterceptor,
)
from meshcall.contract import (
    get_service_contract,
    get_service_warnings,
    normalize_service_name,
)
from meshcall.drivers.local import PIPE_PREFIX, check_unix_supported, pipe_address
from meshcall.drivers.websocket import (
    WebSocketClientDriver,
    WebSocketRouterServerDriver,
)
from meshcall.errors import ContractError, DriverStateError
from meshcall.logging import RpcLogger
from meshcall.protocol import PROTOCOL_VERSION
from meshcall.router_auth import (
    AUTH_SERVICE,
    REGISTRY_SERVICE,
    IssuedToken,
    RegistryService,
    RouterCredentials,
    RouterIdentity,
    RouterScope,
    _DescribeServiceRequest,
    _Empty,
    _IssueRequest,
    _ListServicesResult,
    _normalize_scope_service,
    _RevokeRequest,
    _RevokeResult,
)
from meshcall.router_install import find_router_binary
from meshcall.server import RpcServer
from meshcall.streams import RpcDuplexClient, RpcServerStream
from meshcall.transport import DEFAULT_MAX_FRAME_SIZE

if TYPE_CHECKING:
    import httpx

DEFAULT_ROUTER_URL = "ws://127.0.0.1:8765"
_VALID_URL_SCHEMES = frozenset({"ws", "wss", "http", "https"})


class LocalRouter:
    """Own a local Go ``meshcall-router`` subprocess."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        *,
        unix_path: str | Path | None = None,
        pipe: str | None = None,
        max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
        outbox_size: int | None = None,
        binary_path: str | Path | None = None,
        auth_file: str | Path | None = None,
        max_connections: int | None = None,
        metrics_listen: str | None = None,
        http_cors_origins: Sequence[str] | None = None,
        http_default_timeout: float | None = None,
        startup_timeout: float = 10,
        shutdown_timeout: float = 5,
    ) -> None:
        if sum(v is not None for v in (unix_path, pipe, host or port)) > 1:
            raise ValueError(
                "Choose one of host/port, unix_path (not on Windows) or pipe (Windows)"
            )
        if unix_path is not None:
            check_unix_supported()
        #: Named pipe name (\\.\pipe\NAME), the Windows local transport.
        self.pipe = (
            pipe_address(pipe).removeprefix(PIPE_PREFIX) if pipe is not None else None
        )
        if port is not None and not 0 <= port <= 65535:
            raise ValueError("Port must be between 0 and 65535")
        if not 1 <= max_frame_size <= DEFAULT_MAX_FRAME_SIZE:
            raise ValueError("max_frame_size must be between 1 and 67108864")
        if outbox_size is not None and outbox_size < max_frame_size:
            raise ValueError("outbox_size must be at least max_frame_size")
        if startup_timeout <= 0 or shutdown_timeout <= 0:
            raise ValueError("Process timeouts must be positive")
        if max_connections is not None and max_connections < 1:
            raise ValueError("max_connections must be positive")
        self.host = host or "127.0.0.1"
        self.port = 0 if port is None else port
        self.unix_path = Path(unix_path).absolute() if unix_path is not None else None
        if self.unix_path is not None and len(str(self.unix_path).encode()) > 103:
            raise DriverStateError(
                "Unix socket path exceeds the portable 103-byte limit"
            )
        self.max_frame_size = max_frame_size
        #: Bytes queued for a peer that is not reading; None = Router default.
        self.outbox_size = outbox_size
        self.binary_path = binary_path
        self.auth_file = Path(auth_file).absolute() if auth_file is not None else None
        #: Open WebSocket connections the Router accepts; None = Router default.
        self.max_connections = max_connections
        self.metrics_listen = metrics_listen
        self.http_cors_origins = (
            tuple(http_cors_origins) if http_cors_origins is not None else None
        )
        self.http_default_timeout = http_default_timeout
        self.startup_timeout = startup_timeout
        self.shutdown_timeout = shutdown_timeout
        self._process: asyncio.subprocess.Process | None = None
        self._bound_port: int | None = None
        self._ready = False
        self._lock = asyncio.Lock()
        self._stderr = bytearray()
        self._readers: list[asyncio.Task[None]] = []
        self._is_http = False

    @property
    def is_running(self) -> bool:
        return (
            self._ready
            and self._process is not None
            and self._process.returncode is None
        )

    @property
    def pid(self) -> int | None:
        return (
            self._process.pid if self.is_running and self._process is not None else None
        )

    @property
    def bound_port(self) -> int:
        if self.unix_path is not None or self.pipe is not None:
            raise DriverStateError("A local listener does not have a TCP port")
        if not self.is_running or self._bound_port is None:
            raise DriverStateError("Go Router is not running")
        return self._bound_port

    @property
    def url(self) -> str:
        """Primary endpoint URL for this local Router."""
        if self.unix_path is not None:
            return f"unix://{self.unix_path}"
        if self.pipe is not None:
            return f"pipe://{self.pipe}"
        host_fmt = f"[{self.host}]" if ":" in self.host else self.host
        return f"ws://{host_fmt}:{self.bound_port}"

    @property
    def http_url(self) -> str:
        """HTTP endpoint URL for this local Router (TCP listeners only)."""
        if self.unix_path is not None or self.pipe is not None:
            raise DriverStateError("A local listener does not have a TCP port")
        host_fmt = f"[{self.host}]" if ":" in self.host else self.host
        return f"http://{host_fmt}:{self.bound_port}"

    def _endpoints(self) -> tuple[tuple[str, ...], Path | None, str | None]:
        if not self.is_running:
            raise DriverStateError("Go Router is not running")
        if self.unix_path is not None:
            return (), self.unix_path, None
        if self.pipe is not None:
            return (), None, self.pipe
        return (self.url,), None, None

    async def _drain(self, stream: asyncio.StreamReader, *, diagnostic: bool) -> None:
        while data := await stream.read(4096):
            if diagnostic:
                self._stderr.extend(data)
                del self._stderr[:-8192]

    async def start(self) -> None:
        async with self._lock:
            if self.is_running:
                return
            await self._stop()
            command = [
                find_router_binary(self.binary_path),
                "--shutdown-on-stdin-close",
                "--max-frame-size",
                str(self.max_frame_size),
            ]
            if self.outbox_size is not None:
                command.extend(["--outbox-size", str(self.outbox_size)])
            if self.pipe is not None:
                command.extend(["--pipe", self.pipe])
            elif self.unix_path is not None:
                command.extend(["--unix-socket", str(self.unix_path)])
            else:
                command.extend(["--host", self.host, "--port", str(self.port)])
            if self.auth_file is not None:
                command.extend(["--auth-file", str(self.auth_file)])
            if self.max_connections is not None:
                command.extend(["--max-connections", str(self.max_connections)])
            if self.metrics_listen is not None:
                command.extend(["--metrics-listen", self.metrics_listen])
            if self.http_cors_origins:
                for origin in self.http_cors_origins:
                    command.extend(["--http-cors-origin", origin])
            if self.http_default_timeout is not None:
                ms = max(1, round(self.http_default_timeout * 1000))
                command.extend(["--http-default-timeout", f"{ms}ms"])
            self._stderr.clear()
            try:
                process = await asyncio.create_subprocess_exec(
                    *command,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    limit=16384,
                )
                self._process = process
                assert process.stdout is not None and process.stderr is not None
                self._readers.append(
                    asyncio.create_task(self._drain(process.stderr, diagnostic=True))
                )
                line = await asyncio.wait_for(
                    process.stdout.readline(), timeout=self.startup_timeout
                )
                if not line:
                    raise DriverStateError("Go Router exited before readiness")
                ready = json.loads(line)
                if (
                    not isinstance(ready, dict)
                    or ready.get("kind") != "router.ready"
                    or ready.get("protocol") != PROTOCOL_VERSION
                    or ready.get("pid") != process.pid
                ):
                    raise DriverStateError("Invalid Go Router readiness message")
                if self.pipe is not None:
                    if ready.get("pipe") != self.pipe:
                        raise DriverStateError("Invalid Go Router pipe name")
                elif self.unix_path is not None:
                    if ready.get("unix_path") != str(self.unix_path):
                        raise DriverStateError("Invalid Go Router Unix socket path")
                else:
                    port = ready.get("port")
                    if type(port) is not int or not 1 <= port <= 65535:
                        raise DriverStateError("Invalid Go Router bound port")
                    self._bound_port = port
                self._readers.append(
                    asyncio.create_task(self._drain(process.stdout, diagnostic=False))
                )
                if process.returncode is not None:
                    raise DriverStateError("Go Router exited during startup")
                self._ready = True
            except asyncio.CancelledError:
                await self._stop()
                raise
            except Exception as exc:
                await self._stop()
                detail = self._stderr.decode(errors="replace").strip()
                message = f"Could not start Go Router: {exc}"
                if detail:
                    message += f"\n{detail}"
                raise DriverStateError(message) from exc

    async def stop(self) -> None:
        async with self._lock:
            await self._stop()

    async def _stop(self) -> None:
        process, self._process = self._process, None
        self._ready = False
        self._bound_port = None
        if process is not None:
            if process.stdin is not None:
                process.stdin.close()
            if process.returncode is None:
                try:
                    await asyncio.wait_for(process.wait(), self.shutdown_timeout)
                except TimeoutError:
                    with contextlib.suppress(ProcessLookupError):
                        process.terminate()
                    try:
                        await asyncio.wait_for(process.wait(), self.shutdown_timeout)
                    except TimeoutError:
                        with contextlib.suppress(ProcessLookupError):
                            process.kill()
                        await process.wait()
            await process.wait()
        if self._readers:
            await asyncio.gather(*self._readers)
            self._readers.clear()

    async def wait_closed(self) -> int:
        if self._process is None:
            raise DriverStateError("Go Router is not running")
        return await self._process.wait()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()


# Keep alias for internal/low-level process tests.
WebSocketRouter = LocalRouter


def _resolve_endpoints(
    url: str | Sequence[str] | None,
    unix_path: str | Path | None,
    pipe: str | None,
) -> tuple[tuple[str, ...], Path | None, str | None]:
    if sum(v is not None for v in (url, unix_path, pipe)) > 1:
        raise ValueError("Set at most one of url, unix_path or pipe")
    if unix_path is not None:
        check_unix_supported()
        return (), Path(unix_path), None
    if pipe is not None:
        return (), None, pipe
    if url is None:
        env = os.environ.get("MESHCALL_ROUTER_URL", "").strip()
        if env:
            parts = [part.strip() for part in env.split(",") if part.strip()]
            return _resolve_endpoints(
                parts[0] if len(parts) == 1 else parts, None, None
            )
        return (DEFAULT_ROUTER_URL,), None, None
    urls = (url,) if isinstance(url, str) else tuple(url)
    if not urls:
        raise ValueError("url list must not be empty")
    if len(urls) == 1:
        single = urls[0]
        if single.startswith("unix://"):
            check_unix_supported()
            return (), Path(single.removeprefix("unix://")), None
        if single.startswith("pipe://"):
            return (), None, single.removeprefix("pipe://")
    schemes = set()
    for item in urls:
        parsed = urlsplit(item)
        if parsed.scheme not in _VALID_URL_SCHEMES or not parsed.netloc:
            raise ValueError(
                f"Unsupported Router URL {item!r}; expected ws://, wss://, http://, https://, unix://, or pipe://"
            )
        schemes.add("http" if parsed.scheme in ("http", "https") else "ws")
    if len(schemes) > 1:
        raise ValueError("Cannot mix HTTP and WebSocket URLs in a single Router")
    return urls, None, None


class Router:
    """Layer-2 endpoint descriptor for a remote or external MeshCall Router.

    With no arguments, ``Router()`` resolves ``MESHCALL_ROUTER_URL`` from the
    environment or defaults to ``ws://127.0.0.1:8765``.
    """

    def __init__(
        self,
        url: str | Path | Sequence[str] | None = None,
        *,
        unix_path: str | Path | None = None,
        pipe: str | None = None,
    ) -> None:
        if isinstance(url, Path):
            if unix_path is not None:
                raise ValueError("Pass either Path url or unix_path, not both")
            unix_path = url
            url = None
        self.urls, self.unix_path, self.pipe = _resolve_endpoints(url, unix_path, pipe)
        self._is_http = bool(
            self.urls and self.urls[0].startswith(("http://", "https://"))
        )

    @property
    def url(self) -> str | None:
        """Primary URL, or None when using a Unix socket or named pipe."""
        return self.urls[0] if self.urls else None

    def _endpoints(self) -> tuple[tuple[str, ...], Path | None, str | None]:
        return self.urls, self.unix_path, self.pipe


class RegistryClient:
    """Client for the Router's built-in ``meshcall.router.Registry`` service."""

    def __init__(self, mesh: Mesh) -> None:
        self._mesh = mesh

    async def list(self) -> list[RegistryService]:
        """List all services visible to this caller in the Router's Registry."""
        result = await self._mesh.call_unary(
            REGISTRY_SERVICE, "list", _Empty(), _ListServicesResult
        )
        return result.services

    async def describe(self, service: str) -> RegistryService:
        """Describe a registered service, its instances, methods, and contract."""
        try:
            normalized = normalize_service_name(service)[0]
        except Exception:  # noqa: BLE001
            normalized = service
        return await self._mesh.call_unary(
            REGISTRY_SERVICE,
            "describe",
            _DescribeServiceRequest(service=normalized),
            RegistryService,
        )


class AuthClient:
    """Client for the Router's built-in ``meshcall.router.AuthService`` service."""

    def __init__(self, mesh: Mesh) -> None:
        self._mesh = mesh

    async def whoami(self) -> RouterIdentity:
        """Return the caller's authenticated identity and permissions."""
        return await self._mesh.call_unary(
            AUTH_SERVICE, "whoami", _Empty(), RouterIdentity
        )

    async def issue_token(
        self, scopes: Sequence[RouterScope], ttl_seconds: int = 900
    ) -> IssuedToken:
        """Issue a scoped, short-lived Bearer token."""
        normalized = [
            scope.model_copy(
                update={"service": _normalize_scope_service(scope.service)}
            )
            for scope in scopes
        ]
        return await self._mesh.call_unary(
            AUTH_SERVICE,
            "issue_token",
            _IssueRequest(ttl_seconds=ttl_seconds, scopes=normalized),
            IssuedToken,
        )

    async def revoke_token(self, token_id: str) -> bool:
        """Revoke a previously issued Bearer token by its ``token_id``."""
        result = await self._mesh.call_unary(
            AUTH_SERVICE,
            "revoke_token",
            _RevokeRequest(token_id=token_id),
            _RevokeResult,
        )
        return result.revoked


def _check_strict_services(services: Sequence[object]) -> None:
    for svc in services:
        svc_type = svc if inspect.isclass(svc) else type(svc)
        contract = get_service_contract(svc_type)
        findings = get_service_warnings(svc_type)
        if findings:
            rendered = "\n".join(f.render() for f in findings)
            raise ContractError(
                f"Strict mode: {len(findings)} contract warning(s) for {contract.name}\n"
                f"{rendered}"
            )


class RunningServer:
    """Handle for a running Mesh server session."""

    def __init__(
        self,
        *,
        mesh: Mesh,
        services: Sequence[object],
        instance_id: str | None = None,
        max_inflight: int | None = None,
        drain_timeout: float = 30.0,
        access_log: bool = True,
        log_level: str | int = "INFO",
        colorize: bool = False,
        logger: RpcLogger | None = None,
        interceptors: Sequence[ServerInterceptor] = (),
    ) -> None:
        self.mesh = mesh
        self.router = mesh.router
        self.drain_timeout = drain_timeout
        urls, unix_path, pipe = mesh.router._endpoints()
        self._driver = WebSocketRouterServerDriver(
            uri=urls if urls else None,
            unix_path=unix_path,
            pipe=pipe,
            instance_id=instance_id,
            max_frame_size=mesh.max_frame_size,
            auth=mesh.credentials,
            ping_interval=mesh.ping_interval,
            ping_timeout=mesh.ping_timeout,
            reconnect=mesh.reconnect,
        )
        self.server = RpcServer(
            services=services,
            driver=self._driver,
            access_log=access_log,
            log_level=log_level,
            colorize=colorize,
            logger=logger,
            interceptors=interceptors,
            max_inflight=max_inflight,
        )
        self._stopped = False

    @property
    def instance_id(self) -> str:
        return self.server.instance_id

    @property
    def url(self) -> str | None:
        return self.router.url

    async def start(self) -> None:
        self._stopped = False
        await self.server.start()

    async def stop(
        self, *, drain: bool = True, drain_timeout: float | None = None
    ) -> None:
        if self._stopped:
            return
        self._stopped = True
        timeout = self.drain_timeout if drain_timeout is None else drain_timeout
        try:
            await self.server.stop(drain=drain, drain_timeout=timeout)
        finally:
            if self.mesh._running_server is self:
                self.mesh._running_server = None

    async def wait_closed(self) -> None:
        await self.server.wait_closed()


class _ServeHandle:
    """Dual-mode object returned by :meth:`Mesh.serve`.

    1. ``async with mesh.serve() as srv:`` starts serving on enter and drains +
       closes on exit.
    2. ``await mesh.serve()`` starts serving, waits until ``SIGINT`` /
       ``SIGTERM`` (or server close), then drains and closes.
    """

    def __init__(
        self,
        mesh: Mesh,
        *,
        drain_timeout: float = 30.0,
        access_log: bool = True,
        log_level: str | int = "INFO",
        colorize: bool = False,
        logger: RpcLogger | None = None,
    ) -> None:
        self._mesh = mesh
        self._drain_timeout = drain_timeout
        self._access_log = access_log
        self._log_level = log_level
        self._colorize = colorize
        self._logger = logger
        self._server: RunningServer | None = None

    def _build_server(self) -> RunningServer:
        if self._mesh._running_server is not None:
            raise DriverStateError("Mesh is already serving")
        srv = RunningServer(
            mesh=self._mesh,
            services=self._mesh._mounted_services,
            instance_id=self._mesh._instance_id,
            max_inflight=self._mesh._max_inflight,
            drain_timeout=self._drain_timeout,
            access_log=self._access_log,
            log_level=self._log_level,
            colorize=self._colorize,
            logger=self._logger if self._logger is not None else self._mesh.logger,
            interceptors=self._mesh._server_interceptors,
        )
        self._mesh._running_server = srv
        self._server = srv
        return srv

    async def __aenter__(self) -> RunningServer:
        await self._mesh._ensure_router_started()
        srv = self._build_server()
        try:
            await srv.start()
        except BaseException:
            if self._mesh._running_server is srv:
                self._mesh._running_server = None
            if self._mesh._owns_local_router and not self._mesh._entered:
                await self._mesh.close()
            raise
        return srv

    async def __aexit__(self, *_: object) -> None:
        try:
            if self._server is not None:
                await self._server.stop(drain=True, drain_timeout=self._drain_timeout)
        finally:
            if self._mesh._owns_local_router and not self._mesh._entered:
                await self._mesh.close()

    async def _serve_forever(self) -> None:
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(sig, stop_event.set)
        await self._mesh._ensure_router_started()
        srv = self._build_server()
        try:
            await srv.start()
        except BaseException:
            if self._mesh._running_server is srv:
                self._mesh._running_server = None
            if self._mesh._owns_local_router:
                await self._mesh.close()
            raise
        try:
            closed_task = asyncio.create_task(srv.wait_closed())
            signal_task = asyncio.create_task(stop_event.wait())
            _done, pending = await asyncio.wait(
                {closed_task, signal_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        finally:
            await srv.stop(drain=True, drain_timeout=self._drain_timeout)
            if self._mesh._owns_local_router:
                await self._mesh.close()

    def __await__(self) -> Generator[Any, None, None]:
        return self._serve_forever().__await__()


class Mesh:
    """Layer-3 Mesh participant: shared client connection, built-in services, and service host."""

    def __init__(
        self,
        router: Router | LocalRouter,
        *,
        token: str | None = None,
        credentials: RouterCredentials | None = None,
        headers: Mapping[str, str] | None = None,
        peer_id: str | None = None,
        ping_interval: float | None = DEFAULT_PING_INTERVAL,
        ping_timeout: float = DEFAULT_PING_TIMEOUT,
        reconnect: ReconnectPolicy = DEFAULT_RECONNECT,
        retry: RetryPolicy = DEFAULT_RETRY,
        deadline_grace: float | None = None,
        timeout: float | None = None,
        max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
        interceptors: Sequence[ClientInterceptor] = (),
        logger: RpcLogger | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        if not isinstance(router, (Router, LocalRouter)):
            raise TypeError(
                "Mesh requires a Router or LocalRouter instance "
                "(e.g. Mesh(Router(...)) or Mesh(local_router))"
            )
        if credentials is not None and token is not None:
            raise ValueError("Pass credentials or token, not both")
        self.router = router
        self.credentials = credentials or (
            RouterCredentials.bearer(token) if token is not None else None
        )
        self.headers = dict(headers or {})
        self.peer_id = peer_id
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.reconnect = reconnect
        self.retry = retry
        self.deadline_grace = deadline_grace
        self.timeout = timeout
        self.max_frame_size = max_frame_size
        self.interceptors = tuple(interceptors)
        self.logger = logger
        self._http_client = http_client
        self._client: Any | None = None
        self._mounted_services: list[object] = []
        self._instance_id: str | None = None
        self._max_inflight: int | None = None
        self._server_interceptors: list[ServerInterceptor] = []
        self._running_server: RunningServer | None = None
        self._owns_local_router = False
        self._entered = False

    async def _ensure_router_started(self) -> None:
        if isinstance(self.router, LocalRouter) and not self.router.is_running:
            await self.router.start()
            self._owns_local_router = True

    @property
    def registry(self) -> RegistryClient:
        """Built-in ``meshcall.router.Registry`` client."""
        return RegistryClient(self)

    @property
    def auth(self) -> AuthClient:
        """Built-in ``meshcall.router.AuthService`` client."""
        return AuthClient(self)

    @property
    def is_connected(self) -> bool:
        """Whether the shared client transport is currently open."""
        if self._client is None:
            return False
        if hasattr(self._client, "is_connected"):
            return bool(self._client.is_connected)
        return self._client._root._connection is not None

    def _ensure_client(self) -> Any:
        if self._client is None:
            if self.router._is_http:
                from meshcall.http import _HttpTransport

                assert self.router.url is not None
                self._client = _HttpTransport(
                    self.router.url,
                    credentials=self.credentials,
                    headers=self.headers,
                    retry=self.retry,
                    timeout=self.timeout,
                    client=self._http_client,
                )
            else:
                urls, unix_path, pipe = self.router._endpoints()
                driver = WebSocketClientDriver(
                    uri=urls if urls else None,
                    unix_path=unix_path,
                    pipe=pipe,
                    peer_id=self.peer_id,
                    max_frame_size=self.max_frame_size,
                    auth=self.credentials,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                )
                client = ClientBase(
                    driver,
                    deadline_grace=self.deadline_grace,
                    reconnect=self.reconnect,
                    interceptors=self.interceptors,
                    retry=self.retry,
                )
                if self.timeout is not None:
                    client = client.with_options(timeout=self.timeout)
                self._client = client
        return self._client

    def with_options(
        self,
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        instance: str | None = None,
    ) -> Self:
        """Return a shallow view of this Mesh with per-call options applied."""
        clone = copy.copy(self)
        clone._owns_local_router = False
        clone._client = self._ensure_client().with_options(
            timeout=timeout,
            idle_timeout=idle_timeout,
            metadata=metadata,
            idempotency_key=idempotency_key,
            instance=instance,
        )
        return clone

    async def connect(self) -> Self:
        """Open the underlying client connection explicitly."""
        await self._ensure_router_started()
        client = self._ensure_client()
        if hasattr(client, "start"):
            await client.start()
        return self

    async def close(self) -> None:
        """Close any running server and client connection on this Mesh."""
        try:
            if self._running_server is not None:
                await self._running_server.stop(drain=True)
            if self._client is not None:
                client, self._client = self._client, None
                stop = getattr(client, "close", None) or getattr(client, "stop", None)
                if stop is not None:
                    await stop()
        finally:
            if self._owns_local_router and isinstance(self.router, LocalRouter):
                self._owns_local_router = False
                await self.router.stop()

    async def __aenter__(self) -> Self:
        self._entered = True
        try:
            await self.connect()
        except BaseException:
            self._entered = False
            raise
        return self

    async def __aexit__(self, *_: object) -> None:
        self._entered = False
        await self.close()

    # -- Server Mounting & Lifecycle ---------------------------------------

    def mount(
        self,
        *services: object,
        instance_id: str | None = None,
        max_inflight: int | None = None,
        interceptors: Sequence[ServerInterceptor] = (),
        strict: bool = False,
    ) -> Self:
        """Register one or more ``@service`` objects to be served by this Mesh."""
        if not services:
            raise ValueError("At least one service is required")
        if self.router._is_http:
            raise ContractError(
                "Cannot mount or serve services over an HTTP(S) Router URL; "
                "use ws(s)://, unix://, or pipe://"
            )
        if self._running_server is not None:
            raise DriverStateError(
                "Cannot mount services while Mesh is already serving"
            )
        for svc in services:
            svc_type = svc if inspect.isclass(svc) else type(svc)
            get_service_contract(svc_type)
        if strict:
            _check_strict_services(services)
        self._mounted_services.extend(services)
        if instance_id is not None:
            self._instance_id = instance_id
        if max_inflight is not None:
            self._max_inflight = max_inflight
        if interceptors:
            self._server_interceptors.extend(interceptors)
        return self

    def serve(
        self,
        *,
        drain_timeout: float = 30.0,
        access_log: bool = True,
        log_level: str | int = "INFO",
        colorize: bool = False,
        logger: RpcLogger | None = None,
    ) -> _ServeHandle:
        """Serve mounted services on this Mesh.

        Use either ``async with mesh.serve() as srv:`` (background context
        manager) or ``await mesh.serve()`` (blocks until SIGINT/SIGTERM).
        """
        if not self._mounted_services:
            raise ValueError(
                "No services mounted on Mesh; call mesh.mount(...) before mesh.serve()"
            )
        return _ServeHandle(
            self,
            drain_timeout=drain_timeout,
            access_log=access_log,
            log_level=log_level,
            colorize=colorize,
            logger=logger,
        )

    def run(
        self,
        *,
        drain_timeout: float = 30.0,
        access_log: bool = True,
        log_level: str | int = "INFO",
        colorize: bool = False,
        logger: RpcLogger | None = None,
    ) -> None:
        """Synchronous entrypoint: serve mounted services until interrupted (SIGINT / SIGTERM)."""
        handle = self.serve(
            drain_timeout=drain_timeout,
            access_log=access_log,
            log_level=log_level,
            colorize=colorize,
            logger=logger,
        )
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(handle._serve_forever())

    async def drain(self, timeout: float | None = None) -> None:
        """Gracefully drain and stop the active server session on this Mesh."""
        if self._running_server is not None:
            await self._running_server.stop(drain=True, drain_timeout=timeout)

    # -- MeshCallRpc -------------------------------------------------------

    async def call_unary(self, *args: Any, **kwargs: Any) -> Any:
        return await self._ensure_client().call_unary(*args, **kwargs)

    def call_server_stream(self, *args: Any, **kwargs: Any) -> RpcServerStream[Any]:
        return self._ensure_client().call_server_stream(*args, **kwargs)

    async def call_client_stream(self, *args: Any, **kwargs: Any) -> Any:
        return await self._ensure_client().call_client_stream(*args, **kwargs)

    def call_duplex(self, *args: Any, **kwargs: Any) -> RpcDuplexClient[Any, Any]:
        return self._ensure_client().call_duplex(*args, **kwargs)

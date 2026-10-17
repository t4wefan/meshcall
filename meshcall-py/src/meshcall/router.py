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
from typing import Any, Self
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
from meshcall.errors import ContractError, DriverStateError, UnavailableError
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
from meshcall.transport import DEFAULT_MAX_FRAME_SIZE

DEFAULT_ROUTER_URL = "ws://127.0.0.1:8765"
_LOCAL_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


class WebSocketRouter:
    """Own a Go Router process; all RPC routing and authentication run in Go."""

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
    return urls, None, None


class Router:
    """Unified Router configuration, shared client connection, and built-in services.

    A ``Router`` instance can be:
    1. Passed to ``meshcall.run(..., router=router)`` or ``meshcall.serve(..., router=router)``
       to configure how services connect to (or locally auto-spawn) the Router.
    2. Connected via ``async with Router.connect(...) as router:`` and passed to
       multiple generated service clients (``WorkspaceClient(router)``) so they
       multiplex over one WebSocket connection.
    3. Used directly to call built-in Router services: ``list_services()``,
       ``describe_service()``, ``whoami()``, ``issue_token()``, ``revoke_token()``.
    """

    def __init__(
        self,
        url: str | Sequence[str] | None = None,
        *,
        unix_path: str | Path | None = None,
        pipe: str | None = None,
        auth: RouterCredentials | None = None,
        peer_id: str | None = None,
        ping_interval: float | None = DEFAULT_PING_INTERVAL,
        ping_timeout: float = DEFAULT_PING_TIMEOUT,
        reconnect: ReconnectPolicy = DEFAULT_RECONNECT,
        retry: RetryPolicy = DEFAULT_RETRY,
        deadline_grace: float | None = None,
        max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
        interceptors: Sequence[ClientInterceptor] = (),
        # Local embedded Router auto-spawn options (used by serve / run):
        spawn_if_local: bool = True,
        binary_path: str | Path | None = None,
        metrics_listen: str | None = None,
        http_cors_origins: Sequence[str] | None = None,
        http_default_timeout: float | None = None,
        auth_file: str | Path | None = None,
        outbox_size: int | None = None,
        max_connections: int | None = None,
        startup_timeout: float = 10.0,
        shutdown_timeout: float = 5.0,
    ) -> None:
        self.urls, self.unix_path, self.pipe = _resolve_endpoints(url, unix_path, pipe)
        self.auth = auth
        self.peer_id = peer_id
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.reconnect = reconnect
        self.retry = retry
        self.deadline_grace = deadline_grace
        self.max_frame_size = max_frame_size
        self.interceptors = tuple(interceptors)
        self.spawn_if_local = spawn_if_local
        self.binary_path = binary_path
        self.metrics_listen = metrics_listen
        self.http_cors_origins = (
            tuple(http_cors_origins) if http_cors_origins is not None else None
        )
        self.http_default_timeout = http_default_timeout
        self.auth_file = auth_file
        self.outbox_size = outbox_size
        self.max_connections = max_connections
        self.startup_timeout = startup_timeout
        self.shutdown_timeout = shutdown_timeout
        self._client: ClientBase | None = None

    @property
    def url(self) -> str | None:
        """Primary WebSocket URL, or None when using a Unix socket or named pipe."""
        return self.urls[0] if self.urls else None

    @property
    def is_connected(self) -> bool:
        """Whether the shared client connection is currently open."""
        return self._client is not None and self._client._root._connection is not None

    @classmethod
    def connect(
        cls,
        url: str | Sequence[str] | None = None,
        *,
        unix_path: str | Path | None = None,
        pipe: str | None = None,
        auth: RouterCredentials | None = None,
        peer_id: str | None = None,
        ping_interval: float | None = DEFAULT_PING_INTERVAL,
        ping_timeout: float = DEFAULT_PING_TIMEOUT,
        reconnect: ReconnectPolicy = DEFAULT_RECONNECT,
        retry: RetryPolicy = DEFAULT_RETRY,
        deadline_grace: float | None = None,
        max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
        interceptors: Sequence[ClientInterceptor] = (),
    ) -> Self:
        """Create and connect a shared Router client.

        May be used either as ``async with Router.connect(...) as router:`` or
        ``router = await Router.connect(...)``.
        """
        return cls(
            url=url,
            unix_path=unix_path,
            pipe=pipe,
            auth=auth,
            peer_id=peer_id,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
            reconnect=reconnect,
            retry=retry,
            deadline_grace=deadline_grace,
            max_frame_size=max_frame_size,
            interceptors=interceptors,
        )

    def _ensure_client(self) -> ClientBase:
        if self._client is None:
            driver = WebSocketClientDriver(
                uri=self.urls if self.urls else None,
                unix_path=self.unix_path,
                pipe=self.pipe,
                peer_id=self.peer_id,
                max_frame_size=self.max_frame_size,
                auth=self.auth,
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout,
            )
            self._client = ClientBase(
                driver,
                deadline_grace=self.deadline_grace,
                reconnect=self.reconnect,
                interceptors=self.interceptors,
                retry=self.retry,
            )
        return self._client

    def _create_server_driver(
        self, *, instance_id: str | None = None
    ) -> WebSocketRouterServerDriver:
        return WebSocketRouterServerDriver(
            uri=self.urls if self.urls else None,
            unix_path=self.unix_path,
            pipe=self.pipe,
            instance_id=instance_id,
            max_frame_size=self.max_frame_size,
            auth=self.auth,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout,
            reconnect=self.reconnect,
        )

    def _local_spawn_target(
        self,
    ) -> tuple[str | None, int | None, Path | None, str | None] | None:
        if self.unix_path is not None:
            return None, None, self.unix_path, None
        if self.pipe is not None:
            return None, None, None, self.pipe
        if len(self.urls) != 1:
            return None
        parsed = urlsplit(self.urls[0])
        if parsed.scheme != "ws" or parsed.hostname not in _LOCAL_HOSTS:
            return None
        port = 8765 if parsed.port is None else parsed.port
        return parsed.hostname, port, None, None

    def _set_bound_port(self, port: int) -> None:
        if len(self.urls) == 1:
            parsed = urlsplit(self.urls[0])
            host = parsed.hostname or "127.0.0.1"
            host_fmt = f"[{host}]" if ":" in host else host
            self.urls = (f"ws://{host_fmt}:{port}",)
            if self._client is not None and isinstance(
                self._client.driver, WebSocketClientDriver
            ):
                self._client.driver.uris = self.urls
                self._client.driver.uri = self.urls[0]

    def _create_embedded_router(
        self,
        host: str | None,
        port: int | None,
        unix_path: Path | None,
        pipe: str | None,
    ) -> WebSocketRouter:
        return WebSocketRouter(
            host=host,
            port=port,
            unix_path=unix_path,
            pipe=pipe,
            max_frame_size=self.max_frame_size,
            outbox_size=self.outbox_size,
            binary_path=self.binary_path,
            auth_file=self.auth_file,
            max_connections=self.max_connections,
            metrics_listen=self.metrics_listen,
            http_cors_origins=self.http_cors_origins,
            http_default_timeout=self.http_default_timeout,
            startup_timeout=self.startup_timeout,
            shutdown_timeout=self.shutdown_timeout,
        )

    def with_options(
        self,
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        instance: str | None = None,
    ) -> Self:
        """Return a shallow view of this Router with per-call options applied."""
        clone = copy.copy(self)
        clone._client = self._ensure_client().with_options(
            timeout=timeout,
            idle_timeout=idle_timeout,
            metadata=metadata,
            idempotency_key=idempotency_key,
            instance=instance,
        )
        return clone

    async def start(self) -> None:
        await self._ensure_client().start()

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.stop()

    async def close(self) -> None:
        await self.stop()

    def __await__(self) -> Generator[Any, None, Self]:
        async def _connect() -> Self:
            await self.start()
            return self

        return _connect().__await__()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    # --- Built-in Router services (Registry & AuthService) -------------------

    async def list_services(self) -> list[RegistryService]:
        """List all services visible to this caller in the Router's Registry."""
        result = await self._ensure_client().call_unary(
            REGISTRY_SERVICE, "list", _Empty(), _ListServicesResult
        )
        return result.services

    async def describe_service(self, service: str) -> RegistryService:
        """Describe a registered service, its instances, methods, and contract."""
        try:
            normalized = normalize_service_name(service)[0]
        except Exception:  # noqa: BLE001
            normalized = service
        return await self._ensure_client().call_unary(
            REGISTRY_SERVICE,
            "describe",
            _DescribeServiceRequest(service=normalized),
            RegistryService,
        )

    async def whoami(self) -> RouterIdentity:
        """Return the caller's authenticated identity and permissions."""
        return await self._ensure_client().call_unary(
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
        return await self._ensure_client().call_unary(
            AUTH_SERVICE,
            "issue_token",
            _IssueRequest(ttl_seconds=ttl_seconds, scopes=normalized),
            IssuedToken,
        )

    async def revoke_token(self, token_id: str) -> bool:
        """Revoke a previously issued Bearer token by its ``token_id``."""
        result = await self._ensure_client().call_unary(
            AUTH_SERVICE,
            "revoke_token",
            _RevokeRequest(token_id=token_id),
            _RevokeResult,
        )
        return result.revoked


class RunningServer:
    """Handle returned by :func:`serve`, managing an :class:`RpcServer` and any auto-spawned local Router."""

    def __init__(
        self,
        *,
        services: Sequence[object],
        router: Router,
        instance_id: str | None = None,
        max_inflight: int | None = None,
        drain_timeout: float = 30.0,
        access_log: bool = True,
        log_level: str | int = "INFO",
        colorize: bool = False,
        logger: RpcLogger | None = None,
        interceptors: Sequence[ServerInterceptor] = (),
    ) -> None:
        self.router = router
        self.drain_timeout = drain_timeout
        self._driver = router._create_server_driver(instance_id=instance_id)
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
        self.spawned_router: WebSocketRouter | None = None

    @property
    def instance_id(self) -> str:
        return self.server.instance_id

    async def start(self) -> None:
        target = (
            self.router._local_spawn_target() if self.router.spawn_if_local else None
        )
        # Ephemeral port (ws://127.0.0.1:0): spawn the embedded Router first so
        # we know which port it bound before registering the service.
        if target is not None and target[1] == 0:
            host, port, unix_path, pipe = target
            spawned = self.router._create_embedded_router(host, port, unix_path, pipe)
            await spawned.start()
            self.spawned_router = spawned
            self.router._set_bound_port(spawned.bound_port)
            self._driver.uri = self.router.url
            self._driver._links[0].endpoint = self.router.url
            try:
                await self.server.start()
            except BaseException:
                await spawned.stop()
                self.spawned_router = None
                raise
            return

        try:
            await self.server.start()
            return
        except (OSError, ConnectionError, UnavailableError):
            if target is None:
                raise

        host, port, unix_path, pipe = target
        spawned = self.router._create_embedded_router(host, port, unix_path, pipe)
        try:
            await spawned.start()
            self.spawned_router = spawned
        except DriverStateError:
            # Another local process may have bound the address concurrently;
            # fall back to connecting to the winner.
            await spawned.stop()
            self.spawned_router = None

        try:
            await self.server.start()
        except BaseException:
            if self.spawned_router is not None:
                await self.spawned_router.stop()
                self.spawned_router = None
            raise

    async def stop(
        self, *, drain: bool = True, drain_timeout: float | None = None
    ) -> None:
        timeout = self.drain_timeout if drain_timeout is None else drain_timeout
        try:
            await self.server.stop(drain=drain, drain_timeout=timeout)
        finally:
            if self.spawned_router is not None:
                spawned, self.spawned_router = self.spawned_router, None
                await spawned.stop()

    async def wait_closed(self) -> None:
        await self.server.wait_closed()

    def __await__(self) -> Generator[Any, None, Self]:
        async def _start() -> Self:
            await self.start()
            return self

        return _start().__await__()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop(drain=True)


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


def _coerce_router(router: Router | str | Sequence[str] | None) -> Router:
    if isinstance(router, Router):
        return router
    return Router(url=router)


def serve(
    *services: object,
    router: Router | str | Sequence[str] | None = None,
    instance_id: str | None = None,
    max_inflight: int | None = None,
    drain_timeout: float = 30.0,
    strict: bool = False,
    access_log: bool = True,
    log_level: str | int = "INFO",
    colorize: bool = False,
    logger: RpcLogger | None = None,
    interceptors: Sequence[ServerInterceptor] = (),
) -> RunningServer:
    """Start one or more MeshCall services and register them with a Router.

    May be used either as ``async with meshcall.serve(Workspace()) as srv:`` or
    ``srv = await meshcall.serve(Workspace())``. When the target is a local
    Router address and nothing is listening yet, an embedded ``meshcall-router``
    is spawned automatically unless ``router=Router(..., spawn_if_local=False)``.
    """
    if not services:
        raise ValueError("At least one service is required")
    if strict:
        _check_strict_services(services)
    router_obj = _coerce_router(router)
    return RunningServer(
        services=services,
        router=router_obj,
        instance_id=instance_id,
        max_inflight=max_inflight,
        drain_timeout=drain_timeout,
        access_log=access_log,
        log_level=log_level,
        colorize=colorize,
        logger=logger,
        interceptors=interceptors,
    )


def run(
    *services: object,
    router: Router | str | Sequence[str] | None = None,
    instance_id: str | None = None,
    max_inflight: int | None = None,
    drain_timeout: float = 30.0,
    strict: bool = False,
    access_log: bool = True,
    log_level: str | int = "INFO",
    colorize: bool = False,
    logger: RpcLogger | None = None,
    interceptors: Sequence[ServerInterceptor] = (),
) -> None:
    """Run one or more MeshCall services until interrupted (SIGINT / SIGTERM)."""
    handle = serve(
        *services,
        router=router,
        instance_id=instance_id,
        max_inflight=max_inflight,
        drain_timeout=drain_timeout,
        strict=strict,
        access_log=access_log,
        log_level=log_level,
        colorize=colorize,
        logger=logger,
        interceptors=interceptors,
    )

    async def _main() -> None:
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(sig, stop_event.set)
        await handle.start()
        try:
            closed_task = asyncio.create_task(handle.wait_closed())
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
            await handle.stop(drain=True, drain_timeout=drain_timeout)

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_main())

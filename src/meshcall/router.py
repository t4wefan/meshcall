from __future__ import annotations

import asyncio
import hashlib
import json
import random
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from stat import S_ISSOCK
from typing import Any

from loguru import logger
from websockets.asyncio.server import Server, ServerConnection, serve, unix_serve

from meshcall.errors import DriverStateError, ErrorCode, ProtocolError
from meshcall.ir import BalanceKind, BalancePolicy
from meshcall.protocol import (
    PROTOCOL_VERSION,
    CallCancelFrame,
    CallErrorFrame,
    CallOpenFrame,
    CallResultFrame,
    ErrorPayload,
    HelloAckFrame,
    HelloFrame,
    MethodRegistrationResult,
    RegisteredMethod,
    ServerRegisterAckFrame,
    ServerRegisterFrame,
)
from meshcall.transport import FrameConnection

MAX_UNIX_SOCKET_PATH_BYTES = 103


@dataclass(eq=False)
class _ClientPeer:
    connection_id: str
    connection: FrameConnection


@dataclass(eq=False)
class _ServiceInstance:
    instance_id: str
    connection: FrameConnection
    methods: set[tuple[str, str]] = field(default_factory=set)
    inflight: int = 0


@dataclass(frozen=True)
class _Route:
    call_id: str
    client: _ClientPeer
    instance: _ServiceInstance


@dataclass
class _MethodPool:
    signature: RegisteredMethod
    instances: list[_ServiceInstance] = field(default_factory=list)
    round_robin_cursor: int = 0


@dataclass
class _ServicePool:
    methods: dict[str, _MethodPool] = field(default_factory=dict)


class WebSocketRouter:
    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        *,
        unix_path: str | Path | None = None,
        max_frame_size: int = 1024 * 1024,
    ) -> None:
        if unix_path is not None and (host is not None or port is not None):
            raise ValueError("Unix socket cannot be combined with host or port")
        self.host = host or "127.0.0.1"
        self.port = 0 if port is None else port
        self.unix_path = Path(unix_path) if unix_path is not None else None
        self.max_frame_size = max_frame_size
        self._server: Server | None = None
        self._lock = asyncio.Lock()
        self._clients: dict[str, _ClientPeer] = {}
        self._instances: dict[str, _ServiceInstance] = {}
        self._services: dict[str, _ServicePool] = {}
        self._routes: dict[str, _Route] = {}

    @property
    def bound_port(self) -> int:
        if self.unix_path is not None:
            raise DriverStateError("Unix socket listener does not have a TCP port")
        if self._server is None or not self._server.sockets:
            raise DriverStateError("WebSocket Router is not running")
        return int(self._server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        if self._server is not None:
            return

        async def handler(websocket: ServerConnection) -> None:
            connection = FrameConnection(
                websocket,
                max_frame_size=self.max_frame_size,
            )
            peer: _ClientPeer | _ServiceInstance | None = None
            try:
                hello = await connection.receive()
                if not isinstance(hello, HelloFrame):
                    raise ProtocolError("Router expects hello as the first frame")
                if hello.protocol != PROTOCOL_VERSION:
                    raise ProtocolError(f"Unsupported protocol: {hello.protocol}")
                connection_id = uuid.uuid4().hex
                await connection.send(HelloAckFrame(connection_id=connection_id))
                if hello.role == "client":
                    peer = await self._add_client(connection_id, connection)
                    await self._client_loop(peer)
                else:
                    peer = await self._add_instance(hello, connection)
                    await self._server_loop(peer)
            except ConnectionError:
                pass
            except ProtocolError as exc:
                logger.warning("Router rejected connection: {}", exc.message)
            finally:
                if isinstance(peer, _ClientPeer):
                    await self._remove_client(peer)
                elif isinstance(peer, _ServiceInstance):
                    await self._remove_instance(peer)
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

    async def stop(self) -> None:
        if self._server is None:
            return
        server, self._server = self._server, None
        server.close()
        await server.wait_closed()
        async with self._lock:
            connections = {
                *(client.connection for client in self._clients.values()),
                *(instance.connection for instance in self._instances.values()),
            }
            self._clients.clear()
            self._instances.clear()
            self._services.clear()
            self._routes.clear()
        for connection in connections:
            await connection.close()
        if (
            self.unix_path is not None
            and self.unix_path.exists()
            and S_ISSOCK(self.unix_path.stat().st_mode)
        ):
            self.unix_path.unlink()

    async def _add_client(
        self,
        connection_id: str,
        connection: FrameConnection,
    ) -> _ClientPeer:
        client = _ClientPeer(connection_id, connection)
        async with self._lock:
            self._clients[connection_id] = client
        return client

    async def _add_instance(
        self,
        hello: HelloFrame,
        connection: FrameConnection,
    ) -> _ServiceInstance:
        registration = await connection.receive()
        if not isinstance(registration, ServerRegisterFrame):
            raise ProtocolError("Server must register immediately after hello")
        if not hello.instance_id or registration.instance_id != hello.instance_id:
            raise ProtocolError("Server instance_id does not match hello")
        services = {service.name: service for service in registration.services}
        if len(services) != len(registration.services):
            raise ProtocolError("Server registration contains duplicate services")
        for service in services.values():
            method_names = {method.name for method in service.methods}
            if len(method_names) != len(service.methods):
                raise ProtocolError(
                    f"Service {service.name} contains duplicate methods"
                )
        instance = _ServiceInstance(
            instance_id=registration.instance_id,
            connection=connection,
        )
        results: list[MethodRegistrationResult] = []
        async with self._lock:
            if instance.instance_id in self._instances:
                raise ProtocolError(
                    f"Duplicate service instance: {instance.instance_id}"
                )
            self._instances[instance.instance_id] = instance
            for service in services.values():
                service_pool = self._services.setdefault(
                    service.name, _ServicePool()
                )
                for method in service.methods:
                    method_pool = service_pool.methods.get(method.name)
                    reason: str | None = None
                    if method_pool is None:
                        method_pool = _MethodPool(signature=method)
                        service_pool.methods[method.name] = method_pool
                    elif (
                        method_pool.signature.stream is not method.stream
                        or method_pool.signature.schema_hash != method.schema_hash
                    ):
                        reason = "schema_mismatch"
                    elif (
                        method_pool.signature.balance.kind is BalanceKind.DISABLED
                        and method_pool.instances
                    ):
                        reason = "balance_disabled"

                    accepted = reason is None
                    results.append(
                        MethodRegistrationResult(
                            service=service.name,
                            method=method.name,
                            accepted=accepted,
                            reason=reason,
                        )
                    )
                    if not accepted:
                        continue
                    instance.methods.add((service.name, method.name))
                    method_pool.instances.append(instance)
                    method_pool.instances.sort(key=lambda item: item.instance_id)
        try:
            await connection.send(
                ServerRegisterAckFrame(
                    instance_id=instance.instance_id,
                    methods=tuple(results),
                )
            )
        except BaseException:
            await self._remove_instance(instance)
            raise
        return instance

    async def _client_loop(self, client: _ClientPeer) -> None:
        while True:
            frame = await client.connection.receive()
            if isinstance(frame, CallOpenFrame):
                await self._open_call(client, frame)
                continue
            call_id = getattr(frame, "call_id", None)
            if not isinstance(call_id, str):
                continue
            async with self._lock:
                route = self._routes.get(call_id)
                if route is None or route.client is not client:
                    route = None
            if route is not None:
                try:
                    await route.instance.connection.send(frame)
                except ConnectionError:
                    await self._remove_instance(route.instance)

    async def _server_loop(self, instance: _ServiceInstance) -> None:
        while True:
            frame = await instance.connection.receive()
            call_id = getattr(frame, "call_id", None)
            if not isinstance(call_id, str):
                continue
            async with self._lock:
                route = self._routes.get(call_id)
                if route is None or route.instance is not instance:
                    route = None
            if route is None:
                continue
            try:
                await route.client.connection.send(frame)
            except ConnectionError:
                await self._remove_client(route.client)
                continue
            if isinstance(frame, (CallResultFrame, CallErrorFrame)):
                await self._finish_route(route)

    async def _open_call(
        self,
        client: _ClientPeer,
        frame: CallOpenFrame,
    ) -> None:
        error: CallErrorFrame | None = None
        route: _Route | None = None
        async with self._lock:
            if frame.call_id in self._routes:
                error = _call_error(
                    frame.call_id,
                    ErrorCode.PROTOCOL_ERROR,
                    "Duplicate call_id",
                )
            else:
                try:
                    instance = self._select_instance(frame)
                except _RouteSelectionError as exc:
                    error = _call_error(frame.call_id, exc.code, exc.message)
                else:
                    route = _Route(frame.call_id, client, instance)
                    self._routes[frame.call_id] = route
                    instance.inflight += 1
        if error is not None:
            await client.connection.send(error)
            return
        if route is None:
            raise RuntimeError("Router selection produced no route")
        try:
            await route.instance.connection.send(frame)
        except ConnectionError:
            await self._finish_route(route)
            await client.connection.send(
                _call_error(
                    frame.call_id,
                    ErrorCode.UNAVAILABLE,
                    "Selected service instance disconnected",
                    retryable=True,
                )
            )

    def _select_instance(self, frame: CallOpenFrame) -> _ServiceInstance:
        service_pool = self._services.get(frame.service)
        if service_pool is None:
            raise _RouteSelectionError(
                ErrorCode.UNAVAILABLE,
                f"No instances registered for {frame.service}",
            )
        method_pool = service_pool.methods.get(frame.method)
        if method_pool is None:
            raise _RouteSelectionError(
                ErrorCode.METHOD_NOT_FOUND,
                f"Unknown method {frame.service}.{frame.method}",
            )
        instances = method_pool.instances
        if not instances:
            raise _RouteSelectionError(
                ErrorCode.UNAVAILABLE,
                f"No instances registered for {frame.service}.{frame.method}",
            )
        policy = method_pool.signature.balance
        if policy.kind is BalanceKind.ROUND_ROBIN:
            selected = instances[method_pool.round_robin_cursor % len(instances)]
            method_pool.round_robin_cursor = (
                method_pool.round_robin_cursor + 1
            ) % len(instances)
            return selected
        if policy.kind is BalanceKind.LEAST_INFLIGHT:
            return min(instances, key=lambda item: (item.inflight, item.instance_id))
        if policy.kind is BalanceKind.RANDOM:
            return random.choice(instances)
        if policy.kind is BalanceKind.STICKY:
            return _select_sticky(instances, policy, frame.payload)
        if len(instances) != 1:
            raise _RouteSelectionError(
                ErrorCode.UNAVAILABLE,
                f"Balance is disabled for {frame.service}.{frame.method}",
            )
        return instances[0]

    async def _finish_route(self, route: _Route) -> None:
        async with self._lock:
            if self._routes.get(route.call_id) is route:
                self._routes.pop(route.call_id, None)
                route.instance.inflight = max(0, route.instance.inflight - 1)

    async def _remove_client(self, client: _ClientPeer) -> None:
        async with self._lock:
            self._clients.pop(client.connection_id, None)
            routes = [
                route for route in self._routes.values() if route.client is client
            ]
            for route in routes:
                self._routes.pop(route.call_id, None)
                route.instance.inflight = max(0, route.instance.inflight - 1)
        for route in routes:
            try:
                await route.instance.connection.send(
                    CallCancelFrame(
                        call_id=route.call_id,
                        reason="client_disconnected",
                    )
                )
            except ConnectionError:
                pass

    async def _remove_instance(self, instance: _ServiceInstance) -> None:
        async with self._lock:
            if self._instances.get(instance.instance_id) is not instance:
                return
            self._instances.pop(instance.instance_id, None)
            for service_name, method_name in instance.methods:
                service_pool = self._services.get(service_name)
                if service_pool is None:
                    continue
                method_pool = service_pool.methods.get(method_name)
                if method_pool is None:
                    continue
                method_pool.instances = [
                    item for item in method_pool.instances if item is not instance
                ]
            routes = [
                route for route in self._routes.values() if route.instance is instance
            ]
            for route in routes:
                self._routes.pop(route.call_id, None)
            instance.inflight = 0
        for route in routes:
            try:
                await route.client.connection.send(
                    _call_error(
                        route.call_id,
                        ErrorCode.UNAVAILABLE,
                        "Service instance disconnected",
                        retryable=True,
                    )
                )
            except ConnectionError:
                pass


class _RouteSelectionError(Exception):
    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _select_sticky(
    instances: list[_ServiceInstance],
    policy: BalancePolicy,
    payload: Any,
) -> _ServiceInstance:
    if not policy.key:
        raise _RouteSelectionError(
            ErrorCode.INTERNAL,
            "Sticky policy does not declare a key",
        )
    value = payload
    for part in policy.key.split(".")[1:]:
        if not isinstance(value, dict) or part not in value:
            raise _RouteSelectionError(
                ErrorCode.INVALID_ARGUMENT,
                f"Sticky key {policy.key!r} is missing",
            )
        value = value[part]
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode()).digest()
    index = int.from_bytes(digest[:8], "big") % len(instances)
    return instances[index]


def _call_error(
    call_id: str,
    code: ErrorCode,
    message: str,
    *,
    retryable: bool = False,
) -> CallErrorFrame:
    return CallErrorFrame(
        call_id=call_id,
        error=ErrorPayload(
            code=code,
            message=message,
            retryable=retryable,
        ),
    )

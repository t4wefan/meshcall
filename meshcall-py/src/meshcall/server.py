from __future__ import annotations

import asyncio
import importlib
import inspect
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from loguru import logger as default_logger
from pydantic import BaseModel, ValidationError

from meshcall.contract import (
    MethodBinding,
    get_service_contract,
    get_service_method_binding,
)
from meshcall.driver import DriverBinding, ServerDriver
from meshcall.errors import (
    ContractError,
    DriverStateError,
    ErrorCode,
    MeshCallError,
    ProtocolError,
)
from meshcall.flow import CreditWindow
from meshcall.ir import (
    BindingKind,
    MethodContract,
    ServiceContract,
    StreamKind,
    TypeRef,
)
from meshcall.logging import RpcLogger
from meshcall.protocol import (
    CallCancelFrame,
    CallErrorFrame,
    CallOpenFrame,
    CallResultFrame,
    ErrorPayload,
    Frame,
    PingFrame,
    PongFrame,
    RegisteredMethod,
    RegisteredService,
    StreamEndFrame,
    StreamItemFrame,
    StreamWindowFrame,
)
from meshcall.streams import RpcDuplex, RpcInputStream
from meshcall.transport import FrameConnection
from meshcall.worker import WorkerLoop, submit_to_loop

INITIAL_STREAM_CREDIT = 16
_STREAM_END = object()


class ServerState(StrEnum):
    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"


@dataclass(frozen=True)
class RuntimeMethod:
    service_name: str
    contract: MethodContract
    handler: Any
    method_binding: MethodBinding
    request_type: type[BaseModel]
    response_type: type[BaseModel] | None
    input_type: type[BaseModel] | None
    output_type: type[BaseModel] | None


class ServerRuntime:
    def __init__(
        self,
        services: Sequence[object],
        worker: WorkerLoop,
        *,
        access_log: bool = True,
        log_level: str | int = "INFO",
        colorize: bool = False,
        logger: RpcLogger | None = None,
    ) -> None:
        self.worker = worker
        self.access_log = access_log
        self.log_level = log_level
        self.colorize = colorize
        self.logger: RpcLogger = logger if logger is not None else default_logger
        self._contracts: list[ServiceContract] = []
        self._methods: dict[tuple[str, str], RuntimeMethod] = {}
        for service in services:
            service_type = service if inspect.isclass(service) else type(service)
            contract = get_service_contract(service_type)
            if any(existing.name == contract.name for existing in self._contracts):
                raise ContractError(f"Duplicate service name: {contract.name}")
            self._contracts.append(contract)
            service_instance = None if inspect.isclass(service) else service
            method_bindings = {
                method.name: get_service_method_binding(service_type, method.name)
                for method in contract.methods
            }
            if service_instance is None and any(
                method.binding is BindingKind.INSTANCE for method in contract.methods
            ):
                try:
                    service_instance = service_type()
                except TypeError as exc:
                    raise ContractError(
                        f"Service {service_type.__qualname__} has instance RPC "
                        "methods and cannot be constructed without arguments; "
                        "pass a service instance to RpcServer"
                    ) from exc
            for method in contract.methods:
                method_binding = method_bindings[method.name]
                owner = (
                    service_type
                    if method.binding is BindingKind.STATIC
                    else service_instance
                )
                if owner is None:
                    raise ContractError(
                        f"Service {service_type.__qualname__} has no instance for "
                        f"method {method.name}"
                    )
                self._methods[(contract.name, method.name)] = RuntimeMethod(
                    service_name=contract.name,
                    contract=method,
                    handler=getattr(owner, method.name),
                    method_binding=method_binding,
                    request_type=method_binding.request_type,
                    response_type=_resolve_optional_type(method.response),
                    input_type=_resolve_optional_type(method.input_item),
                    output_type=_resolve_optional_type(method.output_item),
                )

    @property
    def contracts(self) -> tuple[ServiceContract, ...]:
        return tuple(self._contracts)

    def registration(self) -> tuple[RegisteredService, ...]:
        return tuple(
            RegisteredService(
                name=service.name,
                methods=tuple(
                    RegisteredMethod(
                        name=method.name,
                        stream=method.stream,
                        balance=method.balance,
                    )
                    for method in service.methods
                ),
            )
            for service in self._contracts
        )

    async def serve_connection(self, connection: FrameConnection) -> None:
        session = _ServerSession(
            connection,
            self._methods,
            self.worker,
            access_log=self.access_log,
            log_level=self.log_level,
            colorize=self.colorize,
            logger=self.logger,
        )
        try:
            while True:
                frame = await connection.receive()
                await session.handle(frame)
        except ConnectionError:
            pass
        finally:
            await session.close()


class _ServerSession:
    def __init__(
        self,
        connection: FrameConnection,
        methods: dict[tuple[str, str], RuntimeMethod],
        worker: WorkerLoop,
        *,
        access_log: bool,
        log_level: str | int,
        colorize: bool,
        logger: RpcLogger,
    ) -> None:
        self.connection = connection
        self.methods = methods
        self.worker = worker
        self.access_log = access_log
        self.log_level = log_level
        self.colorize = colorize
        self.logger = logger
        self.calls: dict[str, _ServerCall] = {}

    async def handle(self, frame: Frame) -> None:
        if isinstance(frame, PingFrame):
            await self.connection.send(PongFrame(nonce=frame.nonce))
            return
        if isinstance(frame, CallOpenFrame):
            await self._open(frame)
            return
        call_id = getattr(frame, "call_id", None)
        if not isinstance(call_id, str):
            return
        call = self.calls.get(call_id)
        if call is None:
            return
        if isinstance(frame, StreamItemFrame):
            await call.receive_item(frame)
        elif isinstance(frame, StreamEndFrame):
            await call.receive_end(frame)
        elif isinstance(frame, StreamWindowFrame):
            await call.receive_window(frame)
        elif isinstance(frame, CallCancelFrame):
            call.cancel(frame.reason)

    async def _open(self, frame: CallOpenFrame) -> None:
        started_at = time.perf_counter()
        if frame.call_id in self.calls:
            try:
                await self.connection.send(
                    _error_frame(
                        frame.call_id,
                        ErrorCode.PROTOCOL_ERROR,
                        "Duplicate call_id",
                    )
                )
            finally:
                _log_access(
                    self.logger,
                    enabled=self.access_log,
                    call_id=frame.call_id,
                    service=frame.service,
                    method=frame.method,
                    started_at=started_at,
                    status=ErrorCode.PROTOCOL_ERROR.value,
                    level=self.log_level,
                    colorize=self.colorize,
                )
            return
        method = self.methods.get((frame.service, frame.method))
        if method is None:
            try:
                await self.connection.send(
                    _error_frame(
                        frame.call_id,
                        ErrorCode.METHOD_NOT_FOUND,
                        f"Unknown method {frame.service}.{frame.method}",
                    )
                )
            finally:
                _log_access(
                    self.logger,
                    enabled=self.access_log,
                    call_id=frame.call_id,
                    service=frame.service,
                    method=frame.method,
                    started_at=started_at,
                    status=ErrorCode.METHOD_NOT_FOUND.value,
                    level=self.log_level,
                    colorize=self.colorize,
                )
            return
        call = _ServerCall(self, frame, method)
        self.calls[frame.call_id] = call
        call.start()

    def remove(self, call: _ServerCall) -> None:
        if self.calls.get(call.call_id) is call:
            self.calls.pop(call.call_id, None)

    async def close(self) -> None:
        calls = tuple(self.calls.values())
        self.calls.clear()
        for call in calls:
            call.cancel("connection_closed")
        if calls:
            await asyncio.gather(
                *(call.wait() for call in calls),
                return_exceptions=True,
            )


class _ServerCall:
    def __init__(
        self,
        session: _ServerSession,
        open_frame: CallOpenFrame,
        method: RuntimeMethod,
    ) -> None:
        self.session = session
        self.connection = session.connection
        self.open_frame = open_frame
        self.call_id = open_frame.call_id
        self.method = method
        self.rpc_loop = asyncio.get_running_loop()
        self.input_queue: asyncio.Queue[Any] = asyncio.Queue()
        self.send_credit = CreditWindow()
        self.input_allowance = 0
        self.input_sequence = 0
        self.output_sequence = 0
        self.input_closed = False
        self.output_closed = False
        self.terminal = False
        self._terminal_lock = asyncio.Lock()
        self.logger = session.logger.bind(
            meshcall_service=method.service_name,
            meshcall_method=method.contract.name,
            meshcall_call_id=self.call_id,
        )
        self.started_at = time.perf_counter()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(
            self._run(),
            name=f"meshcall-server-{self.call_id}",
        )
        self._task.add_done_callback(lambda _: self.session.remove(self))

    async def wait(self) -> None:
        if self._task is not None:
            await self._task

    def cancel(self, reason: str) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel(reason)

    async def receive_item(self, frame: StreamItemFrame) -> None:
        if frame.direction != "client":
            await self._protocol_failure("Client sent a server-direction item")
            return
        if self.method.input_type is None or self.input_closed:
            await self._protocol_failure("Call does not accept input stream items")
            return
        if self.input_allowance <= 0:
            await self._protocol_failure("Input stream exceeded granted credit")
            return
        if frame.sequence != self.input_sequence:
            await self._protocol_failure(
                f"Expected client sequence {self.input_sequence}, got {frame.sequence}"
            )
            return
        self.input_allowance -= 1
        self.input_sequence += 1
        await self.input_queue.put(frame.payload)

    async def receive_end(self, frame: StreamEndFrame) -> None:
        if frame.direction != "client" or self.method.input_type is None:
            await self._protocol_failure("Invalid input stream end")
            return
        if not self.input_closed:
            self.input_closed = True
            await self.input_queue.put(_STREAM_END)

    async def receive_window(self, frame: StreamWindowFrame) -> None:
        if frame.direction != "server" or self.method.output_type is None:
            await self._protocol_failure("Invalid output stream window")
            return
        await self.send_credit.grant(frame.credit)

    async def input_consumed(self) -> None:
        if not self.input_closed and not self.terminal:
            self.input_allowance += 1
            await self.connection.send(
                StreamWindowFrame(
                    call_id=self.call_id,
                    direction="client",
                    credit=1,
                )
            )

    async def next_input_payload(self) -> Any:
        value = await self.input_queue.get()
        if value is not _STREAM_END:
            await self.input_consumed()
        return value

    async def send_output_payload(self, payload: Any) -> None:
        if self.method.output_type is None or self.output_closed:
            raise ProtocolError("Call output stream is closed")
        if self.terminal:
            raise MeshCallError(ErrorCode.CANCELLED, "Call is closed")
        await self.send_credit.acquire()
        if self.terminal:
            raise MeshCallError(ErrorCode.CANCELLED, "Call is closed")
        await self.connection.send(
            StreamItemFrame(
                call_id=self.call_id,
                direction="server",
                sequence=self.output_sequence,
                payload=payload,
            )
        )
        self.output_sequence += 1

    async def close_output(self) -> None:
        if self.method.output_type is not None and not self.output_closed:
            self.output_closed = True
            await self.connection.send(
                StreamEndFrame(call_id=self.call_id, direction="server")
            )

    async def _run(self) -> None:
        try:
            if self.method.input_type is not None:
                self.input_allowance = INITIAL_STREAM_CREDIT
                await self.connection.send(
                    StreamWindowFrame(
                        call_id=self.call_id,
                        direction="client",
                        credit=INITIAL_STREAM_CREDIT,
                    )
                )
            deadline = self.open_frame.deadline_unix_ms
            if deadline is None:
                payload = await self.session.worker.run(self._execute_worker())
            else:
                remaining = deadline / 1000 - time.time()
                if remaining <= 0:
                    raise TimeoutError
                async with asyncio.timeout(remaining):
                    payload = await self.session.worker.run(self._execute_worker())
            await self.close_output()
            await self._send_result(payload)
        except TimeoutError:
            await self._send_error(
                MeshCallError(ErrorCode.DEADLINE_EXCEEDED, "Call deadline exceeded")
            )
        except asyncio.CancelledError:
            await self._send_error(
                MeshCallError(ErrorCode.CANCELLED, "Call was cancelled")
            )
        except MeshCallError as exc:
            await self._send_error(exc)
        except ValidationError as exc:
            await self._send_error(MeshCallError(ErrorCode.INVALID_ARGUMENT, str(exc)))
        except Exception:
            self.logger.exception(  # noqa: PLE1205
                "Unhandled exception in {}.{}",
                self.method.service_name,
                self.method.contract.name,
            )
            await self._send_error(
                MeshCallError(ErrorCode.INTERNAL, "Internal service error")
            )
        finally:
            error = MeshCallError(ErrorCode.CANCELLED, "Call is closed")
            await self.send_credit.close(error)

    async def _execute_worker(self) -> Any:
        request = self.method.request_type.model_validate(self.open_frame.payload)
        kind = self.method.contract.stream
        if kind is StreamKind.UNARY:
            args, kwargs = self.method.method_binding.arguments(
                request,
                logger=self.logger,
            )
            result = await self.method.handler(*args, **kwargs)
        elif kind is StreamKind.SERVER:
            args, kwargs = self.method.method_binding.arguments(
                request,
                logger=self.logger,
            )
            iterator = self.method.handler(*args, **kwargs)
            async for item in iterator:
                await self._send_worker_output(item)
            result = None
        else:
            inbound = _ServerInputStream(self)
            if kind is StreamKind.CLIENT:
                args, kwargs = self.method.method_binding.arguments(
                    request,
                    inbound,
                    logger=self.logger,
                )
                result = await self.method.handler(*args, **kwargs)
            else:
                channel = _ServerDuplex(self)
                args, kwargs = self.method.method_binding.arguments(
                    request,
                    channel,
                    logger=self.logger,
                )
                result = await self.method.handler(*args, **kwargs)

        if self.method.response_type is None:
            return None
        validated = self.method.response_type.model_validate(result)
        return validated.model_dump(mode="json")

    async def _send_worker_output(self, item: BaseModel) -> None:
        output_type = self.method.output_type
        if output_type is None:
            raise ProtocolError("Call does not declare an output stream")
        validated = output_type.model_validate(item)
        payload = validated.model_dump(mode="json")
        await self._run_on_rpc(self.send_output_payload(payload))

    async def _run_on_rpc(self, coroutine: Any) -> Any:
        return await asyncio.wrap_future(submit_to_loop(self.rpc_loop, coroutine))

    async def _send_result(self, payload: Any) -> None:
        await self._send_terminal(
            CallResultFrame(call_id=self.call_id, payload=payload)
        )

    async def _send_error(self, error: MeshCallError) -> None:
        await self._send_terminal(
            CallErrorFrame(
                call_id=self.call_id,
                error=ErrorPayload(
                    code=error.code,
                    message=error.message,
                    retryable=error.retryable,
                    details=error.details,
                ),
            )
        )

    async def _send_terminal(self, frame: CallResultFrame | CallErrorFrame) -> None:
        async with self._terminal_lock:
            if self.terminal:
                return
            self.terminal = True
            try:
                await self.connection.send(frame)
            except ConnectionError:
                pass
            finally:
                _log_access(
                    self.logger,
                    enabled=self.session.access_log,
                    call_id=self.call_id,
                    service=self.method.service_name,
                    method=self.method.contract.name,
                    started_at=self.started_at,
                    status=(
                        "ok" if isinstance(frame, CallResultFrame) else frame.error.code
                    ),
                    level=self.session.log_level,
                    colorize=self.session.colorize,
                )

    async def _protocol_failure(self, message: str) -> None:
        await self._application_failure(ProtocolError(message))

    async def _application_failure(self, error: MeshCallError) -> None:
        await self._send_error(error)
        self.cancel(error.message)


class _ServerInputStream(RpcInputStream[BaseModel]):
    def __init__(self, call: _ServerCall) -> None:
        self.call = call

    def __aiter__(self) -> _ServerInputStream:
        return self

    async def __anext__(self) -> BaseModel:
        value = await asyncio.wrap_future(
            submit_to_loop(
                self.call.rpc_loop,
                self.call.next_input_payload(),
            )
        )
        if value is _STREAM_END:
            raise StopAsyncIteration
        if isinstance(value, BaseException):
            raise value
        input_type = self.call.method.input_type
        if input_type is None:
            raise ProtocolError("Call does not declare an input stream")
        return input_type.model_validate(value)


class _ServerDuplex(_ServerInputStream, RpcDuplex[BaseModel, BaseModel]):
    async def send(self, item: BaseModel) -> None:
        await self.call._send_worker_output(item)

    async def close_send(self) -> None:
        await self.call._run_on_rpc(self.call.close_output())


class RpcServer:
    def __init__(
        self,
        *,
        services: Sequence[object],
        driver: ServerDriver,
        access_log: bool = True,
        log_level: str | int = "INFO",
        colorize: bool = False,
        logger: RpcLogger | None = None,
    ) -> None:
        self._services = list(services)
        self.driver = driver
        self.access_log = access_log
        self.log_level = log_level
        self.colorize = colorize
        self.logger: RpcLogger = logger if logger is not None else default_logger
        self.owner_id = uuid.uuid4().hex
        self.state = ServerState.IDLE
        self._state_lock = asyncio.Lock()
        self._binding: DriverBinding | None = None
        self._runtime: ServerRuntime | None = None
        self._worker: WorkerLoop | None = None
        self._closed = asyncio.Event()
        self._closed.set()

    def include(self, service: object) -> None:
        if self.state is not ServerState.IDLE:
            raise DriverStateError("Services are frozen after server start begins")
        self._services.append(service)

    async def start(self) -> None:
        async with self._state_lock:
            if self.state is ServerState.RUNNING:
                return
            if self.state is not ServerState.IDLE:
                raise DriverStateError(f"Cannot start server in state {self.state}")
            self.state = ServerState.STARTING
            self._closed.clear()
            binding: DriverBinding | None = None
            worker: WorkerLoop | None = None
            try:
                binding = self.driver.acquire_binding(self.owner_id)
                self._binding = binding
                worker = WorkerLoop(name=f"meshcall-worker-{self.owner_id[:8]}")
                await worker.start()
                runtime = ServerRuntime(
                    tuple(self._services),
                    worker,
                    access_log=self.access_log,
                    log_level=self.log_level,
                    colorize=self.colorize,
                    logger=self.logger,
                )
                await self.driver.start(binding, runtime)
            except BaseException:
                if worker is not None:
                    await worker.stop()
                if binding is not None:
                    self.driver.release_binding(binding)
                self._binding = None
                self.state = ServerState.IDLE
                self._closed.set()
                raise
            self._runtime = runtime
            self._worker = worker
            self.state = ServerState.RUNNING

    async def stop(self) -> None:
        async with self._state_lock:
            if self.state is ServerState.IDLE:
                return
            if self.state is not ServerState.RUNNING or self._binding is None:
                raise DriverStateError(f"Cannot stop server in state {self.state}")
            self.state = ServerState.STOPPING
            binding = self._binding
            worker = self._worker
            try:
                await self.driver.stop(binding)
            finally:
                try:
                    if worker is not None:
                        await worker.stop()
                finally:
                    self.driver.release_binding(binding)
                    self._binding = None
                    self._runtime = None
                    self._worker = None
                    self.state = ServerState.IDLE
                    self._closed.set()

    async def wait_closed(self) -> None:
        await self._closed.wait()

    async def run(self) -> None:
        await self.start()
        await self.wait_closed()


def _log_access(
    logger: Any,
    *,
    enabled: bool,
    call_id: str,
    service: str,
    method: str,
    started_at: float,
    status: str,
    level: str | int,
    colorize: bool,
) -> None:
    if not enabled:
        return
    message = (
        "<level>RPC call {}.{} status={} duration_ms={:.2f} call_id={}</level>"
        if colorize
        else "RPC call {}.{} status={} duration_ms={:.2f} call_id={}"
    )
    logger.opt(colors=colorize).log(
        level,
        message,
        service,
        method,
        status,
        (time.perf_counter() - started_at) * 1000,
        call_id,
    )


def _resolve_optional_type(ref: TypeRef | None) -> type[BaseModel] | None:
    return _resolve_type(ref) if ref is not None else None


def _resolve_type(ref: TypeRef) -> type[BaseModel]:
    value: Any = importlib.import_module(ref.module)
    for part in ref.qualname.split("."):
        value = getattr(value, part)
    if not inspect.isclass(value) or not issubclass(value, BaseModel):
        raise ContractError(f"Contract type {ref.module}.{ref.qualname} is invalid")
    return value


def _error_frame(
    call_id: str,
    code: ErrorCode,
    message: str,
) -> CallErrorFrame:
    return CallErrorFrame(
        call_id=call_id,
        error=ErrorPayload(code=code, message=message),
    )

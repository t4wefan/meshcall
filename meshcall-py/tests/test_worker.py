from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import AsyncIterator

import pytest
from pydantic import BaseModel

from meshcall import RpcServer, method, service
from meshcall.client import ClientBase
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
from meshcall.errors import ErrorCode, MeshCallError
from meshcall.streams import RpcServerStream


class WorkRequest(BaseModel):
    delay: float


class WorkResult(BaseModel):
    worker_thread_id: int


blocking_started = threading.Event()


@service(name="test.v1.WorkerIsolationService")
class WorkerIsolationService:
    @method()
    async def block(self, delay: float) -> WorkResult:
        blocking_started.set()
        time.sleep(delay)  # noqa: ASYNC251 - intentional regression case
        return WorkResult(worker_thread_id=threading.get_ident())

    @method()
    async def identify(self, delay: float) -> WorkResult:
        return WorkResult(worker_thread_id=threading.get_ident())


class WorkerIsolationClient(ClientBase):
    service_name = "test.v1.WorkerIsolationService"

    async def block(
        self,
        request: WorkRequest,
        *,
        timeout: float,
    ) -> WorkResult:
        return await self.call_unary(
            self.service_name,
            "block",
            request,
            WorkResult,
            timeout=timeout,
        )

    async def identify(self, request: WorkRequest) -> WorkResult:
        return await self.call_unary(
            self.service_name,
            "identify",
            request,
            WorkResult,
        )


class PushRequest(BaseModel):
    value: int


class LoopBoundResult(BaseModel):
    thread_id: int
    queued: int


@service(name="test.v1.LoopBoundService")
class LoopBoundService:
    """Uses a Queue and a Lock created on the application's loop."""

    def __init__(self) -> None:
        self.queue: asyncio.Queue[int] = asyncio.Queue()
        self.lock = asyncio.Lock()

    @method()
    async def push(self, value: int) -> LoopBoundResult:
        async with self.lock:
            await self.queue.put(value)
        return LoopBoundResult(
            thread_id=threading.get_ident(), queued=self.queue.qsize()
        )

    @method.server_stream()
    async def drain(self, value: int) -> AsyncIterator[LoopBoundResult]:
        for _ in range(value):
            item = await self.queue.get()
            yield LoopBoundResult(thread_id=threading.get_ident(), queued=item)


class LoopBoundClient(ClientBase):
    service_name = "test.v1.LoopBoundService"

    async def push(self, value: int) -> LoopBoundResult:
        return await self.call_unary(
            self.service_name, "push", PushRequest(value=value), LoopBoundResult
        )

    def drain(self, value: int) -> RpcServerStream[LoopBoundResult]:
        return self.call_server_stream(
            self.service_name, "drain", PushRequest(value=value), LoopBoundResult
        )


async def test_handlers_run_on_the_loop_that_started_the_server() -> None:
    service_instance = LoopBoundService()
    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(services=[service_instance], driver=driver)
    await server.start()
    client = LoopBoundClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}")
    )
    try:
        first = await client.push(1)
        second = await client.push(2)
        assert first.thread_id == threading.get_ident()
        assert second.queued == 2
        drained = [item async for item in client.drain(2)]
        assert [item.queued for item in drained] == [1, 2]
        assert {item.thread_id for item in drained} == {threading.get_ident()}
        # The application loop sees what the handlers did, without a bridge.
        assert service_instance.queue.empty()
    finally:
        await client.stop()
        await server.stop()


async def test_blocking_handler_cannot_block_rpc_loop() -> None:
    rpc_thread_id = threading.get_ident()
    blocking_started.clear()
    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(
        services=[WorkerIsolationService],
        driver=driver,
        isolate_handlers=True,
    )
    await server.start()
    client = WorkerIsolationClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}")
    )
    try:
        blocked_call = asyncio.create_task(
            client.block(WorkRequest(delay=0.4), timeout=0.05)
        )
        assert await asyncio.to_thread(blocking_started.wait, 0.2)

        tick_started = asyncio.get_running_loop().time()
        await asyncio.sleep(0.01)
        assert asyncio.get_running_loop().time() - tick_started < 0.1

        with pytest.raises(MeshCallError) as error:
            await blocked_call
        assert error.value.code == ErrorCode.DEADLINE_EXCEEDED

        await asyncio.sleep(0.4)
        result = await client.identify(WorkRequest(delay=0))
        assert result.worker_thread_id != rpc_thread_id
    finally:
        await client.stop()
        await server.stop()

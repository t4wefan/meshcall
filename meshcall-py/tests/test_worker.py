from __future__ import annotations

import asyncio
import threading
import time

import pytest
from pydantic import BaseModel

from meshcall import RpcServer, method, service
from meshcall.client import ClientBase
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
from meshcall.errors import ErrorCode, MeshCallError
from meshcall.worker import cancel_and_wait


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


async def test_blocking_handler_cannot_block_rpc_loop() -> None:
    rpc_thread_id = threading.get_ident()
    blocking_started.clear()
    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(services=[WorkerIsolationService], driver=driver)
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


async def test_cancel_and_wait_keeps_the_callers_own_cancellation() -> None:
    # A supervisor cancelled while it waits for a child to finish must stop:
    # `suppress(CancelledError)` around that wait swallowed the supervisor's
    # cancellation, so it reconnected forever and server.stop() never returned.
    async def slow_to_finish() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0.05)
            raise

    child = asyncio.create_task(slow_to_finish())
    await asyncio.sleep(0)
    after_wait = asyncio.Event()

    async def supervisor() -> None:
        await cancel_and_wait(child)
        after_wait.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(supervisor())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert not after_wait.is_set()
    assert child.cancelled()


async def test_cancel_and_wait_ignores_only_the_tasks_own_ending() -> None:
    async def fails() -> None:
        raise MeshCallError(ErrorCode.UNAVAILABLE, "gone")

    failed = asyncio.create_task(fails())
    await asyncio.sleep(0)
    await cancel_and_wait(failed, MeshCallError)
    sleeper = asyncio.create_task(asyncio.sleep(10))
    await cancel_and_wait(sleeper)
    assert sleeper.cancelled()
    broken = asyncio.create_task(fails())
    await asyncio.sleep(0)
    with pytest.raises(MeshCallError):
        await cancel_and_wait(broken)

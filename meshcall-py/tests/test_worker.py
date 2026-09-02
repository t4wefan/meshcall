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


class WorkRequest(BaseModel):
    delay: float


class WorkResult(BaseModel):
    worker_thread_id: int


blocking_started = threading.Event()


@service(name="test.v1.WorkerIsolationService")
class WorkerIsolationService:
    @method()
    async def block(self, request: WorkRequest) -> WorkResult:
        blocking_started.set()
        time.sleep(request.delay)  # noqa: ASYNC251 - intentional regression case
        return WorkResult(worker_thread_id=threading.get_ident())

    @method()
    async def identify(self, request: WorkRequest) -> WorkResult:
        return WorkResult(worker_thread_id=threading.get_ident())


class WorkerIsolationClient(ClientBase):
    service_name = "test.v1.WorkerIsolationService"

    async def block(
        self,
        request: WorkRequest,
        *,
        timeout: float,
    ) -> WorkResult:
        return await self._unary(
            self.service_name,
            "block",
            request,
            WorkResult,
            timeout=timeout,
        )

    async def identify(self, request: WorkRequest) -> WorkResult:
        return await self._unary(
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

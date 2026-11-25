from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine
from concurrent.futures import Future as ConcurrentFuture
from typing import Any, TypeVar

from loguru import logger

ResultT = TypeVar("ResultT")


class InlineLoop:
    """Runs handlers on the loop that started the server (the default).

    Handlers then share that loop with the application's own clients, pools
    and locks, which asyncio binds to the loop that created them.
    """

    running = True

    async def start(self) -> None:
        return None

    async def run(self, coroutine: Coroutine[Any, Any, ResultT]) -> ResultT:
        return await coroutine

    async def stop(self) -> None:
        return None


class WorkerLoop:
    """Owns a persistent asyncio loop in a dedicated daemon thread.

    Opt-in (``isolate_handlers=True``): a handler that blocks synchronously
    then cannot stall frame I/O, at the cost of every handler running on a
    loop the application's own async resources do not belong to.
    """

    def __init__(self, *, name: str, shutdown_timeout: float = 1.0) -> None:
        self.name = name
        self.shutdown_timeout = shutdown_timeout
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

    @property
    def running(self) -> bool:
        return self._loop is not None and self._thread is not None

    async def start(self) -> None:
        if self.running:
            return
        rpc_loop = asyncio.get_running_loop()
        ready: asyncio.Future[None] = rpc_loop.create_future()
        thread = threading.Thread(
            target=self._thread_main,
            args=(rpc_loop, ready),
            name=self.name,
            daemon=True,
        )
        self._thread = thread
        thread.start()
        try:
            await ready
        except BaseException:
            self._thread = None
            raise

    async def run(self, coroutine: Coroutine[Any, Any, ResultT]) -> ResultT:
        loop = self._loop
        if loop is None:
            coroutine.close()
            raise RuntimeError("Worker loop is not running")
        try:
            future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        except BaseException:
            coroutine.close()
            raise
        return await asyncio.wrap_future(future)

    async def stop(self) -> None:
        loop, self._loop = self._loop, None
        thread, self._thread = self._thread, None
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        if thread is None:
            return
        await asyncio.to_thread(thread.join, self.shutdown_timeout)
        if thread.is_alive():
            logger.warning(
                "Worker loop {} did not stop within {} seconds; "
                "blocked user code is being abandoned",
                self.name,
                self.shutdown_timeout,
            )

    def _thread_main(
        self,
        rpc_loop: asyncio.AbstractEventLoop,
        ready: asyncio.Future[None],
    ) -> None:
        worker_loop: asyncio.AbstractEventLoop | None = None
        try:
            worker_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(worker_loop)
            self._loop = worker_loop
            rpc_loop.call_soon_threadsafe(_resolve_ready, ready, None)
            worker_loop.run_forever()
        except Exception as exc:  # noqa: BLE001
            rpc_loop.call_soon_threadsafe(_resolve_ready, ready, exc)
        finally:
            if worker_loop is not None:
                pending = asyncio.all_tasks(worker_loop)
                for task in pending:
                    task.cancel()
                if pending:
                    worker_loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True)
                    )
                worker_loop.close()
            asyncio.set_event_loop(None)


HandlerLoop = InlineLoop | WorkerLoop


async def run_on_loop(
    loop: asyncio.AbstractEventLoop,
    coroutine: Coroutine[Any, Any, ResultT],
) -> ResultT:
    """Await a coroutine on its owning loop, hopping threads only if needed."""

    if asyncio.get_running_loop() is loop:
        return await coroutine
    future: ConcurrentFuture[ResultT]
    try:
        future = asyncio.run_coroutine_threadsafe(coroutine, loop)
    except BaseException:
        coroutine.close()
        raise
    return await asyncio.wrap_future(future)


def _resolve_ready(
    future: asyncio.Future[None],
    error: BaseException | None,
) -> None:
    if future.done():
        return
    if error is None:
        future.set_result(None)
    else:
        future.set_exception(error)

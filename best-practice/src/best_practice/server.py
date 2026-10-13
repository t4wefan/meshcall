"""Run the best-practice Python service as a local WebSocket server."""

from __future__ import annotations

import asyncio
import os
import signal

from meshcall import RpcServer
from meshcall.drivers import WebSocketDirectServerDriver

from .service import LlmService


async def serve_forever(
    host: str = "127.0.0.1",
    port: int = 8765,
    *,
    access_log: bool = True,
    log_level: str = "INFO",
    colorize: bool = False,
) -> None:
    driver = WebSocketDirectServerDriver(host=host, port=port)
    server = RpcServer(
        services=[LlmService()],
        driver=driver,
        access_log=access_log,
        log_level=log_level,
        colorize=colorize,
    )
    await server.start()
    print(
        f"MeshCall LLM server listening on ws://{host}:{driver.bound_port}", flush=True
    )
    # SIGTERM/SIGINT drain first: stop taking calls, let streams finish, then exit.
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    try:
        await stop.wait()
    finally:
        await server.stop(drain=True, drain_timeout=30)


def main() -> None:
    port = int(os.environ.get("MESHCALL_BEST_PRACTICE_PORT", "8765"))
    access_log = _env_bool("MESHCALL_ACCESS_LOG", default=True)
    colorize = _env_bool("MESHCALL_LOG_COLOR", default=False)
    log_level = os.environ.get("MESHCALL_LOG_LEVEL", "INFO")
    asyncio.run(
        serve_forever(
            port=port,
            access_log=access_log,
            log_level=log_level,
            colorize=colorize,
        )
    )


def _env_bool(name: str, *, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}

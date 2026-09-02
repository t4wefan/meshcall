"""Run the best-practice Python service as a local WebSocket server."""

from __future__ import annotations

import asyncio
import os

from meshcall import RpcServer
from meshcall.drivers import WebSocketDirectServerDriver

from .service import LlmService


async def serve_forever(host: str = "127.0.0.1", port: int = 8765) -> None:
    driver = WebSocketDirectServerDriver(host=host, port=port)
    server = RpcServer(services=[LlmService()], driver=driver)
    await server.start()
    print(f"MeshCall LLM server listening on ws://{host}:{driver.bound_port}", flush=True)
    try:
        await asyncio.Event().wait()
    finally:
        await server.stop()


def main() -> None:
    port = int(os.environ.get("MESHCALL_BEST_PRACTICE_PORT", "8765"))
    try:
        asyncio.run(serve_forever(port=port))
    except KeyboardInterrupt:
        pass

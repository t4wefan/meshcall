from __future__ import annotations

import asyncio

from meshcall import RpcServer
from meshcall.drivers import WebSocketRouterServerDriver
from router_demo.service import CounterService


async def main() -> None:
    server = RpcServer(
        services=[CounterService],
        driver=WebSocketRouterServerDriver(
            "ws://127.0.0.1:8765",
            instance_id="counter-1",
        ),
    )
    await server.start()
    try:
        await asyncio.Event().wait()
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())

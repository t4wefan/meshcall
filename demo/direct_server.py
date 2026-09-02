from __future__ import annotations

import asyncio

from demo.service import CounterService
from meshcall import RpcServer
from meshcall.drivers import WebSocketDirectServerDriver


async def main() -> None:
    server = RpcServer(
        services=[CounterService],
        driver=WebSocketDirectServerDriver(host="127.0.0.1", port=8765),
    )
    await server.start()
    try:
        await asyncio.Event().wait()
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())

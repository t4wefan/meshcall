from __future__ import annotations

import asyncio

from demo.router.generated_client import CounterServiceClient
from meshcall.drivers import WebSocketClientDriver


async def main() -> None:
    client = CounterServiceClient(
        WebSocketClientDriver("ws://127.0.0.1:8765")
    )
    async with client:
        stream = client.count(10, timeout=5)
        async for item in stream:
            print(item.value)


if __name__ == "__main__":
    asyncio.run(main())

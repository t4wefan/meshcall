"""Call the quickstart service through the Router."""

from __future__ import annotations

import asyncio

from quickstart_demo.generated_client import CounterServiceClient


async def main() -> None:
    # connect() resolves the Router like run() does; no driver is built by hand.
    async with CounterServiceClient.connect() as client:
        async for item in client.count(10):
            print(item.value)


if __name__ == "__main__":
    asyncio.run(main())

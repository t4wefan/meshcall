"""Call the quickstart service through the Router."""

from __future__ import annotations

import asyncio

from meshcall import Mesh, Router
from quickstart_demo.generated_client import CounterServiceClient


async def main() -> None:
    async with Mesh(Router()) as mesh:
        client = CounterServiceClient(mesh)
        async for item in client.count(10):
            print(item.value)


if __name__ == "__main__":
    asyncio.run(main())

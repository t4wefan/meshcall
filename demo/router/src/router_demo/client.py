"""Inspect the Router's registry and call the service through it."""

from __future__ import annotations

import asyncio

from meshcall import Mesh, Router
from router_demo.generated_client import CounterServiceClient


async def main() -> None:
    async with Mesh(Router("ws://127.0.0.1:8765")) as mesh:
        for service in await mesh.registry.list():
            print(service.name)
        client = CounterServiceClient(mesh)
        async for item in client.with_options(timeout=5).count(10):
            print(item.value)


if __name__ == "__main__":
    asyncio.run(main())

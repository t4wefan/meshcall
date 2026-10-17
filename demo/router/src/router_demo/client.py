"""Inspect the Router's registry and call the service through it."""

from __future__ import annotations

import asyncio

from meshcall import Router
from router_demo.generated_client import CounterServiceClient


async def main() -> None:
    router = await Router.connect("ws://127.0.0.1:8765")
    try:
        for service in await router.list_services():
            print(service.name)
        client = CounterServiceClient.connect(router)
        async for item in client.with_options(timeout=5).count(10):
            print(item.value)
    finally:
        await router.close()


if __name__ == "__main__":
    asyncio.run(main())

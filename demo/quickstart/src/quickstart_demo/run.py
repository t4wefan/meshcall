"""Run the smallest complete MeshCall demo: a service, a Router and a client."""

from __future__ import annotations

import asyncio

from meshcall import Router, serve
from quickstart_demo.generated_client import CounterServiceClient
from quickstart_demo.service import CounterService


async def run_demo() -> None:
    """Serve the service with an embedded Router, then call it."""
    async with serve(
        CounterService(),
        router=Router("ws://127.0.0.1:0"),
        access_log=False,
    ) as server:
        async with CounterServiceClient.connect(server.url) as client:
            values = [item.value async for item in client.count(5)]
        print(f"routed count={values}")


def main() -> None:
    asyncio.run(run_demo())

"""Run the smallest complete MeshCall demo."""

from __future__ import annotations

import asyncio

from meshcall import RpcServer
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
from quickstart_demo.generated_client import CounterServiceClient
from quickstart_demo.service import CounterService


async def run_demo() -> None:
    """Start a short-lived direct server and call its generated client."""
    server_driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(services=[CounterService], driver=server_driver)
    await server.start()

    client = CounterServiceClient(
        WebSocketClientDriver(
            f"ws://127.0.0.1:{server_driver.bound_port}"
        )
    )
    try:
        async with client:
            values = [item.value async for item in client.count(5)]
        print(f"direct count={values}")
    finally:
        await server.stop()


def main() -> None:
    asyncio.run(run_demo())

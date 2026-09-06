"""Run a complete Router demo in short-lived Python and Go processes."""

from __future__ import annotations

import asyncio

from meshcall import RpcServer, WebSocketRouter
from meshcall.drivers import WebSocketClientDriver, WebSocketRouterServerDriver
from router_demo.generated_client import CounterServiceClient
from router_demo.service import CounterService


async def run_demo() -> None:
    """Start Router, service, and generated client, then shut them down."""
    router = WebSocketRouter(host="127.0.0.1", port=0)
    await router.start()

    server = RpcServer(
        services=[CounterService],
        driver=WebSocketRouterServerDriver(
            f"ws://127.0.0.1:{router.bound_port}",
            instance_id="counter-demo-1",
        ),
    )
    await server.start()

    client = CounterServiceClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{router.bound_port}")
    )
    try:
        async with client:
            values = [item.value async for item in client.count(5)]
        print(f"routed count={values}")
    finally:
        await server.stop()
        await router.stop()


def main() -> None:
    asyncio.run(run_demo())

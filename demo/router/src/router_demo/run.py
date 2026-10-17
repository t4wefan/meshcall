"""Run an explicit Router, a service registered with it, and a client."""

from __future__ import annotations

import asyncio

from meshcall import Router, WebSocketRouter, serve
from router_demo.generated_client import CounterServiceClient
from router_demo.service import CounterService


async def run_demo() -> None:
    """Start a standalone Router, register the service, inspect and call it."""
    async with WebSocketRouter(host="127.0.0.1", port=0) as router_process:
        url = f"ws://127.0.0.1:{router_process.bound_port}"
        async with (
            serve(
                CounterService(),
                router=Router(url),
                instance_id="counter-demo-1",
                access_log=False,
            ),
            await Router.connect(url) as router,
        ):
            services = await router.list_services()
            print(f"registry={[service.name for service in services]}")
            client = CounterServiceClient.connect(router)
            values = [item.value async for item in client.count(5)]
            print(f"routed count={values}")


def main() -> None:
    asyncio.run(run_demo())

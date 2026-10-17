"""Run an explicit LocalRouter, a service registered with it, and a client."""

from __future__ import annotations

import asyncio

from meshcall import LocalRouter, Mesh, Router
from router_demo.generated_client import CounterServiceClient
from router_demo.service import CounterService


async def run_demo() -> None:
    """Start a standalone Router, register the service, inspect and call it."""
    async with LocalRouter(host="127.0.0.1", port=0) as local_router:
        server_mesh = Mesh(local_router).mount(
            CounterService(), instance_id="counter-demo-1"
        )
        async with (
            server_mesh.serve(access_log=False),
            Mesh(Router(local_router.url)) as mesh,
        ):
            services = await mesh.registry.list()
            print(f"registry={[service.name for service in services]}")
            client = CounterServiceClient(mesh)
            values = [item.value async for item in client.count(5)]
            print(f"routed count={values}")


def main() -> None:
    asyncio.run(run_demo())

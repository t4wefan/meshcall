"""Run the smallest complete MeshCall demo: a service, a LocalRouter and a client."""

from __future__ import annotations

import asyncio

from meshcall import LocalRouter, Mesh, Router
from quickstart_demo.generated_client import CounterServiceClient
from quickstart_demo.service import CounterService


async def run_demo() -> None:
    """Start a LocalRouter, serve the service, then call it."""
    async with LocalRouter(port=0) as local_router:
        server_mesh = Mesh(local_router).mount(CounterService())
        async with (
            server_mesh.serve(access_log=False),
            Mesh(Router(local_router.url)) as mesh,
        ):
            client = CounterServiceClient(mesh)
            values = [item.value async for item in client.count(5)]
            print(f"routed count={values}")


def main() -> None:
    asyncio.run(run_demo())

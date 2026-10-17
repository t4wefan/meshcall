"""Run the complete MeshCall showcase against a LocalRouter."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from meshcall import LocalRouter, Mesh, Router
from showcase_demo.generated_client import (
    NumberItem,
    ShowcaseServiceClient,
    WelcomeRequest,
)
from showcase_demo.service import ShowcaseService


async def number_items() -> AsyncIterator[NumberItem]:
    for value in (2, 3, 5):
        yield NumberItem(value=value)


async def run_demo() -> None:
    """Serve the service, then call it with one generated client."""
    async with LocalRouter(port=0) as local_router:
        server_mesh = Mesh(local_router).mount(ShowcaseService(greeting_prefix="Hi"))
        async with (
            server_mesh.serve(access_log=False),
            Mesh(Router(local_router.url)) as mesh,
        ):
            client = ShowcaseServiceClient(mesh).with_options(
                metadata={"x-caller-id": "showcase"},
                timeout=10,
            )
            print("0. Router registry")
            services = await mesh.registry.list()
            print(f"   services={[service.name for service in services]}")

            print("1. query with expanded parameters")
            greeting = await client.greet("MeshCall", repeat=2)
            print(f"   {greeting.message}")

            print("2. query with a request object")
            welcome = await client.welcome(
                WelcomeRequest(name="Python", tags=["typed", "easy"])
            )
            print(f"   {welcome.message} tags={welcome.tags}")

            print("3. mutating call with metadata injection")
            visit = await client.visit("MeshCall")
            print(f"   visits={visit.visits} caller={visit.caller}")

            print("4. server stream")
            values = [item.value async for item in client.count(3, 7)]
            print(f"   received={values}")

            print("5. client stream")
            total = await client.sum_values(10, number_items())
            print(f"   total={total.total}")


def main() -> None:
    asyncio.run(run_demo())

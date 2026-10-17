"""Run the complete MeshCall showcase against an embedded Router."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from meshcall import Router, serve
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
    async with serve(
        ShowcaseService(greeting_prefix="Hi"),
        router=Router("ws://127.0.0.1:0"),
        access_log=False,
    ) as server:
        router = await Router.connect(server.url)
        client = (ShowcaseServiceClient.connect(router)).with_options(
            metadata={"x-caller-id": "showcase"},
            timeout=10,
        )
        try:
            print("0. Router registry")
            services = await router.list_services()
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
        finally:
            await router.close()


def main() -> None:
    asyncio.run(run_demo())

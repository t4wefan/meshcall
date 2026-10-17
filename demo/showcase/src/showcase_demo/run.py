"""Run the complete MeshCall showcase without a separately managed server."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from meshcall import RpcServer
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
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
    """Start a short-lived local server and exercise the recommended API."""
    server_driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(
        services=[ShowcaseService(greeting_prefix="Hi")],
        driver=server_driver,
    )
    await server.start()

    client = ShowcaseServiceClient(
        WebSocketClientDriver(
            f"ws://127.0.0.1:{server_driver.bound_port}"
        )
    )
    try:
        print("1. default unary with expanded parameters")
        greeting = await client.greet("MeshCall", repeat=2)
        print(f"   {greeting.message}")

        print("2. unary with a request object")
        welcome = await client.welcome(
            WelcomeRequest(name="Python", tags=["typed", "easy"])
        )
        print(f"   {welcome.message} tags={welcome.tags}")

        print("3. server stream")
        values = [item.value async for item in client.count(3, 7)]
        print(f"   received={values}")

        print("4. client stream")
        total = await client.sum_values(10, number_items())
        print(f"   total={total.total}")

        print("5. generated client models and signatures")
        print("   all request/response payloads were validated by Pydantic")
        print("   all four methods came from the generated client module")
    finally:
        await client.stop()
        await server.stop()


def main() -> None:
    asyncio.run(run_demo())

"""Several Routers (docs/protocol.md → Several Routers).

conformance-scenario: routers.several
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from pydantic import BaseModel

from meshcall import (
    LocalRouter,
    Mesh,
    Router,
    RpcContext,
    RpcDuplex,
    RpcInputStream,
    UnavailableError,
    method,
    service,
)


class Empty(BaseModel):
    pass


class Count(BaseModel):
    n: int


class Where(BaseModel):
    instance: str


@service(name="test.v1.Box", targeted=True)
class Box:
    """A box on one of several Routers."""

    @method(read_only=True)
    async def whoami(self, ctx: RpcContext) -> Where:
        """Name this instance."""
        return Where(instance=ctx.instance_id)

    @method.server_stream(read_only=True)
    async def count(self, n: int) -> AsyncIterator[Count]:
        """Count up to n."""
        for i in range(n):
            yield Count(n=i)

    @method.client_stream()
    async def sum(self, n: int, items: RpcInputStream[Count]) -> Count:
        """Add the items to n."""
        total = n
        async for item in items:
            total += item.n
        return Count(n=total)

    @method.duplex(reason="answers as items arrive")
    async def double(self, channel: RpcDuplex[Count, Count]) -> None:
        """Double each item."""
        async for item in channel:
            await channel.send(Count(n=item.n * 2))


@service(name="test.v1.Plain")
class Plain:
    """A service registered on one Router only."""

    @method(read_only=True)
    async def hello(self, ctx: RpcContext) -> Where:
        """Answer."""
        return Where(instance=ctx.instance_id)


async def counts(*values: int) -> AsyncIterator[Count]:
    for n in values:
        yield Count(n=n)


async def test_calls_reach_the_router_their_instance_or_service_is_on() -> None:
    async with LocalRouter(port=0) as a, LocalRouter(port=0) as b:
        # Registered on b only: a answers instance_unknown / no_instances.
        box = Mesh(Router(b.url)).mount(
            Box(), Plain(), instance_id="box-b", accept_tunnels=True
        )
        async with box.serve(access_log=False):
            routers = [a.url, b.url]

            def fresh() -> Mesh:
                return Mesh(Router(routers))

            async with fresh() as mesh:
                assert mesh.routers == tuple(routers)
                for _ in range(2):  # the second from memory
                    assert await mesh.call_unary(
                        "test.v1.Box", "whoami", Empty(), Where, instance="box-b"
                    ) == Where(instance="box-b")
                assert await mesh.call_unary(
                    "test.v1.Plain", "hello", Empty(), Where
                ) == Where(instance="box-b")
                with pytest.raises(UnavailableError) as unknown:
                    await mesh.call_unary(
                        "test.v1.Box", "whoami", Empty(), Where, instance="nowhere"
                    )
                assert unknown.value.reason == "instance_unknown"

            # Every call shape moves on from a Router that did not route it,
            # with nothing lost from what the caller already handed over.
            async with fresh() as mesh:
                stream = mesh.call_server_stream(
                    "test.v1.Box", "count", Count(n=3), Count, instance="box-b"
                )
                assert [item.n async for item in stream] == [0, 1, 2]
            async with fresh() as mesh:
                assert await mesh.call_client_stream(
                    "test.v1.Box",
                    "sum",
                    Count(n=1),
                    counts(2, 3, 4),
                    Count,
                    Count,
                    instance="box-b",
                ) == Count(n=10)
            async with fresh() as mesh:
                call = mesh.call_duplex(
                    "test.v1.Box", "double", Empty(), Count, Count, instance="box-b"
                )
                await call.send(Count(n=1))
                await call.send(Count(n=2))
                await call.close_send()
                assert [item.n async for item in call] == [2, 4]

            # What belongs to one Router is asked of that Router.
            async with fresh() as issuer:
                with pytest.raises(TypeError, match=r"mesh\.at\(router\)"):
                    await issuer.registry.list()
                assert await issuer.at(a.url).registry.list() == []
                assert any(
                    s.name == "test.v1.Box"
                    for s in await issuer.at(a.url).at(b.url).registry.list()
                )
                # A tunnel is opened where the instance is, and revoked everywhere.
                opened = await issuer.tunnels.open(
                    instance="box-b",
                    scopes=["test.v1.Box"],
                    principal="user:alice",
                    ttl_seconds=60,
                )
                assert opened.router == b.url
                async with Mesh(
                    Router(opened.router), tunnel=opened.credential
                ) as client:
                    assert await client.call_unary(
                        "test.v1.Box", "whoami", Empty(), Where
                    ) == Where(instance="box-b")
                assert await issuer.tunnels.revoke(opened.tunnel_id) is True
                assert await issuer.tunnels.revoke(opened.tunnel_id) is False

            # A Router that cannot be reached is skipped too.
            await a.stop()
            mesh = fresh()
            try:
                assert await mesh.call_unary(
                    "test.v1.Box", "whoami", Empty(), Where, instance="box-b"
                ) == Where(instance="box-b")
            finally:
                await mesh.close()


async def test_an_instance_registers_with_every_router_asking_its_source() -> None:
    async with LocalRouter(port=0) as a, LocalRouter(port=0) as b:
        asked: set[str] = set()

        def source(router: str) -> str:
            asked.add(router)
            return f"mt_{len(router)}"

        box = Mesh(Router([a.url, b.url]), token=source).mount(
            Box(), instance_id="box-ab"
        )
        async with box.serve(access_log=False):
            assert asked == {a.url, b.url}
            for router in (a, b):
                async with Mesh(router) as caller:
                    assert await caller.call_unary(
                        "test.v1.Box", "whoami", Empty(), Where, instance="box-ab"
                    ) == Where(instance="box-ab")

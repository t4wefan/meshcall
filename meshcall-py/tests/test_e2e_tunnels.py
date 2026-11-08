"""End-to-end sealed tunnels through a real Router (docs/protocol.md →
End-to-end encryption)."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest
from pydantic import BaseModel

from meshcall import (
    LocalRouter,
    Mesh,
    Router,
    RpcContext,
    RpcInputStream,
    generate_e2e_key,
    method,
    service,
)
from meshcall.errors import MeshCallError, PermissionDeniedError, UnauthenticatedError


class Num(BaseModel):
    value: int


class Empty(BaseModel):
    pass


class Sealed(BaseModel):
    sealed: bool


@service(name="test.v1.Box", targeted=True)
class Box:
    """A box reached end to end."""

    @method(read_only=True)
    async def double(self, value: int) -> Num:
        """Double a number."""
        return Num(value=value * 2)

    @method(read_only=True)
    async def count(self, value: int) -> AsyncIterator[Num]:
        """Yield 0..value-1."""
        for i in range(value):
            yield Num(value=i)

    @method(idempotent=True)
    async def total(self, value: int, items: RpcInputStream[Num]) -> Num:
        """Add the items to value."""
        total = value
        async for item in items:
            total += item.value
        return Num(value=total)

    @method(read_only=True)
    async def seen(self, ctx: RpcContext) -> Sealed:
        """Whether the call came through a sealed tunnel."""
        return Sealed(sealed=ctx.tunnel is not None and ctx.tunnel.sealed)

    @method(read_only=True)
    async def fail(self, value: int) -> Num:
        """Fail with an application error."""
        raise MeshCallError("app.nope", "secret 42", details={"value": value})


async def test_sealed_calls_through_a_router() -> None:
    keys = generate_e2e_key()
    other = generate_e2e_key()

    def instance(router: LocalRouter) -> Mesh:
        return Mesh(router).mount(
            Box(),
            instance_id="box-a",
            accept_tunnels=True,
            e2e_private_key=keys.private_key,
        )

    async with LocalRouter(port=0) as router, Mesh(router) as issuer:
        first = contextlib.AsyncExitStack()
        await first.enter_async_context(instance(router).serve(access_log=False))
        opened = await issuer.tunnels.open(
            instance="box-a",
            scopes=["test.v1.Box"],
            principal="user:alice",
            e2e_key=keys.public_key,
        )
        assert opened.e2e_key == keys.public_key
        with pytest.raises(PermissionDeniedError) as refused:
            await issuer.tunnels.open(
                instance="box-a",
                scopes=["test.v1.Box"],
                principal="p",
                e2e_key=other.public_key,
            )
        assert refused.value.reason == "tunnel_rejected"
        url = Router(router.url)

        async with Mesh(url, tunnel=opened.credential, e2e_key=keys.public_key) as mesh:
            box = mesh.with_options(instance="box-a")
            assert await box.call_unary(
                "test.v1.Box", "double", Num(value=21), Num
            ) == Num(value=42)
            stream = box.call_server_stream("test.v1.Box", "count", Num(value=4), Num)
            assert [i.value async for i in stream] == [0, 1, 2, 3]

            async def inputs() -> AsyncIterator[Num]:
                for i in (1, 2, 3):
                    yield Num(value=i)

            total = await box.call_client_stream(
                "test.v1.Box", "total", Num(value=1), inputs(), Num, Num
            )
            assert total == Num(value=7)
            seen = await box.call_unary("test.v1.Box", "seen", Empty(), Sealed)
            assert seen == Sealed(sealed=True)
            with pytest.raises(MeshCallError) as app:
                await box.call_unary("test.v1.Box", "fail", Num(value=3), Num)
            assert app.value.code == "app.nope" and app.value.message == "secret 42"
            assert (app.value.details or {}).get("value") == 3

            async with Mesh(url, tunnel=opened.credential) as plain:
                with pytest.raises(PermissionDeniedError) as required:
                    await plain.with_options(instance="box-a").call_unary(
                        "test.v1.Box", "double", Num(value=1), Num
                    )
                assert required.value.reason == "tunnel_e2e_required"
            wrong = Mesh(url, tunnel=opened.credential, e2e_key=other.public_key)
            with pytest.raises(UnauthenticatedError) as failed:
                async with wrong:  # connecting handshakes
                    await wrong.with_options(instance="box-a").call_unary(
                        "test.v1.Box", "double", Num(value=1), Num
                    )
                assert failed.value.reason == "e2e_handshake_failed"

            # The instance restarts with the same key and loses its sessions;
            # this client handshakes again after e2e_session_unknown.
            await first.aclose()
            async with instance(router).serve(access_log=False):
                result: Num | None = None
                for _ in range(100):
                    try:
                        result = await box.call_unary(
                            "test.v1.Box", "double", Num(value=2), Num
                        )
                        break
                    except MeshCallError as exc:
                        assert exc.reason != "e2e_session_unknown", exc
                        await asyncio.sleep(0.05)
                assert result == Num(value=4)


def test_e2e_key_needs_a_tunnel_and_a_websocket_router() -> None:
    keys = generate_e2e_key()
    with pytest.raises(ValueError, match="tunnel="):
        Mesh(Router("ws://127.0.0.1:1"), e2e_key=keys.public_key)
    with pytest.raises(ValueError, match="HTTP"):
        Mesh(Router("http://127.0.0.1:1"), tunnel="mtun_x", e2e_key=keys.public_key)
    with pytest.raises(ValueError, match="32 bytes"):
        Mesh(Router("ws://127.0.0.1:1"), tunnel="mtun_x", e2e_key="short")

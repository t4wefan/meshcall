"""Credential sources (docs/protocol.md → Credential sources).

conformance-scenario: auth.credential_source
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest
from pydantic import BaseModel
from router_fixtures import PASSWORD, write_auth_file

from meshcall import (
    CredentialSourceError,
    LocalRouter,
    Mesh,
    MeshCallError,
    Router,
    RouterCredentials,
    RouterScope,
    RpcContext,
    UnauthenticatedError,
    generate_e2e_key,
    method,
    service,
)


class Empty(BaseModel):
    pass


class Seen(BaseModel):
    tunnel: str | None
    sealed: bool


@service(name="test.v1.Box", targeted=True)
class Box:
    """A box reached through tunnels."""

    @method(read_only=True)
    async def whoami(self, ctx: RpcContext) -> Seen:
        """Report the tunnel of this call."""
        tunnel = ctx.tunnel
        return Seen(
            tunnel=tunnel.id if tunnel else None,
            sealed=bool(tunnel and tunnel.sealed),
        )


async def whoami(mesh: Mesh) -> Seen:
    return await mesh.call_unary(
        "test.v1.Box", "whoami", Empty(), Seen, instance="box-a"
    )


async def settle() -> None:
    """Until the Router has closed the revoked tunnel's connection."""
    await asyncio.sleep(0.2)


@pytest.mark.parametrize("sealed", [False, True])
async def test_a_tunnel_source_replaces_a_revoked_tunnel(sealed: bool) -> None:
    keys = generate_e2e_key()
    async with LocalRouter(port=0) as router:
        box = Mesh(router).mount(
            Box(),
            instance_id="box-a",
            accept_tunnels=True,
            **(
                {"e2e_private_key": keys.private_key, "require_e2e": True}
                if sealed
                else {}
            ),
        )
        async with box.serve(access_log=False), Mesh(router) as issuer:

            async def open_tunnel():
                return await issuer.tunnels.open(
                    instance="box-a",
                    scopes=["test.v1.Box"],
                    principal="user:alice",
                    ttl_seconds=60,
                    e2e_key=keys.public_key if sealed else None,
                )

            current = await open_tunnel()
            asked = 0

            async def source(_: str) -> str:
                nonlocal asked
                asked += 1
                return current.credential

            client = Mesh(
                Router(router.url),
                tunnel=source,
                e2e_key=keys.public_key if sealed else None,
            )
            try:
                assert await whoami(client) == Seen(
                    tunnel=current.tunnel_id, sealed=sealed
                )
                assert asked == 1

                # Revoked: the next call connects with what the source returns now.
                first = current
                current = await open_tunnel()
                await issuer.tunnels.revoke(first.tunnel_id)
                await settle()
                assert await whoami(client) == Seen(
                    tunnel=current.tunnel_id, sealed=sealed
                )

                # A source whose new credential is refused too: the call fails…
                await issuer.tunnels.revoke(current.tunnel_id)
                await settle()
                asked = 0
                with pytest.raises(UnauthenticatedError):
                    await whoami(client)
                assert asked == 2, "asked once, and once more after the refusal"

                # …and the refusal is not kept: the next call asks again.
                current = await open_tunnel()
                assert await whoami(client) == Seen(
                    tunnel=current.tunnel_id, sealed=sealed
                )
            finally:
                await client.close()


async def test_a_failing_or_wrong_source_fails_the_attempt_not_the_mesh() -> None:
    async with LocalRouter(port=0) as router:
        fail = True

        def source(_: str) -> str:
            if fail:
                raise RuntimeError("vault unreachable")
            return "mt_any"

        mesh = Mesh(Router(router.url), token=source, timeout=2.0)
        try:
            with pytest.raises(MeshCallError) as failed:
                await mesh.registry.list()
            assert failed.value.code == "unavailable"
            assert failed.value.retryable
            assert failed.value.reason == "credential_source"
            # Not kept: once the source answers, the next call connects (a
            # development Router takes any token).
            fail = False
            assert isinstance(await mesh.registry.list(), list)
        finally:
            await mesh.close()

        wrong = Mesh(Router(router.url), token=lambda _: "mtun_abc")
        try:
            with pytest.raises(CredentialSourceError):
                await wrong.registry.list()
        finally:
            await wrong.close()

    with pytest.raises(Exception, match="cannot serve"):
        Mesh(Router("ws://127.0.0.1:1"), tunnel=lambda _: "mtun_x").mount(Box())


async def test_an_instance_with_a_token_source_registers_again(tmp_path: Path) -> None:
    auth_file = write_auth_file(tmp_path / "auth.json", "test.v1.Box")
    basic = RouterCredentials.basic("issuer", PASSWORD)
    async with (
        LocalRouter(port=0, auth_file=auth_file) as router,
        Mesh(Router(router.url), credentials=basic) as issuer,
    ):

        async def issue():
            return await issuer.auth.issue_token(
                [
                    RouterScope(
                        service="test.v1.Box",
                        register_service=True,
                        instance_ids=["box-a"],
                    )
                ],
                ttl_seconds=60,
            )

        current = await issue()
        asked = 0

        def source(_: str) -> str:
            nonlocal asked
            asked += 1
            return current.token

        box = Mesh(Router(router.url), token=source).mount(Box(), instance_id="box-a")
        async with box.serve(access_log=False):
            assert await whoami(issuer) == Seen(tunnel=None, sealed=False)

            first = current
            current = await issue()
            before = asked
            await issuer.auth.revoke_token(first.token_id)
            # The instance reconnects with backoff, asking the source again.
            deadline = time.monotonic() + 10
            while True:
                try:
                    assert await whoami(issuer) == Seen(tunnel=None, sealed=False)
                    break
                except MeshCallError:
                    if time.monotonic() > deadline:
                        raise
                    await asyncio.sleep(0.1)
            assert asked > before, "the source was asked again"

        # Over HTTP: a request answered 401 is sent once more with a new token.
        async def caller_token():
            return await issuer.auth.issue_token(
                [RouterScope(service="test.v1.Box", methods=["*"])], ttl_seconds=60
            )

        caller = await caller_token()
        http_url = router.url.replace("ws://", "http://")
        async with Mesh(Router(http_url), token=lambda _: caller.token) as http:
            box2 = Mesh(Router(router.url), credentials=basic).mount(
                Box(), instance_id="box-a"
            )
            async with box2.serve(access_log=False):
                assert await whoami(http) == Seen(tunnel=None, sealed=False)
                old = caller
                caller = await caller_token()
                await issuer.auth.revoke_token(old.token_id)
                assert await whoami(http) == Seen(tunnel=None, sealed=False)

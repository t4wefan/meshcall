"""Clients that authenticate themselves on end-to-end tunnels (docs/protocol.md
→ Client authentication): IK for known clients, IKpsk2 to pair a new one."""

from __future__ import annotations

import asyncio
import contextlib

import pytest
from pydantic import BaseModel

from meshcall import (
    E2EIdentity,
    E2EPairing,
    E2EPeer,
    LocalRouter,
    Mesh,
    Router,
    RpcContext,
    generate_e2e_key,
    generate_e2e_psk,
    method,
    service,
)
from meshcall.errors import (
    ContractError,
    MeshCallError,
    PermissionDeniedError,
    UnauthenticatedError,
)


class Empty(BaseModel):
    pass


class Seen(BaseModel):
    protocol: str | None
    peer_key: str | None
    pairing_id: str | None


@service(name="test.v1.Device")
class Device:
    """Served to paired clients only."""

    @method(read_only=True)
    async def whoami(self, ctx: RpcContext) -> Seen:
        """The end-to-end session of this call."""
        e2e = ctx.e2e
        return Seen(
            protocol=e2e.protocol if e2e else None,
            peer_key=e2e.peer_key if e2e else None,
            pairing_id=e2e.pairing_id if e2e else None,
        )


@service(name="test.v1.Open")
class Open:
    """Served to any end-to-end client."""

    @method(read_only=True)
    async def whoami(self, ctx: RpcContext) -> Seen:
        """The end-to-end session of this call."""
        e2e = ctx.e2e
        return Seen(
            protocol=e2e.protocol if e2e else None,
            peer_key=e2e.peer_key if e2e else None,
            pairing_id=e2e.pairing_id if e2e else None,
        )


class Owner:
    """What the device keeps: paired client keys and one-time pairings."""

    def __init__(self) -> None:
        self.paired: set[str] = set()
        self.pairings: dict[str, str] = {}
        self.asked: list[E2EPeer] = []

    def psk(self, pairing_id: str) -> str | None:
        return self.pairings.get(pairing_id)

    async def authorize(self, peer: E2EPeer) -> bool:
        self.asked.append(peer)
        assert peer.peer_key is not None
        if peer.pairing_id is not None:
            # Spent here, once the client proved it holds the PSK.
            if self.pairings.pop(peer.pairing_id, None) is None:
                return False
            self.paired.add(peer.peer_key)
            return True
        return peer.peer_key in self.paired


def device(router: LocalRouter, keys: object, owner: Owner) -> Mesh:
    return (
        Mesh(router)
        .mount(
            Device(),
            instance_id="dev-a",
            accept_tunnels=True,
            e2e_private_key=keys.private_key,  # type: ignore[attr-defined]
            require_e2e_peer=True,
            authorize_e2e_peer=owner.authorize,
            e2e_pairing_psk=owner.psk,
        )
        .mount(Open(), accept_tunnels=True)
    )


async def whoami(mesh: Mesh, service_name: str = "test.v1.Device") -> Seen:
    return await mesh.call_unary(service_name, "whoami", Empty(), Seen)


async def test_pairing_then_known_client_then_refusals() -> None:
    keys = generate_e2e_key()
    owner = Owner()
    alice = generate_e2e_key()
    mallory = generate_e2e_key()
    async with LocalRouter(port=0) as router, Mesh(router) as issuer:
        first = contextlib.AsyncExitStack()
        await first.enter_async_context(
            device(router, keys, owner).serve(access_log=False)
        )

        async def tunnel() -> str:
            opened = await issuer.tunnels.open(
                instance="dev-a",
                scopes=["test.v1.Device", "test.v1.Open"],
                principal="user:alice",
                e2e_key=keys.public_key,
            )
            return opened.credential

        url = Router(router.url)
        # require_e2e_peer implies require_e2e: a plain tunnel is refused.
        with pytest.raises(PermissionDeniedError) as plain:
            await issuer.tunnels.open(
                instance="dev-a", scopes=["test.v1.Device"], principal="p"
            )
        assert plain.value.reason == "tunnel_rejected"
        # Not through a tunnel at all: refused by the instance itself.
        with pytest.raises(PermissionDeniedError) as direct:
            await whoami(issuer.with_options(instance="dev-a"))
        assert direct.value.reason == "e2e_peer_required"

        # An unknown client key: the handshake fails, without saying why.
        stranger = Mesh(
            url,
            tunnel=await tunnel(),
            e2e_key=keys.public_key,
            e2e_identity=E2EIdentity(mallory.private_key, "dev-a"),
        )
        with pytest.raises(UnauthenticatedError) as unknown:
            async with stranger:
                await whoami(stranger)
        assert unknown.value.reason == "e2e_handshake_failed"
        assert owner.asked[-1].peer_key == mallory.public_key

        # An NK client reaches services that do not require a peer, not others.
        async with Mesh(url, tunnel=await tunnel(), e2e_key=keys.public_key) as nk:
            seen = await whoami(nk, "test.v1.Open")
            assert seen.protocol == "Noise_NK_25519_AESGCM_SHA256"
            assert seen.peer_key is None
            with pytest.raises(PermissionDeniedError) as weak:
                await whoami(nk)
            assert weak.value.reason == "e2e_peer_required"

        # Pairing: the owner shows a pairing id and PSK on the device.
        owner.pairings["pair-1"] = generate_e2e_psk()
        # A wrong PSK fails on the client and does not spend the pairing.
        wrong = Mesh(
            url,
            tunnel=await tunnel(),
            e2e_key=keys.public_key,
            e2e_identity=E2EIdentity(
                mallory.private_key, "dev-a", E2EPairing("pair-1", generate_e2e_psk())
            ),
        )
        with pytest.raises(UnauthenticatedError) as bad_psk:
            async with wrong:
                await whoami(wrong)
        assert bad_psk.value.reason == "e2e_handshake_failed"
        assert "pair-1" in owner.pairings and mallory.public_key not in owner.paired

        pairing = E2EIdentity(
            alice.private_key, "dev-a", E2EPairing("pair-1", owner.pairings["pair-1"])
        )
        # The wrong instance id is bound in the prologue: no session.
        elsewhere = Mesh(
            url,
            tunnel=await tunnel(),
            e2e_key=keys.public_key,
            e2e_identity=E2EIdentity(alice.private_key, "dev-b", pairing.pairing),
        )
        with pytest.raises(UnauthenticatedError):
            async with elsewhere:
                await whoami(elsewhere)
        assert "pair-1" in owner.pairings

        credential = await tunnel()
        async with Mesh(
            url, tunnel=credential, e2e_key=keys.public_key, e2e_identity=pairing
        ) as mesh:
            seen = await whoami(mesh)
            assert seen == Seen(
                protocol="Noise_IKpsk2_25519_AESGCM_SHA256",
                peer_key=alice.public_key,
                pairing_id="pair-1",
            )
            assert owner.paired == {alice.public_key} and not owner.pairings

            # The device restarts and loses its sessions: the same client
            # handshakes again with IK (the pairing is spent) and goes on.
            await first.aclose()
            async with device(router, keys, owner).serve(access_log=False):
                result: Seen | None = None
                for _ in range(100):
                    try:
                        result = await whoami(mesh)
                        break
                    except MeshCallError as exc:
                        assert exc.reason != "e2e_handshake_failed", exc
                        await asyncio.sleep(0.05)
                assert result is not None
                assert result.protocol == "Noise_IK_25519_AESGCM_SHA256"
                assert result.peer_key == alice.public_key
                assert result.pairing_id is None

                # Another client cannot reuse the spent pairing.
                again = Mesh(
                    url,
                    tunnel=await tunnel(),
                    e2e_key=keys.public_key,
                    e2e_identity=E2EIdentity(
                        mallory.private_key, "dev-a", pairing.pairing
                    ),
                )
                with pytest.raises(UnauthenticatedError) as spent:
                    async with again:
                        await whoami(again)
                assert spent.value.reason == "e2e_handshake_failed"
                assert owner.paired == {alice.public_key}


def test_identity_needs_a_pinned_key_and_a_hook() -> None:
    keys = generate_e2e_key()
    me = generate_e2e_key()
    with pytest.raises(ValueError, match="e2e_key"):
        Mesh(
            Router("ws://127.0.0.1:1"),
            tunnel="mtun_e2e.x",
            e2e_identity=E2EIdentity(me.private_key, "dev-a"),
        )
    with pytest.raises(ValueError, match="instance_id"):
        Mesh(
            Router("ws://127.0.0.1:1"),
            tunnel="mtun_e2e.x",
            e2e_key=keys.public_key,
            e2e_identity=E2EIdentity(me.private_key, ""),
        )
    with pytest.raises(ValueError, match="PSK"):
        Mesh(
            Router("ws://127.0.0.1:1"),
            tunnel="mtun_e2e.x",
            e2e_key=keys.public_key,
            e2e_identity=E2EIdentity(me.private_key, "a", E2EPairing("p", "short")),
        )
    with pytest.raises(ValueError, match="require_e2e_peer needs accept_tunnels"):
        Mesh(Router("ws://127.0.0.1:1")).mount(Device(), require_e2e_peer=True)


async def test_require_e2e_peer_needs_an_authorization_hook() -> None:
    keys = generate_e2e_key()
    async with LocalRouter(port=0) as router:
        mesh = Mesh(router).mount(
            Device(),
            accept_tunnels=True,
            e2e_private_key=keys.private_key,
            require_e2e_peer=True,
        )
        with pytest.raises(ContractError, match="authorize_e2e_peer"):
            async with mesh.serve(access_log=False):
                pass

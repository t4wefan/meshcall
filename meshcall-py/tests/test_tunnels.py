"""Tunnels: issuer API, consent at mount, ctx.tunnel, scoping (docs/protocol.md → Tunnels)."""

from __future__ import annotations

import asyncio
import importlib.util
import json
from pathlib import Path

import pytest
from loguru import logger
from pydantic import BaseModel
from router_fixtures import PASSWORD, password_hash

from meshcall import (
    LocalRouter,
    Mesh,
    Router,
    RouterCredentials,
    RpcContext,
    TunnelClosed,
    TunnelOffer,
    TunnelScope,
    method,
    service,
)
from meshcall.context import CallOptions
from meshcall.di import MetaPlan
from meshcall.errors import (
    ContractError,
    InvalidArgumentError,
    PermissionDeniedError,
    UnauthenticatedError,
)


class Empty(BaseModel):
    pass


class Caller(BaseModel):
    instance: str
    tunnel_id: str | None
    principal: str | None


@service(name="test.v1.Workspace", targeted=True)
class Workspace:
    """One developer box."""

    @method(read_only=True)
    async def whoami(self, ctx: RpcContext) -> Caller:
        """Report the instance and the tunnel the call came through."""
        tunnel = ctx.tunnel
        return Caller(
            instance=ctx.instance_id,
            tunnel_id=tunnel.id if tunnel else None,
            principal=tunnel.principal if tunnel else None,
        )


@service(name="test.v1.Slow", targeted=True)
class Slow:
    """Holds a call open."""

    @method(read_only=True)
    async def hold(self) -> Empty:
        """Answer after five seconds."""
        await asyncio.sleep(5)
        return Empty()


@service(name="test.v1.Admin")
class Admin:
    """Never reachable through a tunnel."""

    @method(read_only=True)
    async def whoami(self, ctx: RpcContext) -> Caller:
        """Report the instance."""
        return Caller(instance=ctx.instance_id, tunnel_id=None, principal=None)


async def whoami(mesh: Mesh, service_name: str, instance: str | None = None) -> Caller:
    return await mesh.with_options(instance=instance).call_unary(
        service_name, "whoami", Empty(), Caller
    )


async def test_tunnel_lifecycle_through_a_router() -> None:
    offers: list[TunnelOffer] = []
    closed: list[TunnelClosed] = []

    async def decide(offer: TunnelOffer) -> bool:
        offers.append(offer)
        return offer.principal != "user:nobody"

    async with LocalRouter(port=0) as router:
        box = Mesh(router).mount(
            Workspace(),
            instance_id="box-a",
            accept_tunnels=decide,
            on_tunnel_closed=closed.append,
        )
        box.mount(Admin())
        other = Mesh(router).mount(
            Workspace(), instance_id="box-b", accept_tunnels=True
        )
        async with (
            box.serve(access_log=False),
            other.serve(access_log=False),
            Mesh(router) as issuer,
        ):
            opened = await issuer.tunnels.open(
                instance="box-a",
                scopes=[TunnelScope(service="test.v1.Workspace")],
                principal="user:alice",
                ttl_seconds=60,
            )
            assert opened.credential.startswith("mtun_")
            assert offers[-1].principal == "user:alice"
            assert not offers[-1].resumed

            with pytest.raises(PermissionDeniedError) as rejected:
                await issuer.tunnels.open(
                    instance="box-a",
                    scopes=["test.v1.Workspace"],
                    principal="user:nobody",
                )
            assert rejected.value.reason == "tunnel_rejected"
            with pytest.raises(PermissionDeniedError) as unscoped:
                # Admin did not opt in, so the instance refuses.
                await issuer.tunnels.open(
                    instance="box-a", scopes=["test.v1.Admin"], principal="p"
                )
            assert unscoped.value.reason == "tunnel_rejected"

            url = router.url
            async with Mesh(Router(url), tunnel=opened.credential) as client:
                seen = await whoami(client, "test.v1.Workspace", instance="box-a")
                assert seen == Caller(
                    instance="box-a",
                    tunnel_id=opened.tunnel_id,
                    principal="user:alice",
                )
                for service_name, instance in (
                    ("test.v1.Workspace", "box-b"),
                    ("test.v1.Admin", None),
                ):
                    with pytest.raises(PermissionDeniedError) as denied:
                        await whoami(client, service_name, instance)
                    assert denied.value.reason == "tunnel_scope"
                services = await client.registry.list()
                assert [s.name for s in services] == ["test.v1.Workspace"]
                assert [i.id for i in services[0].instances] == ["box-a"]
                identity = await client.auth.whoami()
                assert identity.tunnel is not None
                assert identity.tunnel.instance_id == "box-a"

                listed = await issuer.tunnels.list()
                assert [t.tunnel_id for t in listed] == [opened.tunnel_id]
                assert listed[0].connections == 1
                assert await issuer.tunnels.revoke(opened.tunnel_id)
                for _ in range(100):
                    if closed:
                        break
                    await asyncio.sleep(0.02)
                assert closed == [
                    TunnelClosed(opened.tunnel_id, "user:alice", "revoked")
                ]
                # Closed at once (4401 tunnel_revoked); reconnecting is refused
                # (HTTP 401). Either way the caller sees unauthenticated.
                with pytest.raises(UnauthenticatedError):
                    await whoami(client, "test.v1.Workspace", instance="box-a")
            assert not await issuer.tunnels.revoke(opened.tunnel_id)


async def test_tunnel_credentials_have_one_way_in() -> None:
    router = Router("ws://127.0.0.1:1")
    with pytest.raises(ValueError, match="tunnel="):
        Mesh(router, token="mtun_abc")
    with pytest.raises(ValueError, match="mtun_"):
        Mesh(router, tunnel="mt_abc")
    with pytest.raises(ValueError, match="one of"):
        Mesh(router, token="mt_abc", tunnel="mtun_abc")
    with pytest.raises(ContractError, match="cannot serve"):
        Mesh(router, tunnel="mtun_abc").mount(Workspace())
    with pytest.raises(ValueError, match="needs accept_tunnels"):
        Mesh(router).mount(Workspace(), on_tunnel_closed=print)


async def test_generated_targeted_client_over_a_tunnel(meshcall_cli, tmp_path) -> None:
    """Generated clients keep `instance` required (T76): on a tunnel the
    caller passes the tunnel's instance, and the Router checks it."""
    out = tmp_path / "workspace_client.py"
    meshcall_cli(
        "generate",
        "test_tunnels:Workspace",
        "--language",
        "python",
        "--single-file",
        "--output",
        str(out),
        cwd=Path(__file__).resolve().parent,
    )
    spec = importlib.util.spec_from_file_location("workspace_client", out)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    async with LocalRouter(port=0) as router:
        box = Mesh(router).mount(Workspace(), instance_id="box-a", accept_tunnels=True)
        async with box.serve(access_log=False), Mesh(router) as issuer:
            opened = await issuer.tunnels.open(
                instance="box-a", scopes=["test.v1.Workspace"], principal="user:alice"
            )
            async with Mesh(Router(router.url), tunnel=opened.credential) as mesh:
                client = module.WorkspaceClient(mesh)
                seen = await client.whoami(instance=opened.instance_id)
                assert seen.principal == "user:alice"
                with pytest.raises(PermissionDeniedError) as wrong:
                    await client.whoami(instance="box-b")
                assert wrong.value.reason == "tunnel_scope"
                assert (wrong.value.details or {}).get("instance_id") == "box-a"


def test_reserved_metadata_keys_ignore_letter_case() -> None:
    """A caller's "MESHCALL-…" key is refused, and meta() never reads one."""
    for key in ("meshcall-x", "MeshCall-Tunnel-Principal"):
        with pytest.raises(InvalidArgumentError) as refused:
            CallOptions(metadata={key: "user:mallory"})
        assert refused.value.reason == "reserved_metadata"
    ctx = RpcContext(
        call_id="c",
        service="s",
        method="m",
        metadata={"MESHCALL-TUNNEL-PRINCIPAL": "user:mallory", "X-User": "bob"},
        logger=logger,
    )
    reserved = MetaPlan("meshcall-tunnel-principal", str, True, None)
    assert reserved.resolve(ctx) is None
    assert MetaPlan("x-user", str, True, None).resolve(ctx) == "bob"
    set_by_router = RpcContext(
        call_id="c",
        service="s",
        method="m",
        metadata={"meshcall-tunnel-principal": "user:alice"},
        logger=logger,
    )
    assert MetaPlan("MeshCall-Tunnel-Principal", str, True, None).resolve(
        set_by_router
    ) == ("user:alice")


async def test_tunnel_errors_say_why() -> None:
    """open is checked by the Router; revoking fails calls in flight with
    unauthenticated / tunnel_revoked, not a bare connection loss."""
    async with LocalRouter(port=0) as router:
        box = Mesh(router).mount(Slow(), instance_id="box-a", accept_tunnels=True)
        async with box.serve(access_log=False), Mesh(router) as issuer:
            with pytest.raises(InvalidArgumentError) as bad_ttl:
                await issuer.tunnels.open(
                    instance="box-a",
                    scopes=["test.v1.Slow"],
                    principal="p",
                    ttl_seconds=0,
                )
            assert "ttl_seconds" in bad_ttl.value.message
            assert (bad_ttl.value.details or {}).get("source") == "router"
            opened = await issuer.tunnels.open(
                instance="box-a", scopes=["test.v1.Slow"], principal="user:alice"
            )
            async with Mesh(Router(router.url), tunnel=opened.credential) as mesh:
                held = asyncio.create_task(
                    mesh.with_options(instance="box-a").call_unary(
                        "test.v1.Slow", "hold", Empty(), Empty
                    )
                )
                await asyncio.sleep(0.2)
                assert await issuer.tunnels.revoke(opened.tunnel_id)
                with pytest.raises(UnauthenticatedError) as revoked:
                    await held
                assert revoked.value.reason == "tunnel_revoked"
                # Later calls get the same answer without reconnecting.
                with pytest.raises(UnauthenticatedError) as later:
                    await mesh.with_options(instance="box-a").call_unary(
                        "test.v1.Slow", "hold", Empty(), Empty
                    )
                assert later.value.reason == "tunnel_revoked"


async def test_a_refused_registration_says_why(tmp_path: Path) -> None:
    auth_file = tmp_path / "auth.json"
    auth_file.write_text(
        json.dumps(
            {
                "users": [
                    {
                        "username": "box",
                        "password_hash": password_hash(),
                        "roles": ["server"],
                        "register": ["test.v1.Slow"],
                        "instance_ids": ["box-a"],
                    }
                ]
            }
        )
    )
    credentials = RouterCredentials.basic("box", PASSWORD)
    async with LocalRouter(port=0, auth_file=auth_file) as router:
        mesh = Mesh(router, credentials=credentials).mount(Slow(), instance_id="box-b")
        with pytest.raises(PermissionDeniedError, match="instance_id not allowed"):
            async with mesh.serve(access_log=False):
                pass

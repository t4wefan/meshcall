"""Tunnels: issuer API, consent at mount, ctx.tunnel, scoping (docs/protocol.md → Tunnels)."""

from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path

import pytest
from pydantic import BaseModel
from websockets.exceptions import InvalidStatus

from meshcall import (
    LocalRouter,
    Mesh,
    Router,
    RpcContext,
    TunnelClosed,
    TunnelOffer,
    TunnelScope,
    method,
    service,
)
from meshcall.errors import ContractError, MeshCallError, PermissionDeniedError


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
                # Closed at once; reconnecting is refused (HTTP 401).
                with pytest.raises((MeshCallError, InvalidStatus)):
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

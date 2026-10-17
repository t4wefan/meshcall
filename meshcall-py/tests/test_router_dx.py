from __future__ import annotations

from pathlib import Path

import pytest
from router_fixtures import PASSWORD, write_auth_file
from test_direct import NumberRequest, NumberResult, NumberService, NumberServiceClient
from test_router import RouteRequest, RouteResult, RouteServiceA

from meshcall import (
    ContractError,
    LocalRouter,
    Mesh,
    Router,
    RouterCredentials,
    RouterScope,
    get_service_contract,
)
from meshcall.client import ClientBase


class RouteServiceClient(ClientBase):
    async def round_robin(self, request: RouteRequest) -> RouteResult:
        return await self.call_unary(
            "test.v1.RouteService",
            "round_robin",
            request,
            RouteResult,
        )


def test_strict_layering_between_router_and_mesh() -> None:
    with pytest.raises(TypeError, match="Router or LocalRouter"):
        Mesh("ws://127.0.0.1:8765")  # type: ignore[arg-type]

    http_mesh = Mesh(Router("http://127.0.0.1:8765"))
    with pytest.raises(ContractError, match="HTTP"):
        http_mesh.mount(NumberService())


async def test_local_router_and_mesh_multiplexed_clients() -> None:
    async with LocalRouter(port=0) as local_router:
        assert local_router.is_running
        assert local_router.bound_port is not None and local_router.bound_port > 0
        assert local_router.pid is not None and local_router.pid > 0
        assert local_router.url.startswith("ws://127.0.0.1:")
        assert local_router.http_url is not None
        assert local_router.http_url.startswith("http://127.0.0.1:")

        server_mesh = Mesh(local_router)
        server_mesh.mount(NumberService(), RouteServiceA(), instance_id="dx-node-1")

        async with (
            server_mesh.serve(access_log=False),
            Mesh(Router(local_router.url)) as mesh,
        ):
            # Built-in Registry RPCs via mesh.registry
            services = await mesh.registry.list()
            names = {s.name for s in services}
            assert "test.v1.NumberService" in names
            assert "test.v1.RouteService" in names

            desc = await mesh.registry.describe("test.v1.NumberService")
            assert desc is not None
            assert desc.name == "test.v1.NumberService"
            assert len(desc.instances) == 1
            assert any(m.name == "unary" for m in desc.methods)

            # Multiplex two service clients over the single Mesh connection
            num_client = NumberServiceClient(mesh)
            route_client = RouteServiceClient(mesh)

            res1 = await num_client.unary(NumberRequest(value=42))
            assert res1 == NumberResult(total=42)

            res2 = await route_client.round_robin(RouteRequest())
            assert res2 == RouteResult(instance="a")

            # .with_options(...) chaining returns a shallow view sharing the same connection
            scoped_num = num_client.with_options(
                timeout=5.0, metadata={"x-trace": "123"}
            )
            res3 = await scoped_num.unary(NumberRequest(value=99))
            assert res3 == NumberResult(total=99)

            scoped_mesh_num = NumberServiceClient(mesh.with_options(timeout=5.0))
            assert await scoped_mesh_num.unary(NumberRequest(value=7)) == NumberResult(
                total=7
            )

            # Closing a multiplexed client is a no-op on the shared Mesh connection
            await num_client.close()
            res4 = await route_client.round_robin(RouteRequest())
            assert res4 == RouteResult(instance="a")

            # Lazy connection without `async with` also works
            lazy_mesh = Mesh(Router(local_router.url))
            try:
                assert await NumberServiceClient(lazy_mesh).unary(
                    NumberRequest(value=5)
                ) == NumberResult(total=5)
            finally:
                await lazy_mesh.close()


async def test_mesh_manages_unstarted_local_router() -> None:
    unstarted_router = LocalRouter(port=0)
    assert not unstarted_router.is_running

    mesh = Mesh(unstarted_router)
    mesh.mount(NumberService())

    async with mesh.serve(access_log=False):
        assert unstarted_router.is_running
        client = NumberServiceClient(mesh)
        assert await client.unary(NumberRequest(value=12)) == NumberResult(total=12)

    assert not unstarted_router.is_running


async def test_mesh_builtin_auth_and_token_scoping(tmp_path: Path) -> None:
    name = get_service_contract(NumberService).name
    auth_file = write_auth_file(tmp_path / "auth.json", name)
    basic_creds = RouterCredentials.basic("issuer", PASSWORD)

    async with LocalRouter(port=0, auth_file=auth_file) as local_router:
        server_mesh = Mesh(local_router, credentials=basic_creds)
        server_mesh.mount(NumberService())

        async with (
            server_mesh.serve(access_log=False),
            Mesh(Router(local_router.url), credentials=basic_creds) as admin_mesh,
        ):
            identity = await admin_mesh.auth.whoami()
            assert identity.username == "issuer"
            assert identity.register_services == [name]

            issued = await admin_mesh.auth.issue_token(
                [RouterScope(service=name, methods=["unary"])],
                ttl_seconds=300,
            )

            async with Mesh(Router(local_router.url), token=issued.token) as user_mesh:
                client = NumberServiceClient(user_mesh)
                assert await client.unary(NumberRequest(value=11)) == NumberResult(
                    total=11
                )

            assert await admin_mesh.auth.revoke_token(issued.token_id)

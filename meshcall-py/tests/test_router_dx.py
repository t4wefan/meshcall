from __future__ import annotations

from pathlib import Path

from router_fixtures import PASSWORD, write_auth_file
from test_direct import NumberRequest, NumberResult, NumberService, NumberServiceClient
from test_router import RouteRequest, RouteResult, RouteServiceA

from meshcall import (
    Router,
    RouterCredentials,
    RouterScope,
    get_service_contract,
    serve,
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


async def test_serve_autospawn_and_router_multiplexed_clients() -> None:
    router_cfg = Router("ws://127.0.0.1:0")
    async with serve(
        NumberService(), RouteServiceA(), router=router_cfg, access_log=False
    ) as running:
        assert running.spawned_router is not None
        assert router_cfg.url is not None
        assert router_cfg.url.startswith("ws://127.0.0.1:")
        assert not router_cfg.url.endswith(":0")

        async with Router.connect(router_cfg.url) as router:
            # Built-in Registry RPCs
            services = await router.list_services()
            names = {s.name for s in services}
            assert "test.v1.NumberService" in names
            assert "test.v1.RouteService" in names

            desc = await router.describe_service("test.v1.NumberService")
            assert desc is not None
            assert desc.name == "test.v1.NumberService"
            assert len(desc.instances) == 1
            assert any(m.name == "unary" for m in desc.methods)

            # Multiplex two service clients over the single Router connection
            num_client = NumberServiceClient(router)
            route_client = RouteServiceClient(router)

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

            # Closing a multiplexed client is a no-op on the shared router connection
            await num_client.close()
            res4 = await route_client.round_robin(RouteRequest())
            assert res4 == RouteResult(instance="a")


async def test_client_connect_and_router_builtin_auth(tmp_path: Path) -> None:
    name = get_service_contract(NumberService).name
    auth_file = write_auth_file(tmp_path / "auth.json", name)
    basic_creds = RouterCredentials.basic("issuer", PASSWORD)

    router_cfg = Router(
        "ws://127.0.0.1:0",
        auth=basic_creds,
        auth_file=auth_file,
    )
    async with (
        serve(NumberService(), router=router_cfg, access_log=False),
        Router.connect(router_cfg.url, auth=basic_creds) as admin_router,
    ):
        identity = await admin_router.whoami()
        assert identity.username == "issuer"
        assert identity.register_services == [name]

        issued = await admin_router.issue_token(
            [RouterScope(service=name, methods=["unary"])],
            ttl_seconds=300,
        )
        bearer_creds = RouterCredentials.bearer(issued.token)

        # Connect directly via GeneratedClient.connect(...)
        async with NumberServiceClient.connect(
            router_cfg.url, auth=bearer_creds
        ) as client:
            assert await client.unary(NumberRequest(value=11)) == NumberResult(total=11)

        assert await admin_router.revoke_token(issued.token_id)

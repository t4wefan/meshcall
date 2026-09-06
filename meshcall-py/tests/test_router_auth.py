from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from router_fixtures import PASSWORD, write_auth_file
from test_direct import NumberRequest, NumberResult, NumberService, NumberServiceClient
from test_router import RouteServiceA
from websockets.exceptions import InvalidStatus

from meshcall import (
    RouterAuthClient,
    RouterCredentials,
    RouterScope,
    RpcServer,
    WebSocketRouter,
    get_service_contract,
)
from meshcall.drivers import WebSocketClientDriver, WebSocketRouterServerDriver
from meshcall.errors import MeshCallError


async def test_accounts_issue_scoped_tokens_and_revoke_active_calls(
    tmp_path: Path,
) -> None:
    name = get_service_contract(NumberService).name
    auth_file = write_auth_file(tmp_path / "auth.json", name)
    async with WebSocketRouter(auth_file=auth_file) as router:
        uri = f"ws://127.0.0.1:{router.bound_port}"
        credentials = RouterCredentials(username="issuer", password=PASSWORD)
        server = RpcServer(
            services=[NumberService],
            driver=WebSocketRouterServerDriver(uri, auth=credentials),
            access_log=False,
        )
        await server.start()
        try:
            async with RouterAuthClient(
                WebSocketClientDriver(uri, auth=credentials)
            ) as auth:
                identity = await auth.whoami()
                assert identity.username == "issuer"
                assert identity.register_services == [name]
                issued = await auth.issue_token(
                    scopes=[RouterScope(service=name, methods=["unary", "download"])],
                )
                assert issued.scopes[0].service == name
                assert issued.token not in repr(issued)
                token_auth = RouterCredentials(token=issued.token)
                assert issued.token not in repr(token_auth)
                async with NumberServiceClient(
                    WebSocketClientDriver(uri, auth=token_auth)
                ) as client:
                    assert await client.unary(NumberRequest(value=7)) == NumberResult(
                        total=7
                    )
                    assert (
                        len(
                            [
                                item
                                async for item in client.download(
                                    NumberRequest(value=64)
                                )
                            ]
                        )
                        == 64
                    )
                    pending = client.download(NumberRequest(value=100000))
                    await anext(pending)
                    assert await auth.revoke_token(issued.token_id)
                    with pytest.raises(MeshCallError):
                        _ = [item async for item in pending]
                    assert not await auth.revoke_token(issued.token_id)
                with pytest.raises(InvalidStatus):
                    await WebSocketClientDriver(uri, auth=token_auth).connect()
                with pytest.raises(MeshCallError) as denied:
                    await auth.issue_token(
                        scopes=[RouterScope(service="other.v1.Service", methods=["*"])]
                    )
                assert denied.value.code == "permission_denied"
        finally:
            await server.stop()


async def test_service_registration_scope_and_revocation(tmp_path: Path) -> None:
    name = get_service_contract(NumberService).name
    async with WebSocketRouter(
        auth_file=write_auth_file(tmp_path / "auth.json", name)
    ) as router:
        uri = f"ws://127.0.0.1:{router.bound_port}"
        credentials = RouterCredentials(username="issuer", password=PASSWORD)
        async with RouterAuthClient(
            WebSocketClientDriver(uri, auth=credentials)
        ) as auth:
            issued = await auth.issue_token(
                scopes=[RouterScope(service=name, register_service=True)]
            )
            token = RouterCredentials(token=issued.token)
            forbidden = RpcServer(
                services=[RouteServiceA],
                driver=WebSocketRouterServerDriver(uri, auth=token),
            )
            try:
                with pytest.raises(ConnectionError):
                    await forbidden.start()
            finally:
                await forbidden.stop()
            worker = RpcServer(
                services=[NumberService],
                access_log=False,
                driver=WebSocketRouterServerDriver(uri, auth=token),
            )
            await worker.start()
            try:
                async with NumberServiceClient(
                    WebSocketClientDriver(uri, auth=credentials)
                ) as client:
                    assert await client.unary(NumberRequest(value=5)) == NumberResult(
                        total=5
                    )
                    pending = client.download(NumberRequest(value=100000))
                    await anext(pending)
                    assert await auth.revoke_token(issued.token_id)
                    with pytest.raises(MeshCallError) as lost:
                        _ = [item async for item in pending]
                    assert lost.value.code == "unavailable"
            finally:
                await worker.stop()


async def test_expiry_closes_existing_connection_and_method_scope_is_enforced(
    tmp_path: Path,
) -> None:
    name = get_service_contract(NumberService).name
    async with WebSocketRouter(
        auth_file=write_auth_file(tmp_path / "auth.json", name)
    ) as router:
        uri = f"ws://127.0.0.1:{router.bound_port}"
        credentials = RouterCredentials(username="issuer", password=PASSWORD)
        async with RouterAuthClient(
            WebSocketClientDriver(uri, auth=credentials)
        ) as auth:
            token = await auth.issue_token(
                ttl_seconds=1, scopes=[RouterScope(service=name, methods=["unary"])]
            )
            async with RouterAuthClient(
                WebSocketClientDriver(uri, auth=RouterCredentials(token=token.token))
            ) as scoped:
                assert (
                    await scoped.whoami()
                ).expires_at_unix_ms == token.expires_at_unix_ms
                with pytest.raises(MeshCallError) as denied:
                    await scoped.issue_token(
                        scopes=[RouterScope(service=name, methods=["unary"])]
                    )
                assert denied.value.code == "permission_denied"
                assert scoped._receive_task is not None
                await asyncio.wait_for(scoped._receive_task, 3)
            reader = WebSocketClientDriver(
                uri, auth=RouterCredentials(username="reader", password=PASSWORD)
            )
            async with NumberServiceClient(reader) as client:
                with pytest.raises(MeshCallError) as denied:
                    _ = [item async for item in client.download(NumberRequest(value=1))]
                assert denied.value.code == "permission_denied"

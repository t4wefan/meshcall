"""The local transport: Unix sockets, or named pipes on Windows (only there)."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import TypedDict

import pytest
from test_direct import NumberRequest, NumberResult, NumberService, NumberServiceClient

from meshcall import RpcServer, WebSocketRouter
from meshcall.drivers import (
    WebSocketClientDriver,
    WebSocketDirectServerDriver,
    WebSocketRouterServerDriver,
)

WINDOWS = sys.platform == "win32"


class LocalEndpoint(TypedDict, total=False):
    pipe: str
    unix_path: str


def local_endpoint() -> LocalEndpoint:
    """A fresh local endpoint for this platform, as driver keyword arguments."""
    if WINDOWS:
        return {"pipe": f"meshcall-test-{uuid.uuid4().hex}"}
    return {"unix_path": str(Path("/tmp") / f"meshcall-{uuid.uuid4().hex}.sock")}


async def test_direct_server_over_the_local_transport() -> None:
    endpoint = local_endpoint()
    server = RpcServer(
        services=[NumberService], driver=WebSocketDirectServerDriver(**endpoint)
    )
    await server.start()
    client = NumberServiceClient(WebSocketClientDriver(**endpoint))
    try:
        assert await client.unary(NumberRequest(value=7)) == NumberResult(total=7)
    finally:
        await client.stop()
        await server.stop()


async def test_router_over_the_local_transport() -> None:
    endpoint = local_endpoint()
    router = WebSocketRouter(**endpoint)
    await router.start()
    server = RpcServer(
        services=[NumberService], driver=WebSocketRouterServerDriver(**endpoint)
    )
    await server.start()
    client = NumberServiceClient(WebSocketClientDriver(**endpoint))
    try:
        assert await client.unary(NumberRequest(value=1)) == NumberResult(total=1)
        with pytest.raises(Exception, match="TCP port"):
            _ = router.bound_port
    finally:
        await client.stop()
        await server.stop()
        await router.stop()


def test_the_other_platforms_transport_is_refused() -> None:
    if WINDOWS:
        with pytest.raises(ValueError, match="use pipe="):
            WebSocketClientDriver(unix_path="/tmp/x.sock")
        with pytest.raises(ValueError, match="use pipe="):
            WebSocketRouter(unix_path="/tmp/x.sock")
    else:
        with pytest.raises(ValueError, match="Windows-only"):
            WebSocketClientDriver(pipe="meshcall")
        with pytest.raises(ValueError, match="Windows-only"):
            WebSocketDirectServerDriver(pipe="meshcall")
        with pytest.raises(ValueError, match="Windows-only"):
            WebSocketRouter(pipe="meshcall")
    with pytest.raises(ValueError, match="exactly one"):
        WebSocketClientDriver("ws://127.0.0.1:1", pipe="meshcall")

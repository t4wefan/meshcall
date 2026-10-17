"""Mesh with an HTTP(S) Router: the HTTP transport for generated clients."""

from __future__ import annotations

import json
import sys
import types
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import BaseModel

from meshcall import (
    LocalRouter,
    Mesh,
    MeshCallError,
    RetryPolicy,
    Router,
    mutates,
    query,
    service,
    stream,
)

TESTS = Path(__file__).resolve().parent


class Sum(BaseModel):
    total: int


class Item(BaseModel):
    value: int


@service("test.v1.HttpShape")
class HttpShape:
    """Every call shape the HTTP API supports."""

    @query
    async def add(self, a: int, b: int) -> Sum:
        """Add two numbers."""
        return Sum(total=a + b)

    @stream
    async def count(self, n: int) -> AsyncIterator[Item]:
        """Yield 0..n-1."""
        for value in range(n):
            yield Item(value=value)

    @mutates(idempotent=True)
    async def upload(self, items: AsyncIterator[Item]) -> Sum:
        """Add every uploaded value."""
        total = 0
        async for item in items:
            total += item.value
        return Sum(total=total)

    @mutates(destructive=True)
    async def fail(self) -> Sum:
        """Raise an application error."""
        raise MeshCallError("app.nope", "no", details={"from": "service"})


@pytest.fixture
def generated(
    meshcall_cli: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Any:
    output = tmp_path / "http_shape_client.py"
    meshcall_cli(
        "generate",
        "test_http_router:HttpShape",
        "--single-file",
        "--output",
        str(output),
        cwd=TESTS,
    )
    module = types.ModuleType("generated_http_shape_client")
    monkeypatch.setitem(sys.modules, module.__name__, module)
    exec(compile(output.read_text(), str(output), "exec"), module.__dict__)  # noqa: S102
    return module


@pytest.fixture
async def router_http_url() -> AsyncIterator[str]:
    async with LocalRouter(host="127.0.0.1", port=0) as local_router:
        server_mesh = Mesh(local_router)
        server_mesh.mount(HttpShape())
        async with server_mesh.serve(access_log=False):
            assert local_router.http_url is not None
            yield local_router.http_url


async def test_generated_client_over_http(generated: Any, router_http_url: str) -> None:
    async with Mesh(Router(router_http_url)) as mesh:
        client = generated.HttpShapeClient(mesh)
        assert (await client.add(2, 3)).total == 5
        assert [item.value async for item in client.count(4)] == [0, 1, 2, 3]

        generated_item = generated.Item

        async def items() -> AsyncIterator[Any]:
            yield generated_item(value=1)
            yield generated_item(value=2)

        assert (await client.upload(items())).total == 3

        shared = generated.HttpShapeClient(mesh)
        assert (await shared.add(1, 1)).total == 2


async def test_http_errors_and_builtin_registry(
    generated: Any, router_http_url: str
) -> None:
    async with Mesh(Router(router_http_url)) as mesh:
        client = generated.HttpShapeClient(mesh)
        with pytest.raises(MeshCallError) as failure:
            await client.fail()
        assert failure.value.code == "app.nope"
        assert (failure.value.details or {})["from"] == "service"

        services = await mesh.registry.list()
        assert [entry.name for entry in services] == ["test.v1.HttpShape"]


async def test_duplex_is_refused_over_http(router_http_url: str) -> None:
    async with Mesh(Router(router_http_url)) as mesh:
        with pytest.raises(MeshCallError) as failure:
            mesh._ensure_client().call_duplex("test.v1.HttpShape", "talk", Sum(total=0))
        assert failure.value.reason == "duplex_over_http"


class _Ping(BaseModel):
    value: int


class _Pong(BaseModel):
    value: int


async def test_unary_retry_follows_idempotency() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            return httpx.Response(
                503,
                json={
                    "error": {
                        "code": "unavailable",
                        "message": "later",
                        "retryable": True,
                    }
                },
            )
        return httpx.Response(200, json={"value": json.loads(request.content)["value"]})

    mock_http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    async with Mesh(
        Router("http://router.test"),
        http_client=mock_http_client,
        retry=RetryPolicy(initial_delay=0.001, max_delay=0.002),
    ) as mesh:
        transport = mesh._ensure_client()
        result = await transport.call_unary(
            "test.v1.Ping", "ping", _Ping(value=7), _Pong, idempotent=True
        )
        assert result.value == 7
        assert attempts == 3

        attempts = 0
        with pytest.raises(MeshCallError):
            await transport.call_unary("test.v1.Ping", "ping", _Ping(value=7), _Pong)
        assert attempts == 1
    await mock_http_client.aclose()


async def test_websocket_mesh_also_accepts_generated_clients(
    generated: Any, router_http_url: str
) -> None:
    ws_url = "ws://" + router_http_url.removeprefix("http://")
    async with Mesh(Router(ws_url)) as mesh:
        client = generated.HttpShapeClient(mesh)
        assert (await client.add(4, 5)).total == 9

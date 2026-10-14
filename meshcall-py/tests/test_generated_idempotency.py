"""Generated unary methods carry an idempotency key to the handler (#28)."""

from __future__ import annotations

import sys
import types
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from meshcall import RpcContext, RpcServer, method, service
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver

TESTS = Path(__file__).resolve().parent


class Key(BaseModel):
    key: str | None


@service("test.v1.Keyed")
class Keyed:
    """Echoes the idempotency key the caller sent."""

    @method(idempotent=True)
    async def put(self, value: int, ctx: RpcContext) -> Key:
        """Store a value; safe to repeat with the same key."""
        return Key(key=ctx.idempotency_key)


@pytest.fixture
def generated(
    meshcall_cli: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Any:
    output = tmp_path / "keyed_client.py"
    meshcall_cli(
        "generate",
        "test_generated_idempotency:Keyed",
        "--single-file",
        "--output",
        str(output),
        cwd=TESTS,
    )
    source = output.read_text(encoding="utf-8")
    assert "idempotency_key: str | None = None," in source
    module = types.ModuleType("generated_keyed_client")
    monkeypatch.setitem(sys.modules, module.__name__, module)
    exec(compile(source, str(output), "exec"), module.__dict__)  # noqa: S102
    return module


@pytest.fixture
async def server() -> AsyncIterator[int]:
    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(services=[Keyed], driver=driver, access_log=False)
    await server.start()
    try:
        yield driver.bound_port
    finally:
        await server.stop()


async def test_generated_unary_method_sends_the_key(
    generated: Any, server: int
) -> None:
    async with generated.KeyedClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{server}")
    ) as client:
        assert (await client.put(1, idempotency_key="order-42")).key == "order-42"
        assert (await client.put(1)).key is None

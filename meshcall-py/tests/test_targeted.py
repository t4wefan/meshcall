"""Targeted calls: instance ids, targeted services, generated-client signatures."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from meshcall import Balance, RpcContext, RpcServer, method, service
from meshcall.client import ClientBase
from meshcall.contract import get_service_contract
from meshcall.diagnostics import lint_contract
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
from meshcall.errors import InvalidArgumentError, UnavailableError

TESTS = Path(__file__).resolve().parent


class Empty(BaseModel):
    pass


class Instance(BaseModel):
    instance: str


@service(name="test.v1.Actor", targeted=True)
class Actor:
    """Holds per-instance state; callers must say which instance."""

    @method(read_only=True)
    async def whoami(self, ctx: RpcContext) -> Instance:
        """Report the serving instance."""
        return Instance(instance=ctx.instance_id)


@service(name="test.v1.Plain")
class Plain:
    """An ordinary service that still accepts targeting."""

    @method(read_only=True)
    async def whoami(self, ctx: RpcContext) -> Instance:
        """Report the serving instance."""
        return Instance(instance=ctx.instance_id)


class Client(ClientBase):
    async def whoami(self, service_name: str, **kwargs: Any) -> Instance:
        return await self._unary(service_name, "whoami", Empty(), Instance, **kwargs)


@pytest.fixture
async def client() -> AsyncIterator[tuple[Client, RpcServer]]:
    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0, instance_id="node-7")
    server = RpcServer(services=[Actor, Plain], driver=driver, access_log=False)
    await server.start()
    try:
        async with Client(
            WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}")
        ) as client:
            yield client, server
    finally:
        await server.stop()


async def test_direct_server_applies_targeting_rules(
    client: tuple[Client, RpcServer],
) -> None:
    rpc, server = client
    assert server.instance_id == "node-7"
    with pytest.raises(InvalidArgumentError) as required:
        await rpc.whoami("test.v1.Actor")
    assert required.value.reason == "instance_required"
    assert (await rpc.whoami("test.v1.Actor", instance="node-7")).instance == "node-7"
    with pytest.raises(UnavailableError) as unknown:
        await rpc.whoami("test.v1.Actor", instance="node-8")
    assert unknown.value.reason == "instance_unknown"
    assert not unknown.value.retryable
    # Ordinary services can be targeted too, and still work untargeted.
    assert (await rpc.whoami("test.v1.Plain")).instance == "node-7"
    assert (await rpc.whoami("test.v1.Plain", instance="node-7")).instance == "node-7"


def test_generated_instance_id_is_stable_per_driver() -> None:
    a = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    b = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    assert a.instance_id and a.instance_id != b.instance_id


def test_contract_and_generated_clients_carry_targeting(meshcall_cli, tmp_path) -> None:
    actor, plain = get_service_contract(Actor), get_service_contract(Plain)
    assert actor.targeted and not plain.targeted
    generate = lambda spec, language, out: (
        meshcall_cli(
            "generate",
            spec,
            "--language",
            language,
            "--single-file",
            "--output",
            str(tmp_path / out),
            cwd=TESTS,
        ),
        (tmp_path / out).read_text(),
    )[1]
    py_actor = generate("test_targeted:Actor", "python", "actor.py")
    py_plain = generate("test_targeted:Plain", "python", "plain.py")
    # A targeted call names its instance at compile time; other options come
    # from with_options().
    assert "        instance: str," in py_actor
    assert "instance=instance" not in py_plain
    assert "instance=instance," in py_actor
    ts_actor = generate("test_targeted:Actor", "typescript", "actor.ts")
    ts_plain = generate("test_targeted:Plain", "typescript", "plain.ts")
    assert "options: CallOptions & { instance: string }" in ts_actor
    assert "options?: CallOptions" in ts_plain


def test_sticky_on_targeted_service_warns() -> None:
    @service(
        name="test.v1.StickyActor", targeted=True, balance=Balance.sticky("request.key")
    )
    class StickyActor:
        """Misconfigured: sticky and targeted."""

        @method(read_only=True)
        async def get(self, key: str) -> Instance:
            """Read."""
            return Instance(instance=key)

    codes = [w.code for w in lint_contract(get_service_contract(StickyActor))]
    assert "W006" in codes

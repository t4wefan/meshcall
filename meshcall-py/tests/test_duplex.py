"""Duplex: the restricted both-ways shape (docs/protocol.md → Duplex calls)."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncIterator
from typing import Any

import pytest
from pydantic import BaseModel

from meshcall import (
    ContractError,
    RpcContext,
    RpcDuplex,
    RpcServer,
    get_service_contract,
    method,
    service,
)
from meshcall.client import ClientBase
from meshcall.context import ClientCallInfo, ClientCallNext
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
from meshcall.errors import ErrorCode, MeshCallError
from meshcall.streams import RpcDuplexClient


class Turn(BaseModel):
    text: str


class Request(BaseModel):
    prefix: str = ""


@service(name="test.v1.DuplexService", description="Duplex fixtures.")
class DuplexService:
    @method.duplex(reason="replies interleave with what the caller keeps sending")
    async def echo(self, prefix: str, channel: RpcDuplex[Turn, Turn]) -> None:
        """Answer each input as it arrives."""
        async for turn in channel:
            await channel.send(Turn(text=prefix + turn.text))

    @method.duplex(reason="the server stops talking before the caller does")
    async def close_early(self, channel: RpcDuplex[Turn, Turn]) -> None:
        """Send one item, close the output, then keep reading input."""
        await channel.send(Turn(text="only"))
        await channel.close_send()
        async for _ in channel:
            pass

    @method.duplex(reason="fails part way through")
    async def fail_after_one(
        self, ctx: RpcContext, channel: RpcDuplex[Turn, Turn]
    ) -> None:
        """Echo one input, then fail with a trailer."""
        async for turn in channel:
            await channel.send(turn)
            ctx.set_trailer("seen", turn.text)
            raise MeshCallError.from_code("app.failed", "gave up")


class DuplexClient(ClientBase):
    service_name = "test.v1.DuplexService"

    def echo(self, prefix: str = "") -> RpcDuplexClient[Turn, Turn]:
        return self.call_duplex(
            self.service_name, "echo", Request(prefix=prefix), Turn, Turn
        )

    def close_early(self) -> RpcDuplexClient[Turn, Turn]:
        return self.call_duplex(self.service_name, "close_early", Request(), Turn, Turn)

    def fail_after_one(self) -> RpcDuplexClient[Turn, Turn]:
        return self.call_duplex(
            self.service_name, "fail_after_one", Request(), Turn, Turn
        )


@pytest.fixture
async def client() -> AsyncIterator[DuplexClient]:
    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(services=[DuplexService()], driver=driver, access_log=False)
    await server.start()
    rpc = DuplexClient(WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}"))
    try:
        yield rpc
    finally:
        await rpc.stop()
        await server.stop()


def test_contract_carries_the_reason() -> None:
    contract = get_service_contract(DuplexService)
    echo = contract.methods[0]
    assert echo.stream == "duplex"
    assert echo.duplex_reason == "replies interleave with what the caller keeps sending"
    assert echo.response is None
    assert echo.input_item is not None and echo.output_item is not None


def test_reason_is_required() -> None:
    with pytest.raises(ContractError, match="reason"):
        method.duplex(reason="  ")


def test_channel_needs_the_duplex_declaration() -> None:
    with pytest.raises(ContractError, match=r"@method\.duplex"):

        @service(name="test.v1.Undeclared")
        class Undeclared:
            @method()
            async def chat(self, channel: RpcDuplex[Turn, Turn]) -> None:
                """Chat."""

        del Undeclared


def test_duplex_has_no_result_value() -> None:
    with pytest.raises(ContractError, match="must return None"):

        @service(name="test.v1.WithResult")
        class WithResult:
            @method.duplex(reason="r")
            async def chat(self, channel: RpcDuplex[Turn, Turn]) -> Turn:
                """Chat."""
                return Turn(text="")

        del WithResult


def test_duplex_cannot_be_idempotent() -> None:
    with pytest.raises(ContractError, match="never replayed"):

        @service(name="test.v1.Idempotent")
        class Idempotent:
            @method(idempotent=True)
            @method.duplex(reason="r")
            async def chat(self, channel: RpcDuplex[Turn, Turn]) -> None:
                """Chat."""

        del Idempotent


async def test_interleaved_echo(client: DuplexClient) -> None:
    async with client.echo("> ") as call:
        for text in ("a", "b", "c"):
            await call.send(Turn(text=text))
            assert (await anext(call)).text == "> " + text
        await call.close_send()
        assert [turn async for turn in call] == []


async def test_reading_concurrently_avoids_credit_deadlock(
    client: DuplexClient,
) -> None:
    # More items than one credit window each way: only safe because the
    # output is read while the input is still being sent.
    call = client.echo()
    count = 500

    async def send_all() -> None:
        for index in range(count):
            await call.send(Turn(text=str(index)))
        await call.close_send()

    sender = asyncio.create_task(send_all())
    received = [turn.text async for turn in call]
    await sender
    assert received == [str(index) for index in range(count)]


async def test_server_half_close_keeps_input_open(client: DuplexClient) -> None:
    call = client.close_early()
    assert (await anext(call)).text == "only"
    for _ in range(3):
        await call.send(Turn(text="still accepted"))
    await call.close_send()
    assert [turn async for turn in call] == []


async def test_error_after_items_is_raised_from_iteration(
    client: DuplexClient,
) -> None:
    call = client.fail_after_one()
    await call.send(Turn(text="x"))
    assert (await anext(call)).text == "x"
    with pytest.raises(MeshCallError) as error:
        await anext(call)
    assert error.value.code == "app.failed"
    assert call.trailers == {"seen": "x"}


async def test_idempotency_key_is_refused() -> None:
    # The client API has no idempotency key for duplex; an interceptor that
    # smuggles one in meets the server's own check.
    async def add_key(info: ClientCallInfo, call_next: ClientCallNext) -> Any:
        info.options = dataclasses.replace(info.options, idempotency_key="k")
        return await call_next(info)

    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(services=[DuplexService()], driver=driver, access_log=False)
    await server.start()
    rpc = DuplexClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}"),
        interceptors=[add_key],
    )
    try:
        with pytest.raises(MeshCallError) as error:
            async for _ in rpc.echo():
                pass
        assert error.value.code == ErrorCode.INVALID_ARGUMENT
        assert error.value.details == {
            "source": "server",
            "reason": "idempotency_key_on_duplex",
        }
    finally:
        await rpc.stop()
        await server.stop()

"""Error model: typed subclasses, redacted validation details, fault attribution."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
from pydantic import BaseModel

from meshcall import RpcServer, method, service
from meshcall.client import ClientBase
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
from meshcall.errors import (
    DeadlineExceededError,
    ErrorCode,
    InternalError,
    InvalidArgumentError,
    MeshCallError,
    ProtocolError,
)
from meshcall.server import _access_level
from meshcall.streams import RpcServerStream

SECRET = "not-an-int-s3cr3t"


class Request(BaseModel):
    value: int


class Looks(BaseModel):
    value: str


class Result(BaseModel):
    total: int


class Empty(BaseModel):
    pass


@service(name="test.v1.ErrorService")
class ErrorService:
    """Handlers that are right, wrong, or fed wrong input."""

    @method(read_only=True)
    async def echo(self, value: int) -> Result:
        """Echo ``value``."""
        return Result(total=value)

    @method(read_only=True)
    async def bad_response(self) -> Result:
        """Break the response contract on purpose."""
        return cast(Result, {"total": "nope"})

    @method.server_stream(read_only=True)
    async def bad_items(self) -> AsyncIterator[Result]:
        """Yield one good item, then break the output contract."""
        yield Result(total=1)
        yield cast(Result, {"total": "nope"})


class ErrorClient(ClientBase):
    service_name = "test.v1.ErrorService"

    async def echo(self, request: BaseModel, **kwargs: Any) -> Result:
        return await self._unary(self.service_name, "echo", request, Result, **kwargs)

    async def bad_response(self) -> Result:
        return await self._unary(self.service_name, "bad_response", Empty(), Result)

    def bad_items(self) -> RpcServerStream[Result]:
        return self._server_stream(self.service_name, "bad_items", Empty(), Result)


@pytest.fixture
async def client() -> AsyncIterator[ErrorClient]:
    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(services=[ErrorService], driver=driver, access_log=False)
    await server.start()
    try:
        async with ErrorClient(
            WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}")
        ) as client:
            yield client
    finally:
        await server.stop()


async def test_invalid_request_is_typed_and_redacted(client: ErrorClient) -> None:
    with pytest.raises(InvalidArgumentError) as info:
        await client.echo(Looks(value=SECRET))
    error = info.value
    assert error.message == "Invalid request"
    assert error.source == "server"
    assert error.reason == "request"
    [violation] = error.violations
    assert violation.path == "value"
    assert violation.code == "int_parsing"
    assert SECRET not in repr(error.details) + str(error)
    # The connection survives a rejected call.
    assert (await client.echo(Request(value=4))).total == 4


async def test_invalid_response_is_the_servers_fault(client: ErrorClient) -> None:
    with pytest.raises(InternalError) as info:
        await client.bad_response()
    assert info.value.message == "Internal service error"
    assert info.value.reason == "invalid_response"
    assert "nope" not in repr(info.value.details)


async def test_invalid_output_item_is_the_servers_fault(client: ErrorClient) -> None:
    seen: list[int] = []
    with pytest.raises(InternalError) as info:
        async for item in client.bad_items():
            seen.append(item.total)
    assert seen == [1]
    assert info.value.reason == "invalid_output_item"


def test_from_code_dispatches_to_subclasses() -> None:
    error = MeshCallError.from_code(
        "deadline_exceeded", "late", details={"source": "router"}
    )
    assert isinstance(error, DeadlineExceededError)
    assert error.code == ErrorCode.DEADLINE_EXCEEDED
    assert error.source == "router"
    assert isinstance(MeshCallError.from_code("protocol_error", "bad"), ProtocolError)
    custom = MeshCallError.from_code("app.v1.quota", "no", retryable=True)
    assert type(custom) is MeshCallError
    assert custom.retryable


def test_access_log_levels_follow_fault() -> None:
    assert _access_level("ok", "INFO") == "INFO"
    assert _access_level("invalid_argument", "INFO") == "WARNING"
    assert _access_level("deadline_exceeded", "INFO") == "WARNING"
    assert _access_level("internal", "INFO") == "ERROR"
    assert _access_level("protocol_error", "INFO") == "ERROR"

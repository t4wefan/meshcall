from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from loguru import logger as loguru_logger
from pydantic import BaseModel

from meshcall import (
    RpcLogger,
    RpcServer,
    get_service_method_binding,
    method,
    service,
)
from meshcall.client import ClientBase
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
from meshcall.errors import ErrorCode, MeshCallError


class AccessLogResult(BaseModel):
    value: int


@service(name="test.v1.AccessLogService")
class AccessLogService:
    @method()
    async def ok(self, value: int, logger: RpcLogger) -> AccessLogResult:
        logger.info(f"injected logger value={value}")
        return AccessLogResult(value=value)

    @method()
    async def fail(self, value: int) -> AccessLogResult:
        raise MeshCallError(ErrorCode.INVALID_ARGUMENT, f"Rejected {value}")


@asynccontextmanager
async def running_server(
    *,
    access_log: bool = True,
    log_level: str | int = "INFO",
    colorize: bool = False,
) -> AsyncIterator[ClientBase]:
    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(
        services=[AccessLogService],
        driver=driver,
        access_log=access_log,
        log_level=log_level,
        colorize=colorize,
    )
    await server.start()
    client = ClientBase(WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}"))
    try:
        yield client
    finally:
        await client.stop()
        await server.stop()


def request(value: int) -> BaseModel:
    request_type = get_service_method_binding(AccessLogService, "ok").request_type
    return request_type(value=value)


@asynccontextmanager
async def captured_logs() -> AsyncIterator[list[tuple[str, str, str]]]:
    messages: list[tuple[str, str, str]] = []

    def sink(message: Any) -> None:
        messages.append(
            (
                message.record["message"],
                message.record["level"].name,
                str(message),
            )
        )

    sink_id = loguru_logger.add(
        sink,
        format="{message}",
        level="DEBUG",
        colorize=True,
    )
    try:
        yield messages
    finally:
        loguru_logger.remove(sink_id)


async def test_access_log_records_success_failures_and_injected_logger() -> None:
    binding = get_service_method_binding(AccessLogService, "ok")
    assert binding.request_fields == ("value",)
    assert binding.logger_parameter == "logger"

    async with (
        captured_logs() as messages,
        running_server(
            log_level="WARNING",
            colorize=True,
        ) as client,
    ):
        result = await client._unary(
            "test.v1.AccessLogService",
            "ok",
            request(7),
            AccessLogResult,
        )
        assert result == AccessLogResult(value=7)

        with pytest.raises(MeshCallError) as failed:
            await client._unary(
                "test.v1.AccessLogService",
                "fail",
                request(8),
                AccessLogResult,
            )
        assert failed.value.code == ErrorCode.INVALID_ARGUMENT

        with pytest.raises(MeshCallError) as missing:
            await client._unary(
                "test.v1.AccessLogService",
                "missing",
                request(9),
                AccessLogResult,
            )
        assert missing.value.code == ErrorCode.METHOD_NOT_FOUND

    access_messages = [
        (message, level, rendered)
        for message, level, rendered in messages
        if message.startswith("RPC call ")
    ]
    assert len(access_messages) == 3
    assert [
        message.split(" status=", maxsplit=1)[0] for message, _, _ in access_messages
    ] == [
        "RPC call test.v1.AccessLogService.ok",
        "RPC call test.v1.AccessLogService.fail",
        "RPC call test.v1.AccessLogService.missing",
    ]
    assert all(level == "WARNING" for _, level, _ in access_messages)
    assert all(
        "duration_ms=" in message and "call_id=" in message
        for message, _, _ in access_messages
    )
    assert any("injected logger value=7" in message for message, _, _ in messages)
    assert any("\x1b[" in rendered for _, _, rendered in access_messages)


async def test_access_log_can_be_disabled() -> None:
    async with (
        captured_logs() as messages,
        running_server(
            access_log=False,
        ) as client,
    ):
        result = await client._unary(
            "test.v1.AccessLogService",
            "ok",
            request(1),
            AccessLogResult,
        )
        assert result == AccessLogResult(value=1)

    assert not any(message.startswith("RPC call ") for message, _, _ in messages)


async def test_access_log_color_can_be_disabled() -> None:
    async with (
        captured_logs() as messages,
        running_server(
            colorize=False,
        ) as client,
    ):
        result = await client._unary(
            "test.v1.AccessLogService",
            "ok",
            request(2),
            AccessLogResult,
        )
        assert result == AccessLogResult(value=2)

    access_messages = [
        rendered for message, _, rendered in messages if message.startswith("RPC call ")
    ]
    assert len(access_messages) == 1
    assert "\x1b[" not in access_messages[0]

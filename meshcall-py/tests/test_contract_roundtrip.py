from __future__ import annotations

import sys
import types
from collections.abc import AsyncIterator
from enum import IntEnum, StrEnum

import pytest
from pydantic import BaseModel, Field, ValidationError

from meshcall import RpcInputStream, RpcServer, get_service_contract, method, service
from meshcall.codegen import render_client
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver


class Status(StrEnum):
    READY = "ready"
    BUSY = "busy"


class Priority(IntEnum):
    LOW = 1
    HIGH = 2


class AliasRequest(BaseModel):
    count: int = Field(alias="item-count")


class AliasItem(BaseModel):
    value: int = Field(alias="item-value")
    status: Status
    priorities: list[Priority] = Field(default_factory=list)


class AliasResult(BaseModel):
    total: int = Field(alias="totalValue")


@service(name="test.v1.ContractRoundTrip")
class ContractRoundTrip:
    @method()
    async def echo(self, request: AliasRequest) -> AliasResult:
        return AliasResult.model_validate({"totalValue": request.count})

    @method.server_stream()
    async def download(self, request: AliasRequest) -> AsyncIterator[AliasItem]:
        for value in range(request.count):
            yield AliasItem.model_validate(
                {"item-value": value, "status": "ready", "priorities": [1, 2]}
            )

    @method.client_stream()
    async def upload(
        self, request: AliasRequest, items: RpcInputStream[AliasItem]
    ) -> AliasResult:
        total = request.count
        async for item in items:
            total += item.value
        return AliasResult.model_validate({"totalValue": total})


@pytest.fixture
def generated(monkeypatch: pytest.MonkeyPatch):
    module = types.ModuleType("generated_contract_roundtrip")
    monkeypatch.setitem(sys.modules, module.__name__, module)
    source = render_client(get_service_contract(ContractRoundTrip))
    # Execute our own generated client to exercise its actual runtime behavior.
    exec(compile(source, "<generated-contract-roundtrip>", "exec"), module.__dict__)  # noqa: S102
    return module


def test_generated_enums_preserve_validation(generated) -> None:
    item = generated.AliasItem.model_validate(
        {"item-value": 7, "status": "ready", "priorities": [1, 2]}
    )
    assert item.model_dump(by_alias=True)["priorities"] == [1, 2]
    with pytest.raises(ValidationError):
        generated.AliasItem.model_validate({"item-value": 7, "status": "invalid"})
    with pytest.raises(ValidationError):
        generated.AliasItem.model_validate(
            {"item-value": 7, "status": "ready", "priorities": [3]}
        )


async def test_generated_aliases_round_trip_in_all_recommended_shapes(generated) -> None:
    driver = WebSocketDirectServerDriver(port=0)
    server = RpcServer(services=[ContractRoundTrip], driver=driver, access_log=False)
    await server.start()
    try:
        async with generated.ContractRoundTripClient(
            WebSocketClientDriver(f"ws://127.0.0.1:{driver.bound_port}")
        ) as client:
            request = generated.AliasRequest.model_validate({"item-count": 40})
            assert (await client.echo(request)).totalValue == 40
            items = [item async for item in client.download(request)]
            assert [item.item_value for item in items] == list(range(40))
            assert all(item.status == "ready" for item in items)

            async def upload_items():
                for item in items:
                    yield item

            assert (await client.upload(request, upload_items())).totalValue == 820
    finally:
        await server.stop()

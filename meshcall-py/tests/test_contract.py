from collections.abc import AsyncIterator

import pytest
from pydantic import BaseModel

from meshcall import (
    Balance,
    BindingKind,
    ContractError,
    RequestStyle,
    RpcDuplex,
    RpcInputStream,
    StreamKind,
    get_service_contract,
    method,
    service,
)


class Request(BaseModel):
    value: int


class Item(BaseModel):
    value: int


class Result(BaseModel):
    total: int


@service(name="test.v1.TestService", balance=Balance.least_inflight())
class TestService:
    @method()
    async def unary(self, request: Request) -> Result:
        return Result(total=request.value)

    @method.server_stream(balance=Balance.round_robin())
    async def download(self, request: Request) -> AsyncIterator[Item]:
        yield Item(value=request.value)

    @method.client_stream()
    async def upload(
        self,
        request: Request,
        items: RpcInputStream[Item],
    ) -> Result:
        return Result(total=request.value)

    @method.duplex()
    async def duplex(
        self,
        request: Request,
        channel: RpcDuplex[Item, Item],
    ) -> Result:
        return Result(total=request.value)


def test_extracts_all_method_shapes() -> None:
    contract = get_service_contract(TestService)

    assert contract.name == "test.v1.TestService"
    assert [method.stream for method in contract.methods] == [
        StreamKind.UNARY,
        StreamKind.SERVER,
        StreamKind.CLIENT,
        StreamKind.DUPLEX,
    ]
    assert [method.binding for method in contract.methods] == [
        BindingKind.INSTANCE,
        BindingKind.INSTANCE,
        BindingKind.INSTANCE,
        BindingKind.INSTANCE,
    ]
    assert [method.request_style for method in contract.methods] == [
        RequestStyle.MODEL,
        RequestStyle.MODEL,
        RequestStyle.MODEL,
        RequestStyle.MODEL,
    ]
    assert contract.methods[0].balance.kind == "least_inflight"
    assert contract.methods[1].balance.kind == "round_robin"
    assert contract.methods[3].input_item is not None
    assert contract.methods[3].output_item is not None


def test_expands_parameters_into_a_request_model() -> None:
    @service(name="test.v1.ExpandedService")
    class ExpandedService:
        @method()
        async def add(self, left: int, right: int = 1) -> Result:
            return Result(total=left + right)

    method_contract = get_service_contract(ExpandedService).methods[0]
    assert method_contract.request_style is RequestStyle.EXPANDED
    assert method_contract.request_fields == ("left", "right")
    assert method_contract.request.schema_["required"] == ["left"]
    assert method_contract.request.schema_["properties"]["right"]["default"] == 1


def test_default_unary_and_explicit_static_decorators() -> None:
    @service(name="test.v1.StaticService")
    class StaticService:
        @method().static
        async def unary(request: Request) -> Result:
            return Result(total=request.value)

    method_contract = get_service_contract(StaticService).methods[0]
    assert method_contract.stream is StreamKind.UNARY
    assert method_contract.binding is BindingKind.STATIC


def test_legacy_staticmethod_syntax_remains_supported() -> None:
    @service(name="test.v1.LegacyStaticService")
    class LegacyStaticService:
        @staticmethod
        @method()
        async def unary(request: Request) -> Result:
            return Result(total=request.value)

    method_contract = get_service_contract(LegacyStaticService).methods[0]
    assert method_contract.binding is BindingKind.STATIC


def test_rejects_untyped_expanded_parameter() -> None:
    with pytest.raises(ContractError, match="needs a type"):

        @service(name="test.v1.InvalidPayloadService")
        class InvalidPayloadService:
            @method()
            async def invalid(self, value) -> Result:
                return Result(total=value)


def test_rejects_declared_shape_mismatch() -> None:
    with pytest.raises(
        ContractError,
        match="declared as unary, but its signature implies server_stream",
    ):

        @service(name="test.v1.MismatchedService")
        class MismatchedService:
            @method.unary()
            async def download(self, request: Request) -> AsyncIterator[Item]:
                yield Item(value=request.value)

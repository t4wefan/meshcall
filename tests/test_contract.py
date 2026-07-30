from collections.abc import AsyncIterator

import pytest
from pydantic import BaseModel

from meshcall import (
    Balance,
    BindingKind,
    ContractError,
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
    @method.unary()
    async def unary(self, request: Request) -> Result:
        return Result(total=request.value)

    @method.server_stream(balance=Balance.round_robin()).static
    async def download(request: Request) -> AsyncIterator[Item]:
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
        BindingKind.STATIC,
        BindingKind.INSTANCE,
        BindingKind.INSTANCE,
    ]
    assert contract.methods[0].balance.kind == "least_inflight"
    assert contract.methods[1].balance.kind == "round_robin"
    assert contract.methods[3].input_item is not None
    assert contract.methods[3].output_item is not None


def test_legacy_method_syntax_infers_instance_binding() -> None:
    @service(name="test.v1.LegacyInstanceService")
    class LegacyInstanceService:
        @method()
        async def unary(self, request: Request) -> Result:
            return Result(total=request.value)

    contract = get_service_contract(LegacyInstanceService)
    assert contract.methods[0].stream is StreamKind.UNARY
    assert contract.methods[0].binding is BindingKind.INSTANCE


def test_legacy_staticmethod_syntax_remains_supported() -> None:
    @service(name="test.v1.LegacyService")
    class LegacyService:
        @staticmethod
        @method()
        async def unary(request: Request) -> Result:
            return Result(total=request.value)

    contract = get_service_contract(LegacyService)
    assert contract.methods[0].stream is StreamKind.UNARY
    assert contract.methods[0].binding is BindingKind.STATIC


def test_rejects_declared_shape_mismatch() -> None:
    with pytest.raises(
        ContractError,
        match="declared as unary, but its signature implies server_stream",
    ):

        @service(name="test.v1.MismatchedService")
        class MismatchedService:
            @method.unary().static
            async def download(request: Request) -> AsyncIterator[Item]:
                yield Item(value=request.value)


def test_rejects_non_pydantic_payload() -> None:
    with pytest.raises(ContractError, match="Pydantic BaseModel"):

        @service(name="test.v1.InvalidPayloadService")
        class InvalidPayloadService:
            @method.unary().static
            async def invalid(request: int) -> Result:
                return Result(total=request)

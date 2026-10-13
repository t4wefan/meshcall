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
    # Parameters are the request: a single model parameter is one field.
    assert [method.parameters for method in contract.methods] == [
        ("request",),
        ("request",),
        ("request",),
        ("request",),
    ]
    assert contract.version == "1"
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
    assert method_contract.parameters == ("left", "right")
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


def test_service_names_are_normalised_and_validated() -> None:
    from meshcall.contract import normalize_service_name

    assert normalize_service_name("chat") == ("chat.v0", "0")
    assert normalize_service_name("chat.v0") == ("chat.v0", "0")
    assert normalize_service_name("chat.v1") == ("chat.v1", "1")
    assert normalize_service_name("demo.v1.Greeting") == ("demo.v1.Greeting", "1")
    assert normalize_service_name("chat.v1.2.0") == ("chat.v1.2.0", "1.2.0")
    assert normalize_service_name("chat.v2.0.0-beta1") == (
        "chat.v2.0.0-beta1",
        "2.0.0-beta1",
    )
    for bad in ("", "v1", "chat.v1.v2", "chat.1", "1chat.v1", "chat..v1", "chat.v1.2"):
        with pytest.raises(ContractError):
            normalize_service_name(bad)

    @service("orders")
    class Orders:
        @method()
        async def ping(self) -> Result:
            return Result(total=0)

    assert get_service_contract(Orders).name == "orders.v0"


def test_descriptions_and_annotations_reach_the_contract() -> None:
    @service("billing.v2", description="Invoices and payments.")
    class Billing:
        """Ignored because an explicit description was given."""

        @method(read_only=True)
        async def get_invoice(self, invoice_id: str) -> Result:
            """Fetch one invoice by id."""
            return Result(total=0)

        @method(destructive=True, description="Void an invoice.", deprecated="cancel")
        async def void_invoice(self, invoice_id: str) -> Result:
            return Result(total=0)

        @method.server_stream(idempotent=True)
        async def history(self, invoice_id: str) -> AsyncIterator[Item]:
            yield Item(value=1)

    contract = get_service_contract(Billing)
    assert contract.description == "Invoices and payments."
    get, void, history = contract.methods
    assert get.description == "Fetch one invoice by id."
    assert get.annotations.read_only and get.annotations.idempotent
    assert not get.annotations.destructive
    assert void.description == "Void an invoice."
    assert void.annotations.destructive and not void.annotations.idempotent
    assert void.deprecated == "cancel"
    assert history.annotations.idempotent and not history.annotations.read_only
    assert history.description is None

    with pytest.raises(ContractError, match="read_only and destructive"):

        @service("bad.v1")
        class Bad:
            @method(read_only=True, destructive=True)
            async def x(self) -> Result:
                return Result(total=0)

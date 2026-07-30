import pytest
from pydantic import BaseModel

from meshcall import ContractError, method, service
from meshcall.server import ServerRuntime


class Request(BaseModel):
    value: int


class Result(BaseModel):
    value: int


@service(name="test.v1.RequiredInstanceService")
class RequiredInstanceService:
    def __init__(self, offset: int) -> None:
        self.offset = offset

    @method.unary()
    async def add(self, request: Request) -> Result:
        return Result(value=request.value + self.offset)


@service(name="test.v1.StaticOnlyService")
class StaticOnlyService:
    def __init__(self, required: str) -> None:
        raise AssertionError(required)

    @method.unary().static
    async def echo(request: Request) -> Result:
        return Result(value=request.value)


def test_required_constructor_needs_a_service_instance() -> None:
    with pytest.raises(
        ContractError,
        match="pass a service instance to RpcServer",
    ):
        ServerRuntime((RequiredInstanceService,))

    runtime = ServerRuntime((RequiredInstanceService(offset=4),))
    assert runtime.contracts[0].name == "test.v1.RequiredInstanceService"


def test_static_only_service_is_not_constructed() -> None:
    runtime = ServerRuntime((StaticOnlyService,))
    assert runtime.contracts[0].name == "test.v1.StaticOnlyService"

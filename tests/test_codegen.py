from test_contract import TestService

from meshcall.codegen import render_client
from meshcall.contract import get_service_contract


def test_generated_client_is_static_typed_python() -> None:
    source = render_client(get_service_contract(TestService))

    compile(source, "generated_test_client.py", "exec")
    assert "uv add 'meshcall>=0.1.0' 'pydantic>=2.11'" in source
    assert "class Request(BaseModel):" in source
    assert "class Item(BaseModel):" in source
    assert "class Result(BaseModel):" in source
    assert "class TestServiceClient(ClientBase):" in source
    assert "async def unary(" in source
    assert "def download(" in source
    assert "async def upload(" in source
    assert "def duplex(" in source
    assert "RpcServerStream[Item]" in source
    assert "AsyncIterable[Item]" in source

from __future__ import annotations

from pydantic import BaseModel, Field

from meshcall import method, service
from meshcall.codegen_portable import render_portable_python_client
from meshcall.codegen_typescript import render_typescript_client
from meshcall.contract import get_service_contract
from meshcall.contract_io import load_contract, render_contract


class GreetingRequest(BaseModel):
    name: str
    tags: list[str] = Field(default_factory=list)


class GreetingResult(BaseModel):
    message: str
    length: int


@service(name="test.v1.GreetingService")
class GreetingService:
    @staticmethod
    @method()
    async def greet(request: GreetingRequest) -> GreetingResult:
        return GreetingResult(
            message=f"Hello, {request.name}",
            length=len(request.name),
        )


def test_renders_typescript_client_from_python_contract() -> None:
    source = render_typescript_client(get_service_contract(GreetingService))

    assert "export interface GreetingRequest" in source
    assert "name: string;" in source
    assert "tags?: Array<string>;" in source
    assert "export interface GreetingResult" in source
    assert "export class GreetingServiceClient" in source
    assert "Promise<GreetingResult>" in source
    assert '"test.v1.GreetingService"' in source


def test_portable_contract_round_trip_and_python_generation(tmp_path) -> None:
    contract = get_service_contract(GreetingService)
    target = tmp_path / "contract.json"
    target.write_text(render_contract((contract,)), encoding="utf-8")

    loaded = load_contract(target)
    source = render_portable_python_client(loaded.services[0])

    compile(source, "generated_portable_client.py", "exec")
    assert "class GreetingRequest(BaseModel):" in source
    assert "tags: list[str] | None = None" in source
    assert "class GreetingResult(BaseModel):" in source
    assert "class GreetingServiceClient(ClientBase):" in source
    assert "async def greet(" in source

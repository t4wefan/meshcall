from __future__ import annotations

from pydantic import BaseModel, Field

from meshcall import method, service
from meshcall.codegen_portable import render_portable_python_client
from meshcall.codegen_typescript import render_typescript_client
from meshcall.contract import get_service_contract
from meshcall.contract_io import load_contract, render_contract
from meshcall.package_codegen import (
    write_python_client_package,
    write_typescript_client_package,
)


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

    assert "// External dependency: yarn add @meshcall/runtime@^0.1.0" in source
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
    assert "uv add 'meshcall>=0.1.0' 'pydantic>=2.11'" in source
    assert "class GreetingRequest(BaseModel):" in source
    assert "tags: list[str] | None = None" in source
    assert "class GreetingResult(BaseModel):" in source
    assert "class GreetingServiceClient(ClientBase):" in source
    assert "async def greet(" in source


def test_default_generators_write_complete_packages(tmp_path) -> None:
    contract = get_service_contract(GreetingService)
    python_root = write_python_client_package(contract, tmp_path / "python-client")
    typescript_root = write_typescript_client_package(
        contract,
        tmp_path / "typescript-client",
    )

    python_module = (
        python_root / "src" / "meshcall_test_v1_greetingservice_client"
    )
    assert (python_root / "pyproject.toml").is_file()
    assert (python_module / "__init__.py").is_file()
    assert (python_module / "models.py").is_file()
    assert (python_module / "client.py").is_file()

    assert (typescript_root / "package.json").is_file()
    assert (typescript_root / "tsconfig.json").is_file()
    assert (typescript_root / "src" / "index.ts").is_file()
    assert (typescript_root / "src" / "models.ts").is_file()
    assert (typescript_root / "src" / "client.ts").is_file()

from __future__ import annotations

import json

from pydantic import BaseModel, Field
from test_contract import TestService

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
    @method()
    async def greet(self, request: GreetingRequest) -> GreetingResult:
        return GreetingResult(
            message=f"Hello, {request.name}",
            length=len(request.name),
        )


class ExpandedResult(BaseModel):
    total: int


@service(name="test.v1.ExpandedService")
class ExpandedService:
    @method()
    async def add(self, left: int, right: int = 1) -> ExpandedResult:
        return ExpandedResult(total=left + right)


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


def test_generators_preserve_expanded_service_signatures(tmp_path) -> None:
    contract = get_service_contract(ExpandedService)
    target = tmp_path / "expanded.meshcall.json"
    target.write_text(render_contract((contract,)), encoding="utf-8")
    loaded = load_contract(target).services[0]
    assert loaded.methods[0].request_style == contract.methods[0].request_style
    assert loaded.methods[0].request_fields == ("left", "right")

    python_source = render_portable_python_client(contract)
    typescript_source = render_typescript_client(contract)

    compile(python_source, "generated_expanded_client.py", "exec")
    assert "class ExpandedServiceAddRequest(BaseModel):" in python_source
    assert "async def add(" in python_source
    assert "left: int," in python_source
    assert "right: int = 1," in python_source
    assert "request = ExpandedServiceAddRequest(" in python_source

    assert "export interface ExpandedServiceAddRequest" in typescript_source
    assert "public add(" in typescript_source
    assert "left: number," in typescript_source
    assert "right: number = 1," in typescript_source
    assert "left: left" in typescript_source


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
    assert (python_module / "py.typed").is_file()
    assert "[tool.uv]" in (python_root / "pyproject.toml").read_text(
        encoding="utf-8"
    )

    assert (typescript_root / "package.json").is_file()
    assert (typescript_root / "tsconfig.json").is_file()
    assert (typescript_root / "src" / "index.ts").is_file()
    assert (typescript_root / "src" / "models.ts").is_file()
    assert (typescript_root / "src" / "client.ts").is_file()
    package_json = json.loads(
        (typescript_root / "package.json").read_text(encoding="utf-8")
    )
    assert package_json["packageManager"] == "yarn@1.22.22"
    assert package_json["exports"]["."]["types"] == "./dist/index.d.ts"


def test_python_package_generation_preserves_all_stream_shapes(tmp_path) -> None:
    package_root = write_python_client_package(
        get_service_contract(TestService),
        tmp_path / "python-streaming-client",
    )
    module_root = package_root / "src" / "meshcall_test_v1_testservice_client"
    source = (module_root / "client.py").read_text(encoding="utf-8")

    compile(source, str(module_root / "client.py"), "exec")
    assert "async def unary(" in source
    assert "def download(" in source
    assert "async def upload(" in source
    assert "def duplex(" in source
    assert "RpcServerStream[Item]" in source
    assert "AsyncIterable[Item]" in source
    assert "RpcDuplexClient[Item, Item, Result]" in source

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path

from pydantic import BaseModel, Field

from meshcall import RpcInputStream, method, service
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


class StreamItem(BaseModel):
    value: int


class StreamResult(BaseModel):
    total: int


@service(name="test.v1.OneWayStreamService")
class OneWayStreamService:
    @method.server_stream()
    async def download(self, start: int) -> AsyncIterator[StreamItem]:
        yield StreamItem(value=start)

    @method.client_stream()
    async def upload(
        self,
        offset: int,
        items: RpcInputStream[StreamItem],
    ) -> StreamResult:
        total = offset
        async for item in items:
            total += item.value
        return StreamResult(total=total)


TESTS = Path(__file__).resolve().parent


def test_renders_typescript_client_from_python_contract(meshcall_cli, tmp_path) -> None:
    output = tmp_path / "greeting.ts"
    meshcall_cli(
        "generate",
        "test_multilang_codegen:GreetingService",
        "--language",
        "typescript",
        "--single-file",
        "--output",
        str(output),
        cwd=TESTS,
    )
    source = output.read_text(encoding="utf-8")

    assert "// External dependency: yarn add meshcall@^0.1.0" in source
    assert "export interface GreetingRequest" in source
    assert "name: string;" in source
    assert "tags?: Array<string>;" in source
    assert "export interface GreetingResult" in source
    assert "export class GreetingServiceClient" in source
    assert "Promise<GreetingResult>" in source
    assert '"test.v1.GreetingService"' in source


def test_generators_preserve_expanded_service_signatures(
    meshcall_cli, tmp_path
) -> None:
    contract = get_service_contract(ExpandedService)
    target = tmp_path / "expanded.meshcall.json"
    target.write_text(render_contract((contract,)), encoding="utf-8")
    loaded = load_contract(target).services[0]
    assert loaded.methods[0].parameters == ("left", "right")
    assert loaded.version == contract.version

    # Both languages, generated from the contract document rather than Python.
    py, ts = tmp_path / "expanded.py", tmp_path / "expanded.ts"
    meshcall_cli("generate", str(target), "--single-file", "--output", str(py))
    meshcall_cli(
        "generate",
        str(target),
        "--language",
        "typescript",
        "--single-file",
        "--output",
        str(ts),
    )
    python_source, typescript_source = py.read_text(), ts.read_text()

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


def test_typescript_generator_preserves_one_way_streaming_shapes(
    meshcall_cli, tmp_path
) -> None:
    output = tmp_path / "oneway.ts"
    meshcall_cli(
        "generate",
        "test_multilang_codegen:OneWayStreamService",
        "--language",
        "typescript",
        "--single-file",
        "--output",
        str(output),
        cwd=TESTS,
    )
    source = output.read_text(encoding="utf-8")

    assert "import type { MeshCallRpc, CallOptions, RpcStream" in source
    assert "public download(" in source
    assert "RpcStream<StreamItem>" in source
    assert (
        "this.rpc.serverStream<OneWayStreamServiceDownloadRequest, StreamItem>"
        in source
    )
    assert "items: AsyncIterable<StreamItem>," in source
    assert (
        "this.rpc.clientStream<OneWayStreamServiceUploadRequest, StreamItem, StreamResult>"
        in source
    )


def test_default_generators_write_complete_packages(meshcall_cli, tmp_path) -> None:
    python_root = tmp_path / "python-client"
    typescript_root = tmp_path / "typescript-client"
    meshcall_cli(
        "generate",
        "test_multilang_codegen:GreetingService",
        "--output",
        str(python_root),
        cwd=TESTS,
    )
    meshcall_cli(
        "generate",
        "test_multilang_codegen:GreetingService",
        "--language",
        "typescript",
        "--output",
        str(typescript_root),
        cwd=TESTS,
    )

    python_module = python_root / "src" / "meshcall_test_v1_greetingservice_client"
    assert (python_root / "pyproject.toml").is_file()
    assert (python_module / "__init__.py").is_file()
    assert (python_module / "models.py").is_file()
    assert (python_module / "client.py").is_file()
    assert (python_module / "py.typed").is_file()
    assert "[tool.uv]" in (python_root / "pyproject.toml").read_text(encoding="utf-8")
    source = (python_module / "models.py").read_text(encoding="utf-8")
    assert "class GreetingRequest(BaseModel):" in source
    assert "tags: list[str] | None = None" in source

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


def test_python_package_generation_preserves_all_stream_shapes(
    meshcall_cli, tmp_path
) -> None:
    package_root = tmp_path / "python-streaming-client"
    meshcall_cli(
        "generate",
        "test_contract:TestService",
        "--output",
        str(package_root),
        cwd=TESTS,
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
    assert "RpcDuplexClient[Item, Item]" in source

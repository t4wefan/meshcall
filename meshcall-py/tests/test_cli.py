from __future__ import annotations

import sys
from pathlib import Path

from meshcall.cli import main


def test_generate_imports_service_from_working_directory(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service_module = tmp_path / "demo_service.py"
    service_module.write_text(
        """
from pydantic import BaseModel
from meshcall import method, service

class Request(BaseModel):
    value: int

class Result(BaseModel):
    value: int

@service(name="demo.v1.DemoService")
class DemoService:
    @method()
    async def echo(self, request: Request) -> Result:
        return Result(value=request.value)
""".lstrip(),
        encoding="utf-8",
    )
    output = tmp_path / "generated"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "meshcall",
            "generate",
            "demo_service:DemoService",
            "--output",
            str(output),
        ],
    )

    main()

    module = output / "src" / "meshcall_demo_v1_demoservice_client"
    generated = (module / "client.py").read_text(encoding="utf-8")
    models = (module / "models.py").read_text(encoding="utf-8")
    assert (output / "pyproject.toml").is_file()
    assert (output / "README.md").is_file()
    assert "class DemoServiceClient(ClientBase):" in generated
    assert "class Request(BaseModel):" in models
    compile(generated, str(module / "client.py"), "exec")
    compile(models, str(module / "models.py"), "exec")


def test_generate_single_file_is_opt_in(tmp_path: Path, monkeypatch) -> None:
    service_module = tmp_path / "single_service.py"
    service_module.write_text(
        """
from pydantic import BaseModel
from meshcall import method, service

class Request(BaseModel):
    value: int

class Result(BaseModel):
    value: int

@service(name="demo.v1.SingleService")
class SingleService:
    @method()
    async def echo(self, request: Request) -> Result:
        return Result(value=request.value)
""".lstrip(),
        encoding="utf-8",
    )
    output = tmp_path / "generated.py"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "meshcall",
            "generate",
            "single_service:SingleService",
            "--output",
            str(output),
            "--single-file",
        ],
    )

    main()

    generated = output.read_text(encoding="utf-8")
    assert "uv add 'meshcall>=0.1.0' 'pydantic>=2.11'" in generated
    assert "class Request(BaseModel):" in generated
    assert "class Result(BaseModel):" in generated
    assert "class SingleServiceClient(ClientBase):" in generated
    compile(generated, str(output), "exec")

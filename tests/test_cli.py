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
    @method.static.unary
    async def echo(request: Request) -> Result:
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
            "demo_service:DemoService",
            "--output",
            str(output),
        ],
    )

    main()

    generated = output.read_text(encoding="utf-8")
    assert "class DemoServiceClient(ClientBase):" in generated
    compile(generated, str(output), "exec")

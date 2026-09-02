from __future__ import annotations

from pathlib import Path

from meshcall.codegen_portable import (
    render_portable_python_client,
    write_portable_python_client,
)
from meshcall.ir import ServiceContract


def render_client(
    contract: ServiceContract,
    *,
    class_name: str | None = None,
    meshcall_requirement: str = "meshcall>=0.1.0",
    pydantic_requirement: str = "pydantic>=2.11",
) -> str:
    """Render one self-contained Python module from a service contract."""
    return render_portable_python_client(
        contract,
        class_name=class_name,
        meshcall_requirement=meshcall_requirement,
        pydantic_requirement=pydantic_requirement,
    )


def write_client(
    contract: ServiceContract,
    output: str | Path,
    *,
    class_name: str | None = None,
    meshcall_requirement: str = "meshcall>=0.1.0",
    pydantic_requirement: str = "pydantic>=2.11",
) -> Path:
    """Write one self-contained Python client module atomically."""
    return write_portable_python_client(
        contract,
        output,
        class_name=class_name,
        meshcall_requirement=meshcall_requirement,
        pydantic_requirement=pydantic_requirement,
    )

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from meshcall.ir import ContractDocument, ServiceContract


def render_contract(services: Iterable[ServiceContract]) -> str:
    document = ContractDocument(services=tuple(services))
    return document.model_dump_json(by_alias=True, indent=2) + "\n"


def write_contract(
    services: Iterable[ServiceContract],
    output: str | Path,
) -> Path:
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(f"{target.suffix}.tmp")
    temporary.write_text(render_contract(services), encoding="utf-8")
    temporary.replace(target)
    return target


def load_contract(path: str | Path) -> ContractDocument:
    return ContractDocument.model_validate_json(Path(path).read_text(encoding="utf-8"))


def select_service(
    document: ContractDocument,
    service_name: str | None,
) -> ServiceContract:
    if service_name is None:
        if len(document.services) != 1:
            raise ValueError("Contract contains multiple services; pass --service")
        return document.services[0]
    for service in document.services:
        if service.name == service_name:
            return service
    raise ValueError(f"Contract does not contain service {service_name!r}")

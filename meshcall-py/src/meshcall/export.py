"""CLI helper: export ``@service`` contracts from Python modules or files.

Invoked by the Go ``meshcall`` binary (through ``MESHCALL_PYTHON``) when
``meshcall export`` or ``meshcall generate`` is given Python sources.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import importlib.util
import os
import sys
import uuid
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType

from websockets.asyncio.client import connect

from .contract import get_service_contract
from .contract_io import write_contract
from .errors import ProtocolError
from .ir import ServiceContract
from .protocol import (
    PROTOCOL_VERSION,
    HelloAckFrame,
    HelloFrame,
    RegisteredMethod,
    RegisteredService,
    ServerRegisterAckFrame,
    ServerRegisterFrame,
)
from .transport import DEFAULT_MAX_FRAME_SIZE, FrameConnection


def split_spec(spec: str) -> tuple[str, str]:
    """Split ``source[:Class]`` into ``(source, class_name)``, preserving Windows drive letters."""
    mod_part, sep, attr_part = spec.rpartition(":")
    if not sep:
        return spec, ""
    if len(mod_part) == 1 and mod_part.isalpha() and attr_part.startswith(("\\", "/")):
        return spec, ""
    if not mod_part:
        raise ValueError(f"Expected module[:Class] or path.py[:Class], got {spec!r}")
    return mod_part, attr_part


def _load_file_module(file_path: Path) -> ModuleType:
    resolved = file_path.resolve()
    if not resolved.is_file():
        raise ValueError(f"Python source file does not exist: {file_path}")
    parts = [resolved.stem]
    pkg_root = resolved.parent
    while (pkg_root / "__init__.py").is_file():
        parts.append(pkg_root.name)
        pkg_root = pkg_root.parent
    mod_name = ".".join(reversed(parts))
    root_str = str(pkg_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    if len(parts) > 1:
        return importlib.import_module(mod_name)
    spec = importlib.util.spec_from_file_location(mod_name, resolved)
    if spec is None or spec.loader is None:
        raise ValueError(f"Cannot load Python module from {resolved}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    return module


def _load_module(mod_part: str) -> ModuleType:
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    src_dir = os.path.join(cwd, "src")
    if os.path.isdir(src_dir) and src_dir not in sys.path:
        sys.path.append(src_dir)
    candidate = Path(mod_part)
    if (
        mod_part.endswith(".py")
        or "/" in mod_part
        or "\\" in mod_part
        or candidate.is_file()
    ):
        return _load_file_module(candidate)
    py_file = Path(cwd) / f"{mod_part}.py"
    try:
        return importlib.import_module(mod_part)
    except ModuleNotFoundError:
        if py_file.is_file():
            return _load_file_module(py_file)
        raise


def load_contracts(specs: Sequence[str]) -> list[ServiceContract]:
    contracts: list[ServiceContract] = []
    seen_names: set[str] = set()
    for spec in specs:
        mod_part, attr_part = split_spec(spec)
        module = _load_module(mod_part)
        if attr_part:
            if not hasattr(module, attr_part):
                raise AttributeError(
                    f"Module {mod_part!r} has no attribute {attr_part!r}"
                )
            contract = get_service_contract(getattr(module, attr_part))
            if contract.name not in seen_names:
                seen_names.add(contract.name)
                contracts.append(contract)
            continue

        own: list[ServiceContract] = []
        imported: list[ServiceContract] = []
        for value in vars(module).values():
            contract = getattr(value, "__meshcall_contract__", None)
            if not isinstance(contract, ServiceContract):
                continue
            owner_mod = getattr(value, "__module__", None) or getattr(
                type(value), "__module__", None
            )
            if owner_mod == module.__name__:
                own.append(contract)
            else:
                imported.append(contract)
        discovered = own if own else imported
        if not discovered:
            raise ValueError(f"No @service classes found in {mod_part!r}")
        for contract in discovered:
            if contract.name not in seen_names:
                seen_names.add(contract.name)
                contracts.append(contract)
    return contracts


async def register_contracts_with_router(
    contracts: Sequence[ServiceContract], router_url: str
) -> None:
    instance_id = f"export-{uuid.uuid4().hex[:12]}"
    registered = tuple(
        RegisteredService(
            name=service.name,
            methods=tuple(
                RegisteredMethod(
                    name=method.name,
                    stream=method.stream,
                    balance=method.balance,
                )
                for method in service.methods
            ),
            contract=service.model_dump(mode="json", by_alias=True),
            targeted=service.targeted,
        )
        for service in contracts
    )
    async with await connect(router_url, max_size=DEFAULT_MAX_FRAME_SIZE) as ws:
        connection = FrameConnection(
            ws,
            max_frame_size=DEFAULT_MAX_FRAME_SIZE,
            source="server",
            ping_interval=None,
            ping_timeout=None,
        )
        try:
            await connection.send(
                HelloFrame(
                    role="server",
                    peer_id=instance_id,
                    instance_id=instance_id,
                )
            )
            hello_ack = await connection.receive()
            if not isinstance(hello_ack, HelloAckFrame):
                raise ProtocolError("Expected hello.ack from WebSocket Router")
            if hello_ack.protocol != PROTOCOL_VERSION:
                raise ProtocolError(f"Unsupported protocol: {hello_ack.protocol}")
            await connection.send(
                ServerRegisterFrame(
                    instance_id=instance_id,
                    services=registered,
                )
            )
            register_ack = await connection.receive()
            if (
                not isinstance(register_ack, ServerRegisterAckFrame)
                or register_ack.instance_id != instance_id
            ):
                raise ProtocolError("Expected matching server.register.ack")
        finally:
            await connection.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m meshcall.export")
    parser.add_argument(
        "specs",
        nargs="+",
        help="module[:Class] or path/to/file.py[:Class] to export",
    )
    parser.add_argument("--output", type=Path, help="Output contract JSON file")
    parser.add_argument(
        "--router",
        type=str,
        help="Ephemeral inspect-only Router WebSocket URL to register contracts with",
    )
    args = parser.parse_args(argv)
    if args.output is None and args.router is None:
        parser.error("at least one of --output or --router is required")

    try:
        contracts = load_contracts(args.specs)
        if args.router is not None:
            asyncio.run(register_contracts_with_router(contracts, args.router))
        if args.output is not None:
            write_contract(tuple(contracts), args.output)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

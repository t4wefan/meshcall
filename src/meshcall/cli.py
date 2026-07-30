from __future__ import annotations

import argparse
import importlib
from typing import Any

from meshcall.codegen import write_client
from meshcall.contract import get_service_contract


def main() -> None:
    parser = argparse.ArgumentParser(prog="meshcall")
    subparsers = parser.add_subparsers(dest="command", required=True)
    generate = subparsers.add_parser("generate", help="generate a Python client")
    generate.add_argument("service", help="import path in module:Class form")
    generate.add_argument("--output", "-o", required=True)
    generate.add_argument("--class-name")
    args = parser.parse_args()

    if args.command == "generate":
        service_type = _import_object(args.service)
        contract = get_service_contract(service_type)
        write_client(contract, args.output, class_name=args.class_name)


def _import_object(path: str) -> Any:
    module_name, separator, object_path = path.partition(":")
    if not separator or not module_name or not object_path:
        raise SystemExit("Service must use module:Class form")
    value: Any = importlib.import_module(module_name)
    for part in object_path.split("."):
        value = getattr(value, part)
    return value


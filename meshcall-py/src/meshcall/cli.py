from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path
from typing import Any

from meshcall.codegen import write_client
from meshcall.codegen_portable import write_portable_python_client
from meshcall.codegen_typescript import write_typescript_client
from meshcall.contract import get_service_contract
from meshcall.contract_io import load_contract, select_service, write_contract
from meshcall.package_codegen import (
    write_python_client_package,
    write_typescript_client_package,
)


def main() -> None:
    parser = argparse.ArgumentParser(prog="meshcall")
    subparsers = parser.add_subparsers(dest="command", required=True)
    generate = subparsers.add_parser("generate", help="generate a client from Python")
    generate.add_argument("service", help="import path in module:Class form")
    generate.add_argument(
        "--output",
        "-o",
        required=True,
        help="package directory, or a file path when --single-file is used",
    )
    generate.add_argument("--class-name")
    generate.add_argument(
        "--single-file",
        action="store_true",
        help="generate one self-contained source file instead of a package",
    )
    generate.add_argument("--package-name")
    generate.add_argument("--package-version", default="0.1.0")
    generate.add_argument(
        "--language",
        choices=("python", "typescript"),
        default="python",
    )
    generate.add_argument("--runtime-import", default="@meshcall/runtime")
    generate.add_argument("--runtime-version", default="^0.1.0")
    generate.add_argument("--meshcall-requirement", default="meshcall>=0.1.0")

    export = subparsers.add_parser("export", help="export a portable contract")
    export.add_argument("service", help="import path in module:Class form")
    export.add_argument("--output", "-o", required=True)

    portable = subparsers.add_parser(
        "generate-contract",
        help="generate a client from a portable contract",
    )
    portable.add_argument("contract")
    portable.add_argument(
        "--output",
        "-o",
        required=True,
        help="package directory, or a file path when --single-file is used",
    )
    portable.add_argument("--service")
    portable.add_argument("--class-name")
    portable.add_argument(
        "--single-file",
        action="store_true",
        help="generate one self-contained source file instead of a package",
    )
    portable.add_argument("--package-name")
    portable.add_argument("--package-version", default="0.1.0")
    portable.add_argument(
        "--language",
        choices=("python", "typescript"),
        required=True,
    )
    portable.add_argument("--runtime-import", default="@meshcall/runtime")
    portable.add_argument("--runtime-version", default="^0.1.0")
    portable.add_argument("--meshcall-requirement", default="meshcall>=0.1.0")
    args = parser.parse_args()

    if args.command == "generate":
        service_type = _import_object(args.service)
        contract = get_service_contract(service_type)
        if args.single_file and args.language == "python":
            write_client(
                contract,
                args.output,
                class_name=args.class_name,
                meshcall_requirement=args.meshcall_requirement,
            )
        elif args.single_file:
            write_typescript_client(
                contract,
                args.output,
                class_name=args.class_name,
                runtime_import=args.runtime_import,
                runtime_version=args.runtime_version,
            )
        elif args.language == "python":
            write_python_client_package(
                contract,
                args.output,
                package_name=args.package_name,
                package_version=args.package_version,
                class_name=args.class_name,
                meshcall_requirement=args.meshcall_requirement,
            )
        else:
            write_typescript_client_package(
                contract,
                args.output,
                package_name=args.package_name,
                package_version=args.package_version,
                class_name=args.class_name,
                runtime_import=args.runtime_import,
                runtime_version=args.runtime_version,
            )
    elif args.command == "export":
        service_type = _import_object(args.service)
        write_contract((get_service_contract(service_type),), args.output)
    elif args.command == "generate-contract":
        document = load_contract(args.contract)
        contract = select_service(document, args.service)
        if args.single_file and args.language == "python":
            write_portable_python_client(
                contract,
                args.output,
                class_name=args.class_name,
                meshcall_requirement=args.meshcall_requirement,
            )
        elif args.single_file:
            write_typescript_client(
                contract,
                args.output,
                class_name=args.class_name,
                runtime_import=args.runtime_import,
                runtime_version=args.runtime_version,
            )
        elif args.language == "python":
            write_python_client_package(
                contract,
                args.output,
                package_name=args.package_name,
                package_version=args.package_version,
                class_name=args.class_name,
                meshcall_requirement=args.meshcall_requirement,
            )
        else:
            write_typescript_client_package(
                contract,
                args.output,
                package_name=args.package_name,
                package_version=args.package_version,
                class_name=args.class_name,
                runtime_import=args.runtime_import,
                runtime_version=args.runtime_version,
            )


def _import_object(path: str) -> Any:
    module_name, separator, object_path = path.partition(":")
    if not separator or not module_name or not object_path:
        raise SystemExit("Service must use module:Class form")
    project_root = str(Path.cwd())
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    value: Any = importlib.import_module(module_name)
    for part in object_path.split("."):
        value = getattr(value, part)
    return value

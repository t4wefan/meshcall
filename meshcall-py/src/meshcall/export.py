"""Export a Python service's portable contract: ``python -m meshcall.export``.

This is what the Go ``meshcall`` CLI runs (through the interpreter named by
``MESHCALL_PYTHON``) when ``meshcall generate`` is given a ``module:Class``
source. Everything else about client generation lives in the Go CLI.
"""

from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path
from typing import Any

from meshcall.contract import get_service_contract
from meshcall.contract_io import write_contract


def import_object(path: str) -> Any:
    """Import ``module:Class`` with the working directory on ``sys.path``."""
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


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m meshcall.export")
    parser.add_argument("service", help="import path in module:Class form")
    parser.add_argument("--output", "-o", required=True)
    args = parser.parse_args(argv)
    write_contract((get_service_contract(import_object(args.service)),), args.output)


if __name__ == "__main__":
    main()

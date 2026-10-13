"""The ``meshcall`` console script: run the Go CLI from a Python environment.

``uv run meshcall …`` must behave exactly like the ``meshcall`` binary that
ships with the Router release, so this script only locates (or, on first use,
downloads) that binary and replaces itself with it. ``MESHCALL_PYTHON`` tells
the CLI which interpreter resolves ``module:Class`` sources for ``generate``
and ``export``. Only the bootstrap commands ``router install`` and
``router path`` are answered here, because they may run before any binary
exists.
"""

from __future__ import annotations

import os
import subprocess
import sys

from meshcall.errors import DriverStateError
from meshcall.router_install import find_cli_binary, find_router_binary, install_router


def main() -> None:
    args = sys.argv[1:]
    if args[:2] == ["router", "install"]:
        version, destination, force = _install_args(args[2:])
        print(install_router(version, destination=destination, force=force))
        return
    if args[:2] == ["router", "path"]:
        print(find_router_binary())
        return
    try:
        binary = find_cli_binary()
    except DriverStateError:
        # First use in a fresh environment: fetch the release, then continue.
        install_router()
        binary = find_cli_binary()
    env = {**os.environ, "MESHCALL_PYTHON": sys.executable}
    if os.name == "nt":
        raise SystemExit(subprocess.call([binary, *args], env=env))
    os.execve(binary, [binary, *args], env)


def _install_args(args: list[str]) -> tuple[str | None, str | None, bool]:
    version = destination = None
    force = False
    it = iter(args)
    for arg in it:
        if arg == "--version":
            version = next(it, None)
        elif arg.startswith("--version="):
            version = arg.split("=", 1)[1]
        elif arg == "--destination":
            destination = next(it, None)
        elif arg.startswith("--destination="):
            destination = arg.split("=", 1)[1]
        elif arg == "--force":
            force = True
        else:
            raise SystemExit(f"unknown option for router install: {arg}")
    return version, destination, force

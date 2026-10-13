from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEMO_PROJECTS = ("quickstart", "py2ts", "showcase", "router", "ts2py")
for import_root in (
    PROJECT_ROOT,
    *(PROJECT_ROOT / "demo" / name / "src" for name in DEMO_PROJECTS),
    PROJECT_ROOT / "demo" / "ts2py" / "py-client" / "src",
):
    import_root_string = str(import_root)
    if import_root_string not in sys.path:
        sys.path.insert(0, import_root_string)


import os
import subprocess

import pytest

from meshcall.router_install import find_cli_binary


@pytest.fixture(scope="session")
def meshcall_cli():
    """Run the Go ``meshcall`` CLI (built by scripts/check.sh) with the current
    interpreter available for ``module:Class`` exports."""
    binary = find_cli_binary()

    def run(*args: str, cwd: str | os.PathLike[str] | None = None) -> str:
        env = {**os.environ, "MESHCALL_PYTHON": sys.executable}
        completed = subprocess.run(
            [binary, *args],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr
        return completed.stdout

    return run

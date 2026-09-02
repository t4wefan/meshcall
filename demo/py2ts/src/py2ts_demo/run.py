"""Run a Python service with its generated TypeScript client."""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path

from meshcall import RpcServer
from meshcall.drivers import WebSocketDirectServerDriver
from py2ts_demo.service import PythonGreetingService

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_ROOT = PROJECT_ROOT.parents[1]
TYPESCRIPT_RUNTIME_ROOT = REPOSITORY_ROOT / "meshcall-ts"
TYPESCRIPT_CLIENT_ROOT = PROJECT_ROOT / "ts-client"


async def run_demo() -> None:
    """Build the TypeScript side, call Python, and clean up all processes."""
    _require_node_tools()
    await _run_process(
        "yarn",
        "install",
        "--frozen-lockfile",
        cwd=TYPESCRIPT_RUNTIME_ROOT,
    )
    await _run_process("yarn", "run", "build", cwd=TYPESCRIPT_RUNTIME_ROOT)
    await _run_process(
        "yarn",
        "install",
        "--frozen-lockfile",
        cwd=TYPESCRIPT_CLIENT_ROOT,
    )
    await _run_process("yarn", "run", "build", cwd=TYPESCRIPT_CLIENT_ROOT)

    server_driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(
        services=[PythonGreetingService],
        driver=server_driver,
    )
    await server.start()
    try:
        output = await _run_process(
            "node",
            "run.mjs",
            f"ws://127.0.0.1:{server_driver.bound_port}",
            cwd=TYPESCRIPT_CLIENT_ROOT,
        )
        result = json.loads(output)
        print(f"Python service -> TypeScript client: {result}")
    finally:
        await server.stop()


def main() -> None:
    asyncio.run(run_demo())


def _require_node_tools() -> None:
    missing = [name for name in ("node", "yarn") if shutil.which(name) is None]
    if missing:
        joined = ", ".join(missing)
        raise RuntimeError(f"py2ts demo requires these executables: {joined}")


async def _run_process(*command: str, cwd: Path) -> str:
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        raise RuntimeError(
            f"Command failed ({process.returncode}): {' '.join(command)}\n"
            f"stdout:\n{stdout.decode()}\nstderr:\n{stderr.decode()}"
        )
    return stdout.decode().strip()

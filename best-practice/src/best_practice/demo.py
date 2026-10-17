"""Run the best-practice service end to end: TypeScript CLI and Go client.

Starts a ``LocalRouter`` on an ephemeral port, mounts and serves ``LlmService``
on a ``Mesh``, builds the two clients, runs them against that Router, then
drains and stops everything.
"""

from __future__ import annotations

import asyncio
import os
import shutil
from pathlib import Path

from meshcall import LocalRouter, Mesh

from .service import LlmService

ROOT = Path(__file__).resolve().parents[2]
TS_CLIENT = ROOT / "ts-client"
GO_CLIENT = ROOT / "go-client"
PROMPT = "Why should a server stream tokens instead of one big answer?"


async def run_demo() -> None:
    _require("node", "yarn", "go")
    await _run("yarn", "install", "--frozen-lockfile", "--check-files", cwd=TS_CLIENT)
    await _run("yarn", "run", "build", cwd=TS_CLIENT)
    go_binary = GO_CLIENT / "bin" / "llm-client"
    go_binary.parent.mkdir(parents=True, exist_ok=True)
    await _run("go", "build", "-o", str(go_binary), ".", cwd=GO_CLIENT)

    async with LocalRouter(port=0) as local_router:
        server_mesh = Mesh(local_router).mount(LlmService())
        async with server_mesh.serve(access_log=False):
            url = local_router.url
            env = {**os.environ, "MESHCALL_ROUTER_URL": url}

            print(f"Router {url}")
            print("1. TypeScript CLI")
            print(await _run("node", "dist/cli.js", PROMPT, cwd=TS_CLIENT, env=env))
            print("2. Go client")
            print(await _run(str(go_binary), cwd=GO_CLIENT, env=env))


def main() -> None:
    asyncio.run(run_demo())


def _require(*tools: str) -> None:
    missing = [tool for tool in tools if shutil.which(tool) is None]
    if missing:
        raise RuntimeError(f"best-practice demo requires: {', '.join(missing)}")


async def _run(
    *command: str,
    cwd: Path,
    env: dict[str, str] | None = None,
) -> str:
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=cwd,
        env=env,
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

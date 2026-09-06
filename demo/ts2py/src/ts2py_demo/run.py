"""Run a TypeScript service with its generated Python client."""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path

from meshcall_demo_ts2py_client import (
    CountRequest,
    GreetingRequest,
    SumRequest,
    TypeScriptGreetingServiceClient,
)

from meshcall.drivers import WebSocketClientDriver

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_ROOT = PROJECT_ROOT.parents[1]
TYPESCRIPT_RUNTIME_ROOT = REPOSITORY_ROOT / "meshcall-ts"
TYPESCRIPT_SERVER_ROOT = PROJECT_ROOT / "ts-server"


async def run_demo() -> None:
    """Build the TypeScript side, call it from Python, and clean up."""
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
        cwd=TYPESCRIPT_SERVER_ROOT,
    )
    await _run_process("yarn", "run", "build", cwd=TYPESCRIPT_SERVER_ROOT)

    process, port = await _start_typescript_server()
    client = TypeScriptGreetingServiceClient(
        WebSocketClientDriver(f"ws://127.0.0.1:{port}")
    )
    try:
        async with client:
            response = await client.greet(
                GreetingRequest(name="Python", repeat=2)
            )
            items = [item async for item in client.count(CountRequest(count=40))]

            async def input_items():
                for item in items:
                    yield item

            total = await client.sum(SumRequest(offset=10), input_items())
            assert len(items) == 40
            assert total.total == 790
        print(f"TypeScript service -> Python client: {response.model_dump()}")
        print(f"Streaming: received {len(items)} items, uploaded total={total.total}")
    finally:
        await client.stop()
        await _stop_typescript_server(process)


def main() -> None:
    asyncio.run(run_demo())


def _require_node_tools() -> None:
    missing = [name for name in ("node", "yarn") if shutil.which(name) is None]
    if missing:
        joined = ", ".join(missing)
        raise RuntimeError(f"ts2py demo requires these executables: {joined}")


async def _start_typescript_server() -> tuple[asyncio.subprocess.Process, int]:
    process = await asyncio.create_subprocess_exec(
        "node",
        "dist/server.js",
        cwd=TYPESCRIPT_SERVER_ROOT,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdout is not None
    ready_line = await asyncio.wait_for(process.stdout.readline(), timeout=10)
    if not ready_line:
        raise RuntimeError(await _process_failure(process))
    try:
        ready = json.loads(ready_line)
        return process, int(ready["port"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"Invalid TypeScript server readiness: {ready_line!r}") from exc


async def _stop_typescript_server(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        if process.returncode != 0:
            raise RuntimeError(await _process_failure(process))
        return
    assert process.stdin is not None
    process.stdin.write(b"stop\n")
    await process.stdin.drain()
    try:
        await asyncio.wait_for(process.wait(), timeout=10)
    except TimeoutError:
        process.kill()
        await process.wait()
        raise RuntimeError("TypeScript server did not stop cleanly") from None
    if process.returncode != 0:
        raise RuntimeError(await _process_failure(process))


async def _process_failure(process: asyncio.subprocess.Process) -> str:
    stderr = b""
    if process.stderr is not None:
        stderr = await process.stderr.read()
    return f"TypeScript server failed with {process.returncode}: {stderr.decode()}"


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

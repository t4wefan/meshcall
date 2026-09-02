from __future__ import annotations

import asyncio
import importlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from interop_service import PythonInteropService

from meshcall import RpcServer, get_service_contract
from meshcall.contract_io import load_contract
from meshcall.drivers import WebSocketClientDriver, WebSocketDirectServerDriver
from meshcall.package_codegen import (
    write_python_client_package,
    write_typescript_client_package,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
TYPESCRIPT_ROOT = REPOSITORY_ROOT / "typescript"
RUNTIME_INDEX = TYPESCRIPT_ROOT / "dist" / "src" / "index.js"
INTEROP_ROOT = Path(__file__).parent / "interop"
RUN_INTEROP = os.environ.get("MESHCALL_RUN_INTEROP") == "1"

pytestmark = pytest.mark.skipif(
    not RUN_INTEROP,
    reason="set MESHCALL_RUN_INTEROP=1 to run Node/Python package interop",
)


@pytest.fixture(scope="module", autouse=True)
def build_typescript_runtime() -> None:
    if not RUN_INTEROP:
        return
    for executable in ("node", "yarn", "uv"):
        if shutil.which(executable) is None:
            pytest.fail(f"{executable} is required for cross-language tests")
    if not (TYPESCRIPT_ROOT / "node_modules").is_dir():
        pytest.fail(
            "TypeScript dependencies are missing; run "
            "`yarn --cwd typescript install --frozen-lockfile`"
        )
    subprocess.run(
        ["yarn", "run", "build"],
        cwd=TYPESCRIPT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


async def test_python_service_with_generated_typescript_package(
    tmp_path: Path,
) -> None:
    package_root = write_typescript_client_package(
        get_service_contract(PythonInteropService),
        tmp_path / "typescript-client",
        runtime_version=f"file:{TYPESCRIPT_ROOT}",
    )
    await _run_process("yarn", "install", cwd=package_root)
    await _run_process("yarn", "run", "build", cwd=package_root)

    driver = WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    server = RpcServer(services=[PythonInteropService], driver=driver)
    await server.start()
    try:
        generated_index = package_root / "dist" / "index.js"
        installed_runtime = (
            package_root
            / "node_modules"
            / "@meshcall"
            / "runtime"
            / "dist"
            / "src"
            / "index.js"
        )
        completed = await _run_process(
            "node",
            str(INTEROP_ROOT / "typescript_client.mjs"),
            f"ws://127.0.0.1:{driver.bound_port}",
            str(generated_index),
            str(installed_runtime),
        )
    finally:
        await server.stop()

    assert json.loads(completed.stdout) == {
        "message": "Hello, TypeScript! Hello, TypeScript!",
        "handled_by": "python",
    }


async def test_typescript_service_with_generated_python_package(
    tmp_path: Path,
) -> None:
    contract_path = tmp_path / "typescript-contract.json"
    process = await asyncio.create_subprocess_exec(
        "node",
        str(INTEROP_ROOT / "typescript_server.mjs"),
        str(RUNTIME_INDEX),
        str(contract_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdout is not None
    try:
        ready_line = await asyncio.wait_for(process.stdout.readline(), timeout=10)
        if not ready_line:
            raise AssertionError(await _process_failure(process))
        port = int(json.loads(ready_line)["port"])

        document = load_contract(contract_path)
        package_root = write_python_client_package(
            document.services[0],
            tmp_path / "python-client",
        )
        await _run_process("uv", "build", "--project", str(package_root))
        wheel_files = tuple((package_root / "dist").glob("*.whl"))
        assert len(wheel_files) == 1

        module_name = "meshcall_test_v1_typescriptinteropservice_client"
        wheel_path = str(wheel_files[0])
        sys.path.insert(0, wheel_path)
        try:
            generated = importlib.import_module(module_name)
            request = generated.TypeScriptRequest(value=6, factor=7)
            client = generated.TypeScriptInteropServiceClient(
                WebSocketClientDriver(f"ws://127.0.0.1:{port}")
            )
            try:
                response = await client.multiply(request)
            finally:
                await client.stop()
        finally:
            sys.path.remove(wheel_path)
            for imported_name in tuple(sys.modules):
                if imported_name == module_name or imported_name.startswith(
                    f"{module_name}."
                ):
                    sys.modules.pop(imported_name, None)

        assert response.model_dump() == {
            "product": 42,
            "handled_by": "typescript",
        }
    finally:
        await _stop_server_process(process)


async def _run_process(
    *command: str,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        raise AssertionError(
            f"Command failed ({process.returncode}): {' '.join(command)}\n"
            f"stdout:\n{stdout.decode()}\nstderr:\n{stderr.decode()}"
        )
    return subprocess.CompletedProcess(
        command,
        process.returncode,
        stdout.decode(),
        stderr.decode(),
    )


async def _stop_server_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        if process.returncode != 0:
            raise AssertionError(await _process_failure(process))
        return
    assert process.stdin is not None
    process.stdin.write(b"stop\n")
    await process.stdin.drain()
    try:
        await asyncio.wait_for(process.wait(), timeout=10)
    except TimeoutError:
        process.kill()
        await process.wait()
        raise AssertionError("TypeScript server did not stop cleanly") from None
    if process.returncode != 0:
        raise AssertionError(await _process_failure(process))


async def _process_failure(process: asyncio.subprocess.Process) -> str:
    stderr = b""
    if process.stderr is not None:
        stderr = await process.stderr.read()
    return f"TypeScript server failed with {process.returncode}: {stderr.decode()}"

from __future__ import annotations

import asyncio
import importlib
import json
import os
import shutil
import subprocess
import sys
import uuid
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

import pytest
from interop_service import PythonInteropService
from router_fixtures import PASSWORD, write_auth_file
from test_direct import NumberRequest, NumberResult, NumberService, NumberServiceClient

from meshcall import (
    RouterAuthClient,
    RouterCredentials,
    RouterScope,
    RpcServer,
    WebSocketRouter,
    get_service_contract,
)
from meshcall.contract_io import load_contract
from meshcall.drivers import (
    WebSocketClientDriver,
    WebSocketDirectServerDriver,
    WebSocketRouterServerDriver,
)
from meshcall.errors import ErrorCode, MeshCallError
from meshcall.package_codegen import (
    write_python_client_package,
    write_typescript_client_package,
)

PYTHON_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PYTHON_PACKAGE_ROOT.parent
TYPESCRIPT_ROOT = REPOSITORY_ROOT / "meshcall-ts"
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
            "`yarn --cwd meshcall-ts install --frozen-lockfile`"
        )
    subprocess.run(
        ["yarn", "run", "build"],
        cwd=TYPESCRIPT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize("routed", [False, True])
async def test_python_service_with_generated_typescript_package(
    tmp_path: Path, routed: bool,
) -> None:
    package_root = write_typescript_client_package(
        get_service_contract(PythonInteropService),
        tmp_path / "typescript-client",
        runtime_version=f"file:{TYPESCRIPT_ROOT}",
    )
    await _run_process("yarn", "install", cwd=package_root)
    await _run_process("yarn", "run", "build", cwd=package_root)

    router = WebSocketRouter(port=0)
    if routed:
        await router.start()
    uri = f"ws://127.0.0.1:{router.bound_port}" if routed else ""
    driver = (
        WebSocketRouterServerDriver(uri)
        if routed else WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    )
    server = RpcServer(services=[PythonInteropService], driver=driver)
    await server.start()
    if isinstance(driver, WebSocketDirectServerDriver):
        uri = f"ws://127.0.0.1:{driver.bound_port}"
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
            uri,
            str(generated_index),
            str(installed_runtime),
        )
    finally:
        await server.stop()
        await router.stop()

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


@asynccontextmanager
async def running_typescript(root: Path, options: dict):
    contract_path = root / f"{uuid.uuid4().hex}.json"
    process = await asyncio.create_subprocess_exec(
        "node", str(INTEROP_ROOT / "typescript_server.mjs"),
        str(RUNTIME_INDEX), str(contract_path), json.dumps(options),
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdout is not None
    try:
        line = await asyncio.wait_for(process.stdout.readline(), timeout=10)
        if not line:
            raise AssertionError(await _process_failure(process))
        yield process, contract_path, json.loads(line)
    finally:
        await _stop_server_process(process)


@contextmanager
def generated_typescript_client(contract_path: Path, root: Path):
    contract = load_contract(contract_path).services[0]
    package = write_python_client_package(contract, root / "generated")
    name = "meshcall_test_v1_typescriptinteropservice_client"
    sys.path.insert(0, str(package / "src"))
    try:
        yield importlib.import_module(name)
    finally:
        sys.path.remove(str(package / "src"))
        for imported in tuple(sys.modules):
            if imported == name or imported.startswith(name + "."):
                sys.modules.pop(imported, None)


@pytest.mark.parametrize("routed", [False, True])
@pytest.mark.parametrize("unix", [False, True])
async def test_generated_python_streams_to_typescript_over_direct_and_router(
    tmp_path: Path, routed: bool, unix: bool,
) -> None:
    socket = Path("/tmp") / f"meshcall-interop-{uuid.uuid4().hex}.sock"
    router = WebSocketRouter(unix_path=socket) if unix else WebSocketRouter(port=0)
    options: dict = {}
    if routed:
        await router.start()
        endpoint = {"unixPath": str(socket)} if unix else f"ws://127.0.0.1:{router.bound_port}"
        options = {"router": {"endpoint": endpoint}}
    elif unix:
        options = {"unixPath": str(socket)}
    try:
        async with (
            asyncio.timeout(15),
            running_typescript(tmp_path, options) as (_, contract, ready),
        ):
            driver = (
                WebSocketClientDriver(unix_path=socket)
                if unix else WebSocketClientDriver(
                    f"ws://127.0.0.1:{router.bound_port if routed else ready['port']}"
                )
            )
            with generated_typescript_client(contract, tmp_path) as generated:
                async with generated.TypeScriptInteropServiceClient(driver) as client:
                    request = generated.TypeScriptRequest(value=64, factor=2)
                    assert (await client.multiply(request)).product == 128
                    items = [item async for item in client.download(request)]
                    assert [item.value for item in items] == list(range(0, 128, 2))

                    async def input_items():
                        for value in range(64):
                            yield generated.TypeScriptItem(value=value, handled_by="python")

                    assert (await client.upload(request, input_items())).product == 4096
    finally:
        await router.stop()
    if unix:
        assert not socket.exists()


async def test_typescript_instances_balance_pin_streams_and_clean_disconnects(
    tmp_path: Path,
) -> None:
    router = WebSocketRouter(port=0)
    await router.start()
    uri = f"ws://127.0.0.1:{router.bound_port}"
    try:
        async with (
            asyncio.timeout(15),
            running_typescript(
                tmp_path, {"router": {"endpoint": uri, "instanceId": "ts-a"}},
            ) as (process_a, contract, _),
            running_typescript(
                tmp_path, {"router": {"endpoint": uri, "instanceId": "ts-b"}},
            ) as (process_b, _, _),
        ):
            with generated_typescript_client(contract, tmp_path) as generated:
                async with generated.TypeScriptInteropServiceClient(
                    WebSocketClientDriver(uri)
                ) as client:
                    request = generated.TypeScriptRequest(value=64, factor=1)
                    instances = [(await client.multiply(request)).handled_by for _ in range(4)]
                    assert instances == ["ts-a", "ts-b", "ts-a", "ts-b"]
                    for expected in ("ts-a", "ts-b"):
                        items = [item async for item in client.download(request)]
                        assert len(items) == 64
                        assert {item.handled_by for item in items} == {expected}
                    pending = client.download(
                        generated.TypeScriptRequest(value=100_000, factor=1)
                    )
                    first = await anext(pending)
                    await _stop_server_process(
                        process_a if first.handled_by == "ts-a" else process_b
                    )
                    with pytest.raises(MeshCallError) as error:
                        _ = [item async for item in pending]
                    assert error.value.code == ErrorCode.UNAVAILABLE
                    # Route cleanup is verified in Go; the SDK only owns a process.
                    assert router.is_running
    finally:
        await router.stop()


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


async def _auth_peer(options: dict[str, object]) -> dict[str, object]:
    process = await asyncio.create_subprocess_exec(
        "node", str(INTEROP_ROOT / "router_auth_peer.mjs"), str(RUNTIME_INDEX),
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate((json.dumps(options) + "\n").encode()), 10,
        )
        assert process.returncode == 0, stderr.decode()
        return json.loads(stdout)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


@pytest.mark.parametrize("launcher", ["python", "typescript"])
async def test_go_router_launchers_and_service_scoped_tokens_interoperate(
    tmp_path: Path, launcher: str,
) -> None:
    name = get_service_contract(NumberService).name
    auth_file = write_auth_file(tmp_path / "auth.json", name)
    router = WebSocketRouter(auth_file=auth_file)
    node: asyncio.subprocess.Process | None = None
    try:
        if launcher == "python":
            await router.start()
            port = router.bound_port
        else:
            node = await asyncio.create_subprocess_exec(
                "node", str(INTEROP_ROOT / "router_auth_peer.mjs"), str(RUNTIME_INDEX),
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            assert node.stdin is not None and node.stdout is not None
            node.stdin.write((json.dumps({"action": "router", "authFile": str(auth_file)}) + "\n").encode())
            await node.stdin.drain()
            ready = json.loads(await asyncio.wait_for(node.stdout.readline(), 10))
            assert ready["pid"] != node.pid
            port = ready["port"]
        uri = f"ws://127.0.0.1:{port}"
        credentials = RouterCredentials(username="issuer", password=PASSWORD)
        server = RpcServer(services=[NumberService], access_log=False,
                           driver=WebSocketRouterServerDriver(uri, auth=credentials))
        await server.start()
        try:
            async with RouterAuthClient(WebSocketClientDriver(uri, auth=credentials)) as management:
                token = await management.issue_token(scopes=[RouterScope(service=name, methods=["*"])])
                result = await _auth_peer({"action": "call", "endpoint": uri,
                                           "auth": {"token": token.token}, "service": name})
                assert result == {"unary": {"total": 7}, "count": 64, "upload": {"total": 2023}}
                node_token = await _auth_peer({"action": "issue", "endpoint": uri,
                                              "auth": {"username": "issuer", "password": PASSWORD}, "service": name})
                async with NumberServiceClient(WebSocketClientDriver(
                    uri, auth=RouterCredentials(token=str(node_token["token"])),
                )) as client:
                    assert await client.unary(NumberRequest(value=12)) == NumberResult(total=12)
                assert await management.revoke_token(str(node_token["token_id"]))
        finally:
            await server.stop()
    finally:
        await router.stop()
        if node is not None:
            if node.stdin is not None:
                node.stdin.close()
            try:
                await asyncio.wait_for(node.wait(), 5)
            except TimeoutError:
                node.kill()
                await node.wait()

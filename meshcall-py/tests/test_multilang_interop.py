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
from test_local_transport import WINDOWS, local_endpoint

from meshcall import (
    RouterAuthClient,
    RouterCredentials,
    RouterScope,
    RpcServer,
    WebSocketRouter,
    get_service_contract,
)
from meshcall.contract_io import write_contract
from meshcall.drivers import (
    WebSocketClientDriver,
    WebSocketDirectServerDriver,
    WebSocketRouterServerDriver,
)
from meshcall.errors import ErrorCode, MeshCallError
from meshcall.router_install import find_cli_binary

PYTHON_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PYTHON_PACKAGE_ROOT.parent
TYPESCRIPT_ROOT = REPOSITORY_ROOT / "meshcall-ts"
GO_ROOT = REPOSITORY_ROOT / "meshcall-go"
RUNTIME_INDEX = TYPESCRIPT_ROOT / "dist" / "src" / "index.js"
INTEROP_ROOT = Path(__file__).parent / "interop"
RUN_INTEROP = os.environ.get("MESHCALL_RUN_INTEROP") == "1"


def _tool(name: str) -> str:
    """The full path of a command: on Windows yarn and npm are .cmd scripts,
    which CreateProcess finds only by their full name."""
    return shutil.which(name) or name


def _generate(*args: str) -> None:
    """Generate a client package with the Go CLI (built by scripts/check.sh)."""
    env = {**os.environ, "MESHCALL_PYTHON": sys.executable}
    completed = subprocess.run(
        [find_cli_binary(), "generate", *args],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


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
        [_tool("yarn"), "run", "build"],
        cwd=TYPESCRIPT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize("routed", [False, True])
async def test_python_service_with_generated_typescript_package(
    tmp_path: Path,
    routed: bool,
) -> None:
    package_root = tmp_path / "typescript-client"
    contract_path = tmp_path / "python-interop.meshcall.json"
    write_contract((get_service_contract(PythonInteropService),), contract_path)
    _generate(
        str(contract_path),
        "--language",
        "typescript",
        "--runtime-version",
        f"file:{TYPESCRIPT_ROOT}",
        "--output",
        str(package_root),
    )
    await _run_process("yarn", "install", cwd=package_root)
    await _run_process("yarn", "run", "build", cwd=package_root)

    router = WebSocketRouter(port=0)
    if routed:
        await router.start()
    uri = f"ws://127.0.0.1:{router.bound_port}" if routed else ""
    driver = (
        WebSocketRouterServerDriver(uri)
        if routed
        else WebSocketDirectServerDriver(host="127.0.0.1", port=0)
    )
    server = RpcServer(services=[PythonInteropService], driver=driver)
    await server.start()
    if isinstance(driver, WebSocketDirectServerDriver):
        uri = f"ws://127.0.0.1:{driver.bound_port}"
    try:
        generated_index = package_root / "dist" / "index.js"
        installed_runtime = (
            package_root / "node_modules" / "meshcall" / "dist" / "src" / "index.js"
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
        _tool("node"),
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

        package_root = tmp_path / "python-client"
        _generate(
            str(contract_path), "--language", "python", "--output", str(package_root)
        )
        await _run_process("uv", "build", "--project", str(package_root))
        wheel_files = tuple((package_root / "dist").glob("*.whl"))
        assert len(wheel_files) == 1

        module_name = "meshcall_test_v1_typescriptinteropservice_client"
        wheel_path = str(wheel_files[0])
        sys.path.insert(0, wheel_path)
        try:
            generated = importlib.import_module(module_name)
            client = generated.TypeScriptInteropServiceClient(
                WebSocketClientDriver(f"ws://127.0.0.1:{port}")
            )
            try:
                response = await client.multiply(6, 7)
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
        _tool("node"),
        str(INTEROP_ROOT / "typescript_server.mjs"),
        str(RUNTIME_INDEX),
        str(contract_path),
        json.dumps(options),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
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
    package = root / "generated"
    _generate(str(contract_path), "--language", "python", "--output", str(package))
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
@pytest.mark.parametrize("local", [False, True])
async def test_generated_python_streams_to_typescript_over_direct_and_router(
    tmp_path: Path,
    routed: bool,
    local: bool,
) -> None:
    # The local transport is a Unix socket, or a named pipe on Windows.
    py_local = local_endpoint()
    socket = Path(py_local.get("unix_path", "/nonexistent"))
    pipe = py_local.get("pipe")
    ts_local = {"pipe": pipe} if pipe is not None else {"unixPath": str(socket)}
    router = WebSocketRouter(**py_local) if local else WebSocketRouter(port=0)
    options: dict = {}
    if routed:
        await router.start()
        endpoint = ts_local if local else f"ws://127.0.0.1:{router.bound_port}"
        options = {"router": {"endpoint": endpoint}}
    elif local:
        options = ts_local
    try:
        async with (
            asyncio.timeout(15),
            running_typescript(tmp_path, options) as (_, contract, ready),
        ):
            driver = (
                WebSocketClientDriver(**py_local)
                if local
                else WebSocketClientDriver(
                    f"ws://127.0.0.1:{router.bound_port if routed else ready['port']}"
                )
            )
            with generated_typescript_client(contract, tmp_path) as generated:
                async with generated.TypeScriptInteropServiceClient(driver) as client:
                    assert (await client.multiply(64, 2)).product == 128
                    items = [item async for item in client.download(64, 2)]
                    assert [item.value for item in items] == list(range(0, 128, 2))

                    async def input_items():
                        for value in range(64):
                            yield generated.TypeScriptItem(
                                value=value, handled_by="python"
                            )

                    assert (await client.upload(input_items(), 64, 2)).product == 4096

                    async with client.exchange(0, 3) as exchange:
                        for value in range(5):
                            await exchange.send(
                                generated.TypeScriptItem(
                                    value=value, handled_by="python"
                                )
                            )
                            assert (await anext(exchange)).value == value * 3
                        await exchange.close_send()
                        assert [item async for item in exchange] == []

                    assert [t.seq async for t in client.ticks()] == list(range(5))
                    assert [t.seq async for t in client.ticks(2)] == [3, 4]
    finally:
        await router.stop()
    if local and not WINDOWS:
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
                tmp_path,
                {"router": {"endpoint": uri, "instanceId": "ts-a"}},
            ) as (process_a, contract, _),
            running_typescript(
                tmp_path,
                {"router": {"endpoint": uri, "instanceId": "ts-b"}},
            ) as (process_b, _, _),
        ):
            with generated_typescript_client(contract, tmp_path) as generated:
                async with generated.TypeScriptInteropServiceClient(
                    WebSocketClientDriver(uri)
                ) as client:
                    instances = [
                        (await client.multiply(64, 1)).handled_by for _ in range(4)
                    ]
                    assert instances == ["ts-a", "ts-b", "ts-a", "ts-b"]
                    for expected in ("ts-a", "ts-b"):
                        items = [item async for item in client.download(64, 1)]
                        assert len(items) == 64
                        assert {item.handled_by for item in items} == {expected}
                    pending = client.download(100_000, 1)
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
        _tool(command[0]),
        *command[1:],
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
        _tool("node"),
        str(INTEROP_ROOT / "router_auth_peer.mjs"),
        str(RUNTIME_INDEX),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate((json.dumps(options) + "\n").encode()),
            10,
        )
        assert process.returncode == 0, stderr.decode()
        return json.loads(stdout)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


@pytest.mark.parametrize("launcher", ["python", "typescript"])
async def test_go_router_launchers_and_service_scoped_tokens_interoperate(
    tmp_path: Path,
    launcher: str,
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
                _tool("node"),
                str(INTEROP_ROOT / "router_auth_peer.mjs"),
                str(RUNTIME_INDEX),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            assert node.stdin is not None and node.stdout is not None
            node.stdin.write(
                (
                    json.dumps({"action": "router", "authFile": str(auth_file)}) + "\n"
                ).encode()
            )
            await node.stdin.drain()
            ready = json.loads(await asyncio.wait_for(node.stdout.readline(), 10))
            assert ready["pid"] != node.pid
            port = ready["port"]
        uri = f"ws://127.0.0.1:{port}"
        credentials = RouterCredentials(username="issuer", password=PASSWORD)
        server = RpcServer(
            services=[NumberService],
            access_log=False,
            driver=WebSocketRouterServerDriver(uri, auth=credentials),
        )
        await server.start()
        try:
            async with RouterAuthClient(
                WebSocketClientDriver(uri, auth=credentials)
            ) as management:
                token = await management.issue_token(
                    scopes=[RouterScope(service=name, methods=["*"])]
                )
                result = await _auth_peer(
                    {
                        "action": "call",
                        "endpoint": uri,
                        "auth": {"token": token.token},
                        "service": name,
                    }
                )
                assert result == {
                    "unary": {"total": 7},
                    "count": 64,
                    "upload": {"total": 2023},
                }
                node_token = await _auth_peer(
                    {
                        "action": "issue",
                        "endpoint": uri,
                        "auth": {"username": "issuer", "password": PASSWORD},
                        "service": name,
                    }
                )
                async with NumberServiceClient(
                    WebSocketClientDriver(
                        uri,
                        auth=RouterCredentials(token=str(node_token["token"])),
                    )
                ) as client:
                    assert await client.unary(NumberRequest(value=12)) == NumberResult(
                        total=12
                    )
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


def _go_required() -> None:
    if shutil.which("go") is None:
        pytest.fail("go is required for cross-language tests with the Go runtime")


@contextmanager
def generated_python_client(contract_path: Path, root: Path, module: str):
    package = root / f"generated-{uuid.uuid4().hex}"
    _generate(str(contract_path), "--language", "python", "--output", str(package))
    sys.path.insert(0, str(package / "src"))
    try:
        yield importlib.import_module(module)
    finally:
        sys.path.remove(str(package / "src"))
        for imported in tuple(sys.modules):
            if imported == module or imported.startswith(module + "."):
                sys.modules.pop(imported, None)


async def test_go_service_with_generated_python_client(tmp_path: Path) -> None:
    _go_required()
    router = WebSocketRouter(port=0)
    await router.start()
    contract = tmp_path / "go.meshcall.json"
    process = await asyncio.create_subprocess_exec(
        _tool("go"),
        "run",
        "./internal/interop/server",
        f"ws://127.0.0.1:{router.bound_port}",
        str(contract),
        cwd=GO_ROOT,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdout is not None
    try:
        line = await asyncio.wait_for(process.stdout.readline(), timeout=180)
        if not line:
            raise AssertionError(await _process_failure(process))
        module = "meshcall_test_v1_gointeropservice_client"
        with generated_python_client(contract, tmp_path, module) as generated:
            driver = WebSocketClientDriver(f"ws://127.0.0.1:{router.bound_port}")
            async with generated.GoInteropServiceClient(driver) as client:
                result = await client.multiply(6, 7)
                assert (result.product, result.handled_by) == (42, "go")
                items = [item.value async for item in client.download(4, 3)]
                assert items == [0, 3, 6, 9]

                async def input_items():
                    for value in range(1, 4):
                        yield generated.Item(value=value, handled_by="python")

                assert (await client.upload(input_items(), 10, 2)).product == 22
                assert [t.seq async for t in client.ticks()] == [0, 1, 2, 3, 4]
                assert [t.seq async for t in client.ticks(2)] == [3, 4]
                async with client.exchange(0, 5) as exchange:
                    await exchange.send(generated.Item(value=2, handled_by="python"))
                    assert (await anext(exchange)).value == 10
                    await exchange.close_send()
                    assert [item async for item in exchange] == []
    finally:
        await _stop_server_process(process)
        await router.stop()


GO_CLIENT_MAIN = """package main

import (
\t"context"
\t"encoding/json"
\t"io"
\t"os"

\tmeshcall "github.com/t4wefan/meshcall/meshcall-go"
\t"github.com/t4wefan/meshcall/meshcall-go/testdata/{dir}/pyclient"
\t"github.com/t4wefan/meshcall/meshcall-go/testdata/{dir}/tsclient"
)

func must[T any](v T, err error) T {{
\tif err != nil {{
\t\tpanic(err)
\t}}
\treturn v
}}

func ptr[T any](v T) *T {{ return &v }}

func main() {{
\tctx := context.Background()
\trpc := must(meshcall.NewClient(meshcall.ClientOptions{{}}, os.Args[1]))
\tdefer rpc.Close()
\tout := map[string]any{{}}

\tpy := pyclient.NewPythonInteropServiceClient(rpc)
\tgreet := must(py.Greet(ctx, pyclient.PythonInteropServiceGreetRequest{{Name: "Go", Repeat: ptr(int64(2))}}))
\tout["greet"] = greet.Message
\tdownload := must(py.Download(ctx, pyclient.PythonInteropServiceDownloadRequest{{Count: 3}}))
\tvar values []int64
\tfor item, err := range download.All() {{
\t\tvalues = append(values, must(item, err).ItemValue)
\t}}
\tout["download"] = values
\tupload := must(py.Upload(ctx, pyclient.PythonInteropServiceUploadRequest{{Offset: 7}}, func(yield func(pyclient.PythonItem) bool) {{
\t\tfor _, v := range []int64{{1, 2, 3}} {{
\t\t\tif !yield(pyclient.PythonItem{{ItemValue: v, Status: "ready"}}) {{
\t\t\t\treturn
\t\t\t}}
\t\t}}
\t}}))
\tout["upload"] = upload.Total
\tevents := must(py.Events(ctx, pyclient.PythonInteropServiceEventsRequest{{Since: ptr(int64(2))}}))
\tvar seqs []int64
\tfor event, err := range events.All() {{
\t\tseqs = append(seqs, must(event, err).Seq)
\t}}
\tout["events"] = seqs
\texchange := must(py.Exchange(ctx, pyclient.PythonInteropServiceExchangeRequest{{Offset: 100}}))
\t_ = exchange.Send(pyclient.PythonItem{{ItemValue: 1, Status: "busy"}})
\tout["exchange"] = must(exchange.Recv()).ItemValue
\t_ = exchange.CloseSend()
\tif _, err := exchange.Recv(); err != io.EOF {{
\t\tpanic(err)
\t}}

\tts := tsclient.NewTypeScriptInteropServiceClient(rpc)
\tproduct := must(ts.Multiply(ctx, tsclient.TypeScriptRequest{{Value: 6, Factor: 7}}))
\tout["multiply"] = []any{{product.Product, product.HandledBy}}
\tticks := must(ts.Ticks(ctx, tsclient.TypeScriptTicks{{Since: ptr(int64(1))}}))
\tvar tickSeqs []int64
\tfor tick, err := range ticks.All() {{
\t\ttickSeqs = append(tickSeqs, must(tick, err).Seq)
\t}}
\tout["ticks"] = tickSeqs
\t_ = json.NewEncoder(os.Stdout).Encode(out)
}}
"""


async def test_generated_go_client_calls_python_and_typescript_services(
    tmp_path: Path,
) -> None:
    _go_required()
    router = WebSocketRouter(port=0)
    await router.start()
    url = f"ws://127.0.0.1:{router.bound_port}"
    server = RpcServer(
        services=[PythonInteropService], driver=WebSocketRouterServerDriver(url)
    )
    await server.start()
    py_contract = tmp_path / "python.meshcall.json"
    write_contract((get_service_contract(PythonInteropService),), py_contract)
    work_name = f"interop-{uuid.uuid4().hex[:12]}"
    work = GO_ROOT / "testdata" / work_name
    try:
        async with running_typescript(tmp_path, {"router": {"endpoint": url}}) as (
            _,
            ts_contract,
            _,
        ):
            _generate(
                str(py_contract),
                "--language",
                "go",
                "--package-name",
                "pyclient",
                "--output",
                str(work / "pyclient"),
            )
            _generate(
                str(ts_contract),
                "--language",
                "go",
                "--package-name",
                "tsclient",
                "--output",
                str(work / "tsclient"),
            )
            (work / "main.go").write_text(GO_CLIENT_MAIN.format(dir=work_name))
            completed = await _run_process(
                "go", "run", f"./testdata/{work_name}", url, cwd=GO_ROOT
            )
    finally:
        shutil.rmtree(work, ignore_errors=True)
        await server.stop()
        await router.stop()
    assert json.loads(completed.stdout) == {
        "greet": "Hello, Go! Hello, Go!",
        "download": [0, 1, 2],
        "upload": 13,
        "events": [3, 4],
        "exchange": 101,
        "multiply": [42, "typescript"],
        "ticks": [2, 3, 4],
    }

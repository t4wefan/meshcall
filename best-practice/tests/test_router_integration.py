from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from meshcall import RouterAuthClient, RouterScope, RpcServer, WebSocketRouter
from meshcall.drivers import WebSocketClientDriver, WebSocketRouterServerDriver
from meshcall.errors import MeshCallError
from websockets.exceptions import InvalidStatus

from best_practice.config import SERVICE_NAME, read_credentials
from best_practice.init_router import initialize
from best_practice.service import LlmService

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    os.environ.get("MESHCALL_RUN_INTEROP") != "1",
    reason="set MESHCALL_RUN_INTEROP=1 for short-lived Go/Python/Node integration",
)


async def command(
    *args: str,
    uri: str,
    credentials: Path,
) -> tuple[int, str, str]:
    process = await asyncio.create_subprocess_exec(
        *args,
        cwd=ROOT,
        env={
            **os.environ,
            "MESHCALL_ROUTER_URL": uri,
            "MESHCALL_CLIENT_CREDENTIALS": str(credentials),
        },
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    assert process.returncode is not None
    return process.returncode, stdout.decode(), stderr.decode()


async def cli(uri: str, credentials: Path) -> tuple[int, str, str]:
    return await command(
        "node",
        str(ROOT / "ts-client/dist/cli.js"),
        "Hello Router",
        uri=uri,
        credentials=credentials,
    )


@asynccontextmanager
async def worker(uri: str, credentials: Path) -> AsyncIterator[None]:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "best_practice.server",
        cwd=ROOT,
        env={
            **os.environ,
            "MESHCALL_ROUTER_URL": uri,
            "MESHCALL_WORKER_CREDENTIALS": str(credentials),
            "MESHCALL_INSTANCE_ID": "llm-python-1",
        },
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdout is not None and process.stderr is not None
    diagnostics = asyncio.create_task(process.stderr.read())
    try:
        ready = await asyncio.wait_for(process.stdout.readline(), 10)
        assert b"service registered" in ready, "Python service did not register"
        yield
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 5)
            except TimeoutError:
                process.kill()
                await process.wait()
        await diagnostics


async def test_cli_accounts_scoped_tokens_revocation_and_single_instance(
    tmp_path: Path,
) -> None:
    binary = Path(os.environ["MESHCALL_ROUTER_BINARY"])
    directory = tmp_path / "accounts"
    initialize(directory, binary)
    async with WebSocketRouter(
        binary_path=binary,
        auth_file=directory / "router-auth.json",
    ) as router:
        uri = f"ws://127.0.0.1:{router.bound_port}"
        async with worker(uri, directory / "worker.json"):
            code, stdout, stderr = await cli(uri, directory / "client.json")
            assert code == 0, stderr
            assert "prompt chunks: 2" in stdout  # client stream
            assert "I received: Hello Router" in stdout  # server stream
            assert "(2 messages)" in stdout  # unary + process-local session state
            code, stdout, stderr = await cli(uri, directory / "worker.json")
            # Node maps a policy-close during hello to its transport error.
            assert code != 0 and "[unavailable]" in stderr
            assert "session " not in stdout
            # Client account cannot register or become a token issuer.
            denied = RpcServer(
                services=[LlmService()],
                driver=WebSocketRouterServerDriver(
                    uri,
                    instance_id="unauthorized",
                    auth=read_credentials(directory / "client.json"),
                ),
            )
            try:
                with pytest.raises(ConnectionError):
                    await denied.start()
            finally:
                await denied.stop()
            async with RouterAuthClient(
                WebSocketClientDriver(
                    uri,
                    auth=read_credentials(directory / "client.json"),
                )
            ) as client:
                with pytest.raises(MeshCallError) as error:
                    await client.issue_token(
                        scopes=[
                            RouterScope(service=SERVICE_NAME, methods=["count_tokens"])
                        ]
                    )
                assert error.value.code == "permission_denied"
            with pytest.raises(InvalidStatus) as rejected:
                async with RouterAuthClient(WebSocketClientDriver(uri)) as anonymous:
                    await anonymous.whoami()
            assert rejected.value.response.status_code == 401
            # Exercise the actual token command and the actual TS CLI.
            token_file = directory / "token.json"
            prefix = [
                sys.executable,
                "-m",
                "best_practice.tokens",
                "--credentials",
                str(directory / "issuer.json"),
            ]
            code, issued, stderr = await command(
                *prefix,
                "issue",
                "--output",
                str(token_file),
                uri=uri,
                credentials=token_file,
            )
            assert code == 0, stderr
            token = read_credentials(token_file).token
            assert token and token not in issued + stderr
            token_id = next(
                line.split("=", 1)[1]
                for line in issued.splitlines()
                if line.startswith("token_id=")
            )
            code, stdout, stderr = await cli(uri, token_file)
            assert code == 0, stderr
            assert "I received: Hello Router" in stdout
            code, revoked, stderr = await command(
                *prefix,
                "revoke",
                token_id,
                uri=uri,
                credentials=token_file,
            )
            assert code == 0 and "revoked=true" in revoked, stderr
            code, stdout, stderr = await cli(uri, token_file)
            assert code != 0 and "401" in stderr
            assert token not in stdout + stderr
            limited_file = directory / "limited.json"
            code, _, stderr = await command(
                *prefix,
                "issue",
                "--output",
                str(limited_file),
                "--method",
                "count_tokens",
                uri=uri,
                credentials=limited_file,
            )
            assert code == 0, stderr
            code, _, stderr = await cli(uri, limited_file)
            assert code != 0 and "permission" in stderr.lower()
            async with RouterAuthClient(
                WebSocketClientDriver(
                    uri,
                    auth=read_credentials(limited_file),
                )
            ) as scoped:
                identity = await scoped.whoami()
                assert identity.call == [f"{SERVICE_NAME}/count_tokens"]
                assert identity.register_services == []
            # Independent session stores must not be balanced randomly.
            second = RpcServer(
                services=[LlmService()],
                driver=WebSocketRouterServerDriver(
                    uri,
                    instance_id="llm-python-2",
                    auth=read_credentials(directory / "worker.json"),
                ),
            )
            try:
                await second.start()
                code, _, stderr = await cli(uri, directory / "client.json")
                assert code != 0 and "Balance is disabled" in stderr
            finally:
                await second.stop()


async def test_cli_refuses_an_anonymous_router(tmp_path: Path) -> None:
    credentials = tmp_path / "client.json"
    credentials.write_text('{"username":"client","password":"example-only"}')
    async with WebSocketRouter(
        binary_path=os.environ["MESHCALL_ROUTER_BINARY"]
    ) as router:
        code, stdout, stderr = await cli(
            f"ws://127.0.0.1:{router.bound_port}", credentials
        )
        assert code != 0 and "Router accounts are not configured" in stderr
        assert "session " not in stdout

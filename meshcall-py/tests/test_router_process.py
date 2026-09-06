from __future__ import annotations

import asyncio
import os
import signal
import socket
import sys
from pathlib import Path

import pytest

from meshcall import WebSocketRouter
from meshcall.errors import DriverStateError
from meshcall.router import _router_binary


async def test_go_router_process_lifecycle_and_failed_bind_retry() -> None:
    router = WebSocketRouter(port=0)
    async with router:
        first_pid = router.pid
        assert first_pid is not None and first_pid != os.getpid()
        await router.start()
        assert router.pid == first_pid
        other = WebSocketRouter(port=router.bound_port)
        try:
            with pytest.raises(DriverStateError, match="listen"):
                await other.start()
            assert not other.is_running and other.pid is None
            await router.stop()
            await other.start()
            assert other.is_running
        finally:
            await other.stop()
    await router.stop()
    with pytest.raises(DriverStateError, match="not running"):
        _ = router.bound_port
    await router.start()
    try:
        assert router.pid != first_pid
        assert router.pid is not None
        os.kill(router.pid, signal.SIGTERM)
        assert await asyncio.wait_for(router.wait_closed(), 5) == 0
        assert not router.is_running
    finally:
        await router.stop()


async def test_invalid_binary_and_auth_configuration_fail_cleanly(
    tmp_path: Path,
) -> None:
    missing = WebSocketRouter(binary_path=tmp_path / "missing")
    with pytest.raises(DriverStateError, match="not executable"):
        await missing.start()
    assert not missing.is_running
    path = tmp_path / "auth.json"
    path.write_text('{"users":[]}')
    router = WebSocketRouter(auth_file=path)
    with pytest.raises(DriverStateError, match="at least one user"):
        await router.start()
    assert router.pid is None


async def test_startup_timeout_reaps_child(tmp_path: Path) -> None:
    script = tmp_path / "silent-router"
    script.write_text("#!" + sys.executable + "\nimport time\ntime.sleep(60)\n")
    script.chmod(0o700)
    router = WebSocketRouter(
        binary_path=script,
        startup_timeout=0.05,
        shutdown_timeout=0.05,
    )
    with pytest.raises(DriverStateError, match="Could not start"):
        await asyncio.wait_for(router.start(), 3)
    assert router.pid is None


async def test_go_router_exits_when_owning_parent_is_killed() -> None:
    binary = _router_binary(None)
    code = (
        "import asyncio\n"
        "from meshcall import WebSocketRouter\n"
        "async def main():\n"
        f"    r = WebSocketRouter(binary_path={binary!r})\n"
        "    await r.start()\n"
        "    print(r.pid, r.bound_port, flush=True)\n"
        "    await asyncio.Event().wait()\n"
        "asyncio.run(main())\n"
    )
    parent = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        code,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert parent.stdout is not None
    child_pid: int | None = None
    try:
        line = await asyncio.wait_for(parent.stdout.readline(), 5)
        child_pid, port = map(int, line.split())
        parent.kill()
        await parent.wait()
        async with asyncio.timeout(5):
            while True:
                with socket.socket() as probe:
                    if probe.connect_ex(("127.0.0.1", port)) != 0:
                        break
                await asyncio.sleep(0.01)
    finally:
        if parent.returncode is None:
            parent.kill()
            await parent.wait()
        if child_pid is not None:
            try:
                os.kill(child_pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

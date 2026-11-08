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


import asyncio
import contextlib
import functools
import inspect
import io
import os
import subprocess
import traceback
from collections.abc import Callable, Coroutine
from typing import Any

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


# A test that hangs fails after its deadline with every asyncio task's stack,
# instead of stalling the run until CI cancels the job: faulthandler shows only
# the idle event loop, not the await that never returns. A cancelled test whose
# cleanup hangs too is ended by faulthandler_exit_on_timeout (pyproject.toml).
DEFAULT_DEADLINE = float(os.environ.get("MESHCALL_TEST_DEADLINE", "60"))


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "deadline(seconds): fail the test if it runs longer than this"
    )


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item: pytest.Item):
    function = getattr(item, "obj", None)
    if not isinstance(item, pytest.Function) or not inspect.iscoroutinefunction(
        function
    ):
        return (yield)
    marker = item.get_closest_marker("deadline")
    seconds = float(marker.args[0]) if marker else DEFAULT_DEADLINE
    item.obj = _with_deadline(function, item, seconds)
    try:
        return (yield)
    finally:
        item.obj = function


def _with_deadline(
    function: Callable[..., Coroutine[Any, Any, Any]],
    item: pytest.Function,
    seconds: float,
) -> Callable[..., Coroutine[Any, Any, Any]]:
    @functools.wraps(function)
    async def run(*args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        deadline = asyncio.timeout(None)

        def expire() -> None:
            _report_tasks(item, seconds)
            deadline.reschedule(loop.time())

        handle = loop.call_later(seconds, expire)
        try:
            async with deadline:
                return await function(*args, **kwargs)
        except TimeoutError:
            if not deadline.expired():
                raise
            pytest.fail(
                f"did not finish within {seconds:g}s (asyncio task stacks on "
                "stderr; the cancelled await is in the traceback above)"
            )
        finally:
            handle.cancel()

    return run


def _awaiting_frames(awaitable: object):
    """The frames of a coroutine and of everything it is awaiting, outermost
    first (Task.print_stack shows only the outermost)."""
    while awaitable is not None:
        frame = getattr(awaitable, "cr_frame", None) or getattr(
            awaitable, "ag_frame", None
        )
        if frame is None:
            return
        yield frame, frame.f_lineno
        awaitable = getattr(awaitable, "cr_await", None) or getattr(
            awaitable, "ag_await", None
        )


def _report_tasks(item: pytest.Function, seconds: float) -> None:
    """Write every pending task's stack to the real stderr, now, so a hang
    that also blocks cleanup is still explained."""
    out = io.StringIO()
    print(
        f"\n=== {item.nodeid} still running after {seconds:g}s; asyncio tasks ===",
        file=out,
    )
    for task in asyncio.all_tasks():
        print(f"--- {task.get_name()}", file=out)
        frames = _awaiting_frames(task.get_coro())
        out.writelines(traceback.StackSummary.extract(frames).format())
    capture = item.config.pluginmanager.getplugin("capturemanager")
    disabled = getattr(capture, "global_and_fixture_disabled", contextlib.nullcontext)
    with disabled():
        sys.stderr.write(out.getvalue())
        sys.stderr.flush()

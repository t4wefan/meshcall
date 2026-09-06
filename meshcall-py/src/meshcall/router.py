from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
from pathlib import Path
from typing import Self

from meshcall.errors import DriverStateError
from meshcall.protocol import PROTOCOL_VERSION


def _router_binary(binary_path: str | Path | None) -> str:
    executable = "meshcall-router.exe" if os.name == "nt" else "meshcall-router"
    configured = binary_path or os.environ.get("MESHCALL_ROUTER_BINARY")
    if configured:
        candidate = Path(configured).expanduser().absolute()
        if not candidate.is_file() or not os.access(candidate, os.X_OK):
            raise DriverStateError(f"Router binary is not executable: {candidate}")
        return str(candidate)
    candidates = [Path(__file__).parent / "bin" / executable]
    # A demo can install a wheel into its own venv rather than an editable copy.
    # Find the enclosing source checkout from either the package or the caller.
    roots = dict.fromkeys(
        [
            *Path(__file__).resolve().parents,
            Path.cwd(),
            *Path.cwd().parents,
        ]
    )
    candidates.extend(
        root / "meshcall-router" / "bin" / executable
        for root in roots
        if (root / "meshcall-router" / "go.mod").is_file()
    )
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    installed = shutil.which(executable)
    if installed:
        return installed
    raise DriverStateError(
        "Go Router binary not found. Build meshcall-router or set "
        "MESHCALL_ROUTER_BINARY / binary_path to a prebuilt executable."
    )


class WebSocketRouter:
    """Own a Go Router process; all RPC routing and authentication run in Go."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        *,
        unix_path: str | Path | None = None,
        max_frame_size: int = 1024 * 1024,
        binary_path: str | Path | None = None,
        auth_file: str | Path | None = None,
        startup_timeout: float = 10,
        shutdown_timeout: float = 5,
    ) -> None:
        if unix_path is not None and (host is not None or port is not None):
            raise ValueError("Unix socket cannot be combined with host or port")
        if port is not None and not 0 <= port <= 65535:
            raise ValueError("Port must be between 0 and 65535")
        if not 1 <= max_frame_size <= 64 * 1024 * 1024:
            raise ValueError("max_frame_size must be between 1 and 67108864")
        if startup_timeout <= 0 or shutdown_timeout <= 0:
            raise ValueError("Process timeouts must be positive")
        self.host = host or "127.0.0.1"
        self.port = 0 if port is None else port
        self.unix_path = Path(unix_path).absolute() if unix_path is not None else None
        if self.unix_path is not None and len(str(self.unix_path).encode()) > 103:
            raise DriverStateError(
                "Unix socket path exceeds the portable 103-byte limit"
            )
        self.max_frame_size = max_frame_size
        self.binary_path = binary_path
        self.auth_file = Path(auth_file).absolute() if auth_file is not None else None
        self.startup_timeout = startup_timeout
        self.shutdown_timeout = shutdown_timeout
        self._process: asyncio.subprocess.Process | None = None
        self._bound_port: int | None = None
        self._ready = False
        self._lock = asyncio.Lock()
        self._stderr = bytearray()
        self._readers: list[asyncio.Task[None]] = []

    @property
    def is_running(self) -> bool:
        return (
            self._ready
            and self._process is not None
            and self._process.returncode is None
        )

    @property
    def pid(self) -> int | None:
        return (
            self._process.pid if self.is_running and self._process is not None else None
        )

    @property
    def bound_port(self) -> int:
        if self.unix_path is not None:
            raise DriverStateError("Unix socket listener does not have a TCP port")
        if not self.is_running or self._bound_port is None:
            raise DriverStateError("Go Router is not running")
        return self._bound_port

    async def _drain(self, stream: asyncio.StreamReader, *, diagnostic: bool) -> None:
        while data := await stream.read(4096):
            if diagnostic:
                self._stderr.extend(data)
                del self._stderr[:-8192]

    async def start(self) -> None:
        async with self._lock:
            if self.is_running:
                return
            await self._stop()
            command = [
                _router_binary(self.binary_path),
                "--shutdown-on-stdin-close",
                "--max-frame-size",
                str(self.max_frame_size),
            ]
            if self.unix_path is None:
                command.extend(["--host", self.host, "--port", str(self.port)])
            else:
                command.extend(["--unix-socket", str(self.unix_path)])
            if self.auth_file is not None:
                command.extend(["--auth-file", str(self.auth_file)])
            self._stderr.clear()
            try:
                process = await asyncio.create_subprocess_exec(
                    *command,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    limit=16384,
                )
                self._process = process
                assert process.stdout is not None and process.stderr is not None
                self._readers.append(
                    asyncio.create_task(self._drain(process.stderr, diagnostic=True))
                )
                line = await asyncio.wait_for(
                    process.stdout.readline(), timeout=self.startup_timeout
                )
                if not line:
                    raise DriverStateError("Go Router exited before readiness")
                ready = json.loads(line)
                if (
                    not isinstance(ready, dict)
                    or ready.get("kind") != "router.ready"
                    or ready.get("protocol") != PROTOCOL_VERSION
                    or ready.get("pid") != process.pid
                ):
                    raise DriverStateError("Invalid Go Router readiness message")
                if self.unix_path is None:
                    port = ready.get("port")
                    if type(port) is not int or not 1 <= port <= 65535:
                        raise DriverStateError("Invalid Go Router bound port")
                    self._bound_port = port
                elif ready.get("unix_path") != str(self.unix_path):
                    raise DriverStateError("Invalid Go Router Unix socket path")
                self._readers.append(
                    asyncio.create_task(self._drain(process.stdout, diagnostic=False))
                )
                if process.returncode is not None:
                    raise DriverStateError("Go Router exited during startup")
                self._ready = True
            except asyncio.CancelledError:
                await self._stop()
                raise
            except Exception as exc:
                await self._stop()
                detail = self._stderr.decode(errors="replace").strip()
                message = f"Could not start Go Router: {exc}"
                if detail:
                    message += f"\n{detail}"
                raise DriverStateError(message) from exc

    async def stop(self) -> None:
        async with self._lock:
            await self._stop()

    async def _stop(self) -> None:
        process, self._process = self._process, None
        self._ready = False
        self._bound_port = None
        if process is not None:
            if process.stdin is not None:
                process.stdin.close()
            if process.returncode is None:
                try:
                    await asyncio.wait_for(process.wait(), self.shutdown_timeout)
                except TimeoutError:
                    with contextlib.suppress(ProcessLookupError):
                        process.terminate()
                    try:
                        await asyncio.wait_for(process.wait(), self.shutdown_timeout)
                    except TimeoutError:
                        with contextlib.suppress(ProcessLookupError):
                            process.kill()
                        await process.wait()
            await process.wait()
        if self._readers:
            await asyncio.gather(*self._readers)
            self._readers.clear()

    async def wait_closed(self) -> int:
        if self._process is None:
            raise DriverStateError("Go Router is not running")
        return await self._process.wait()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

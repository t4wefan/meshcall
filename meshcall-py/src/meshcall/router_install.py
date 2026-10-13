"""Locate or download the Go Router binary for this platform.

Resolution order used by :class:`meshcall.router.WebSocketRouter`:

1. an explicit ``binary_path`` or ``MESHCALL_ROUTER_BINARY``;
2. a binary installed next to this package (``meshcall/bin``);
3. the per-user cache populated by ``meshcall router install``;
4. ``meshcall-router/bin`` of the source checkout that contains this package
   (development only);
5. ``meshcall-router`` on ``PATH``.

Downloads come from the GitHub release matching this package's version and
are verified against the release's ``checksums.txt`` before being installed.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import os
import platform
import shutil
import stat
import tarfile
import tempfile
import urllib.request
from pathlib import Path

from meshcall.errors import DriverStateError

RELEASE_BASE_URL = "https://github.com/t4wefan/meshcall/releases/download"
EXECUTABLE = "meshcall-router.exe" if os.name == "nt" else "meshcall-router"


def router_version() -> str:
    """The Router release that matches this package."""
    override = os.environ.get("MESHCALL_ROUTER_VERSION")
    if override:
        return override.lstrip("v")
    try:
        return importlib.metadata.version("meshcall")
    except importlib.metadata.PackageNotFoundError:  # source checkout without install
        return "0.0.0"


def cache_dir(version: str | None = None) -> Path:
    base = os.environ.get("XDG_CACHE_HOME")
    root = Path(base) if base else Path.home() / ".cache"
    return root / "meshcall" / "router" / (version or router_version())


def platform_asset(version: str) -> str:
    system = platform.system().lower()
    if system not in {"linux", "darwin", "windows"}:
        raise DriverStateError(f"No Router build for platform {system}")
    machine = platform.machine().lower()
    arch = {
        "x86_64": "amd64",
        "amd64": "amd64",
        "aarch64": "arm64",
        "arm64": "arm64",
    }.get(machine)
    if arch is None:
        raise DriverStateError(f"No Router build for architecture {machine}")
    return f"meshcall-router_{version}_{system}_{arch}.tar.gz"


def find_router_binary(binary_path: str | Path | None = None) -> str:
    configured = binary_path or os.environ.get("MESHCALL_ROUTER_BINARY")
    if configured:
        candidate = Path(configured).expanduser().absolute()
        if not candidate.is_file() or not os.access(candidate, os.X_OK):
            raise DriverStateError(f"Router binary is not executable: {candidate}")
        return str(candidate)
    package = Path(__file__).resolve().parent
    candidates = [package / "bin" / EXECUTABLE, cache_dir() / EXECUTABLE]
    # Development: the source checkout that contains this very file. The
    # working directory is deliberately not searched.
    candidates.extend(
        root / "meshcall-router" / "bin" / EXECUTABLE
        for root in package.parents
        if (root / "meshcall-router" / "go.mod").is_file()
    )
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    installed = shutil.which(EXECUTABLE)
    if installed:
        return installed
    raise DriverStateError(
        "Go Router binary not found. Run `meshcall router install`, build "
        "meshcall-router from source, or set MESHCALL_ROUTER_BINARY / binary_path."
    )


def install_router(
    version: str | None = None,
    *,
    destination: str | Path | None = None,
    force: bool = False,
) -> Path:
    """Download and verify the Router for this platform; return its path."""
    version = (version or router_version()).lstrip("v")
    target_dir = Path(destination) if destination is not None else cache_dir(version)
    target = target_dir / EXECUTABLE
    if target.is_file() and not force:
        return target
    asset = platform_asset(version)
    base = f"{RELEASE_BASE_URL}/v{version}"
    checksums = _fetch(f"{base}/checksums.txt").decode()
    expected = next(
        (
            line.split()[0]
            for line in checksums.splitlines()
            if line.strip().endswith(asset)
        ),
        None,
    )
    if expected is None:
        raise DriverStateError(f"Release v{version} has no checksum for {asset}")
    archive = _fetch(f"{base}/{asset}")
    actual = hashlib.sha256(archive).hexdigest()
    if actual != expected:
        raise DriverStateError(
            f"Checksum mismatch for {asset}: expected {expected}, got {actual}"
        )
    target_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=target_dir) as scratch:
        archive_path = Path(scratch) / asset
        archive_path.write_bytes(archive)
        with tarfile.open(archive_path, "r:gz") as tar:
            member = next(
                (
                    entry
                    for entry in tar.getmembers()
                    if Path(entry.name).name == EXECUTABLE and entry.isfile()
                ),
                None,
            )
            if member is None:
                raise DriverStateError(f"{asset} does not contain {EXECUTABLE}")
            extracted = tar.extractfile(member)
            if extracted is None:
                raise DriverStateError(f"Could not read {EXECUTABLE} from {asset}")
            staged = Path(scratch) / EXECUTABLE
            staged.write_bytes(extracted.read())
        staged.chmod(staged.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        os.replace(staged, target)
    return target


def _fetch(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "meshcall-installer"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read()
    except OSError as exc:
        raise DriverStateError(f"Could not download {url}: {exc}") from exc

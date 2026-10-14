"""Bundle the Go binaries into platform wheels.

Release builds set MESHCALL_BUNDLE_DIR to a directory holding `meshcall-router`
and `meshcall` (`.exe` on Windows) for one platform and MESHCALL_WHEEL_TAG to
that platform's wheel tag (e.g. `manylinux_2_17_x86_64.manylinux2014_x86_64`,
`macosx_11_0_arm64`, `win_amd64`). The binaries land in `meshcall/bin/`, which
is the first place `meshcall.router_install` looks. Without the variables the
build is the ordinary pure wheel, and the runtime falls back to downloading
the release on first use.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class BundleBinaries(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        if self.target_name != "wheel":
            return
        bundle = os.environ.get("MESHCALL_BUNDLE_DIR")
        tag = os.environ.get("MESHCALL_WHEEL_TAG")
        if not bundle and not tag:
            return
        if not (bundle and tag):
            raise RuntimeError("set both MESHCALL_BUNDLE_DIR and MESHCALL_WHEEL_TAG")
        source = Path(bundle)
        found = 0
        for name in ("meshcall-router", "meshcall", "meshcall-router.exe", "meshcall.exe"):
            binary = source / name
            if binary.is_file():
                build_data["force_include"][str(binary)] = f"meshcall/bin/{name}"
                found += 1
        if found < 2:
            raise RuntimeError(f"{source} must contain meshcall-router and meshcall")
        build_data["pure_python"] = False
        build_data["infer_tag"] = False
        build_data["tag"] = f"py3-none-{tag}"

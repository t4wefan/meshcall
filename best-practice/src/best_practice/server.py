"""Run the best-practice Python service through the Router.

``Router()`` resolves ``MESHCALL_ROUTER_URL`` (default ``ws://127.0.0.1:8765``);
``mesh.run(...)`` registers the mounted services with the Router and drains on
SIGINT/SIGTERM.
"""

from __future__ import annotations

import os

from meshcall import Mesh, Router

from .service import LlmService


def main() -> None:
    mesh = Mesh(Router()).mount(LlmService())
    mesh.run(
        access_log=_env_bool("MESHCALL_ACCESS_LOG", default=True),
        log_level=os.environ.get("MESHCALL_LOG_LEVEL", "INFO"),
        colorize=_env_bool("MESHCALL_LOG_COLOR", default=False),
    )


def _env_bool(name: str, *, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}

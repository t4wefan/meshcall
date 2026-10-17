"""Run the best-practice Python service through a Router.

When ``MESHCALL_ROUTER_URL`` is set, ``Router()`` connects to that external
Router; otherwise ``LocalRouter(port=8765)`` starts a local ``meshcall-router``
subprocess on ``127.0.0.1:8765``.
"""

from __future__ import annotations

import os

from meshcall import LocalRouter, Mesh, Router

from .service import LlmService


def main() -> None:
    router = (
        Router()
        if os.environ.get("MESHCALL_ROUTER_URL")
        else LocalRouter(port=8765)
    )
    mesh = Mesh(router).mount(LlmService())
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

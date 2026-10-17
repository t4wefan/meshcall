"""Run the best-practice Python service through the Router.

``meshcall.run`` resolves the Router (``MESHCALL_ROUTER_URL``, default
``ws://127.0.0.1:8765``), starts a bundled Router when the address is local
and nothing listens there, drains on SIGINT/SIGTERM, and stops the Router it
started.
"""

from __future__ import annotations

import os

from meshcall import run

from .service import LlmService


def main() -> None:
    run(
        LlmService(),
        access_log=_env_bool("MESHCALL_ACCESS_LOG", default=True),
        log_level=os.environ.get("MESHCALL_LOG_LEVEL", "INFO"),
        colorize=_env_bool("MESHCALL_LOG_COLOR", default=False),
    )


def _env_bool(name: str, *, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}

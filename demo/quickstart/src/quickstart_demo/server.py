"""Serve the quickstart service through a Router until interrupted."""

from __future__ import annotations

import os

from meshcall import LocalRouter, Mesh, Router
from quickstart_demo.service import CounterService


def main() -> None:
    router = (
        Router()
        if os.environ.get("MESHCALL_ROUTER_URL")
        else LocalRouter(port=8765)
    )
    Mesh(router).mount(CounterService()).run()


if __name__ == "__main__":
    main()

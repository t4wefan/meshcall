"""Serve the quickstart service through a Router until interrupted."""

from __future__ import annotations

from meshcall import Mesh, Router
from quickstart_demo.service import CounterService


def main() -> None:
    # Router() resolves MESHCALL_ROUTER_URL (default ws://127.0.0.1:8765);
    # mesh.run() serves all mounted services and drains on SIGINT/SIGTERM.
    mesh = Mesh(Router()).mount(CounterService())
    mesh.run()


if __name__ == "__main__":
    main()

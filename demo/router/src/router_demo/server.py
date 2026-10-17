"""Register the service with an already running Router."""

from __future__ import annotations

from meshcall import Mesh, Router
from router_demo.service import CounterService


def main() -> None:
    mesh = Mesh(Router("ws://127.0.0.1:8765")).mount(
        CounterService(), instance_id="counter-1"
    )
    mesh.run()


if __name__ == "__main__":
    main()

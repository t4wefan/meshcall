"""Register the service with an already running Router."""

from __future__ import annotations

from meshcall import Router, run
from router_demo.service import CounterService


def main() -> None:
    run(
        CounterService(),
        router=Router("ws://127.0.0.1:8765"),
        instance_id="counter-1",
    )


if __name__ == "__main__":
    main()

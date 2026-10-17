"""Serve the quickstart service through a Router until interrupted."""

from __future__ import annotations

from meshcall import run
from quickstart_demo.service import CounterService


def main() -> None:
    # run() resolves MESHCALL_ROUTER_URL (default ws://127.0.0.1:8765), starts a
    # bundled Router when the address is local and nothing listens, and drains
    # on SIGINT/SIGTERM.
    run(CounterService())


if __name__ == "__main__":
    main()

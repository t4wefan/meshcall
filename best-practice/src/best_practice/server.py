"""Register the best-practice Python service with an authenticated Go Router."""

from __future__ import annotations

import asyncio
import os
import signal
from pathlib import Path

from meshcall import RouterCredentials, RpcServer
from meshcall.drivers import WebSocketRouterServerDriver

from .config import read_credentials, router_url
from .service import LlmService


async def serve_forever(
    uri: str,
    credentials: RouterCredentials,
    *,
    instance_id: str = "llm-python-1",
    access_log: bool = True,
    log_level: str = "INFO",
    colorize: bool = False,
) -> None:
    driver = WebSocketRouterServerDriver(
        uri,
        instance_id=instance_id,
        auth=credentials,
    )
    server = RpcServer(
        services=[LlmService()],
        driver=driver,
        access_log=access_log,
        log_level=log_level,
        colorize=colorize,
    )
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals: list[signal.Signals] = []
    for name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(name, stopped.set)
            signals.append(name)
        except NotImplementedError:  # Windows console uses KeyboardInterrupt.
            pass
    try:
        await server.start()
        print(f"MeshCall LLM service registered as {instance_id}", flush=True)
        await stopped.wait()
    finally:
        await server.stop()
        for name in signals:
            loop.remove_signal_handler(name)


def main() -> None:
    access_log = _env_bool("MESHCALL_ACCESS_LOG", default=True)
    colorize = _env_bool("MESHCALL_LOG_COLOR", default=False)
    log_level = os.environ.get("MESHCALL_LOG_LEVEL", "INFO")
    try:
        asyncio.run(
            serve_forever(
                router_url(),
                read_credentials(
                    Path(
                        os.environ.get(
                            "MESHCALL_WORKER_CREDENTIALS",
                            ".local/worker.json",
                        )
                    )
                ),
                instance_id=os.environ.get("MESHCALL_INSTANCE_ID", "llm-python-1"),
                access_log=access_log,
                log_level=log_level,
                colorize=colorize,
            )
        )
    except KeyboardInterrupt:
        pass
    except (OSError, ValueError) as exc:
        raise SystemExit(f"meshcall service: {exc}") from None


def _env_bool(name: str, *, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


if __name__ == "__main__":
    main()

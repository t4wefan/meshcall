"""Issue or revoke a service-scoped token using the separate issuer account."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from meshcall import RouterAuthClient, RouterScope
from meshcall.drivers import WebSocketClientDriver
from meshcall.errors import MeshCallError

from .config import (
    CALL_METHODS,
    SERVICE_NAME,
    read_credentials,
    router_url,
    write_private_json,
)


async def run(args: argparse.Namespace) -> None:
    credentials = read_credentials(args.credentials)
    if credentials.token is not None:
        raise ValueError("Token management requires an issuer account")
    async with RouterAuthClient(
        WebSocketClientDriver(router_url(), auth=credentials)
    ) as auth:
        if args.command == "revoke":
            revoked = await auth.revoke_token(args.token_id)
            print(f"revoked={str(revoked).lower()}")
            return
        if args.output.exists() or args.output.is_symlink():
            raise ValueError("Token output already exists; choose a new file")
        issued = await auth.issue_token(
            ttl_seconds=args.ttl_seconds,
            scopes=[
                RouterScope(
                    service=SERVICE_NAME,
                    methods=list(dict.fromkeys(args.method or CALL_METHODS)),
                )
            ],
        )
        try:
            write_private_json(args.output, {"token": issued.token})
        except BaseException:
            await auth.revoke_token(issued.token_id)
            raise
        print(f"Token saved to {args.output}")
        print(f"token_id={issued.token_id}")
        print(f"expires_at_unix_ms={issued.expires_at_unix_ms}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials", type=Path, default=Path(".local/issuer.json"))
    commands = parser.add_subparsers(dest="command", required=True)
    issue = commands.add_parser("issue")
    issue.add_argument("--output", type=Path, default=Path(".local/token.json"))
    issue.add_argument("--ttl-seconds", type=int, default=900)
    issue.add_argument("--method", action="append", choices=CALL_METHODS)
    commands.add_parser("revoke").add_argument("token_id")
    args = parser.parse_args()
    if args.command == "issue" and not 1 <= args.ttl_seconds <= 3600:
        parser.error("--ttl-seconds must be between 1 and 3600")
    try:
        asyncio.run(run(args))
    except (OSError, ValueError, MeshCallError) as exc:
        parser.exit(1, f"router-token: {exc}\n")


if __name__ == "__main__":
    main()

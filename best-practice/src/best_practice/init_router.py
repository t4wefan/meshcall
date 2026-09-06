"""Explicitly prepare separate Router, worker, client, and issuer credentials."""

from __future__ import annotations

import argparse
import os
import secrets
import subprocess
from pathlib import Path

from meshcall.router_auth import AUTH_SERVICE

from .config import CALL_METHODS, SERVICE_NAME, write_private_json


def initialize(directory: Path, binary: Path) -> None:
    if directory.exists() or directory.is_symlink():
        raise ValueError("Credential directory already exists; choose a new directory")
    binary = binary.expanduser().resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("Router binary must be an explicit executable file")
    rules = [f"{SERVICE_NAME}/{method}" for method in CALL_METHODS]
    permissions = {
        "worker": {"roles": ["server"], "register": [SERVICE_NAME]},
        "client": {"roles": ["client"], "call": rules},
        "issuer": {
            "roles": ["client"],
            "call": [
                *rules,
                f"{AUTH_SERVICE}/issue_token",
                f"{AUTH_SERVICE}/revoke_token",
            ],
        },
    }
    users: list[dict[str, object]] = []
    credentials: dict[str, dict[str, str]] = {}
    for name, grants in permissions.items():
        password = secrets.token_urlsafe(32)
        try:
            result = subprocess.run(
                [str(binary), "hash-password"],
                input=password,
                text=True,
                capture_output=True,
                timeout=10,
                check=True,
            )
        except (OSError, subprocess.SubprocessError):
            raise ValueError("Go Router hash-password failed") from None
        password_hash = result.stdout.strip()
        if not password_hash.startswith("pbkdf2-sha256$600000$"):
            raise ValueError("Unexpected Go Router password hash format")
        users.append({"username": name, "password_hash": password_hash, **grants})
        credentials[name] = {"username": name, "password": password}
    # Generate everything before creating the private directory. Never rotate
    # or overwrite an existing setup implicitly.
    directory.mkdir(mode=0o700)
    write_private_json(directory / "router-auth.json", {"users": users})
    for name, account in credentials.items():
        write_private_json(directory / f"{name}.json", account)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path(".local"))
    parser.add_argument("--binary", type=Path, required=True)
    args = parser.parse_args()
    try:
        initialize(args.directory, args.binary)
    except (OSError, ValueError) as exc:
        parser.exit(1, f"init-router: {exc}\n")
    print(f"Router and account files created in {args.directory}. No listener started.")


if __name__ == "__main__":
    main()

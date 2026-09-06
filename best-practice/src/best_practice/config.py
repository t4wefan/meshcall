"""Connection settings and private credential files for the example."""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import urlsplit

from meshcall import RouterCredentials
from pydantic import BaseModel, ConfigDict, Field

SERVICE_NAME = "best_practice.v1.LlmService"
CALL_METHODS = (
    "new_session",
    "list_sessions",
    "count_tokens",
    "assemble_prompt",
    "stream_chat",
)
DEFAULT_ROUTER_URL = "ws://127.0.0.1:8765"


class _Credentials(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    username: str | None = None
    password: str | None = Field(default=None, repr=False)
    token: str | None = Field(default=None, repr=False)


def read_credentials(path: Path) -> RouterCredentials:
    try:
        data = _Credentials.model_validate_json(path.read_text())
        if data.model_fields_set not in ({"username", "password"}, {"token"}):
            raise ValueError("Set either an account or a token")
        return RouterCredentials(
            username=data.username,
            password=data.password,
            token=data.token,
        )
    except (OSError, ValueError):
        # Validation exceptions can include the input, including a secret.
        raise ValueError(
            f"Cannot load credentials from {path}; use init-router or a valid "
            "username/password or token JSON file"
        ) from None


def router_url() -> str:
    url = os.environ.get("MESHCALL_ROUTER_URL", DEFAULT_ROUTER_URL)
    try:
        parsed = urlsplit(url)
        valid = (
            parsed.scheme in {"ws", "wss"}
            and parsed.hostname is not None
            and parsed.username is None
            and parsed.password is None
            and not parsed.query
            and not parsed.fragment
            and (parsed.port is None or 1 <= parsed.port <= 65535)
        )
        if not valid:
            raise ValueError
        if parsed.scheme == "ws" and parsed.hostname not in {
            "127.0.0.1",
            "::1",
            "localhost",
        }:
            raise ValueError
    except ValueError:
        raise ValueError(
            "MESHCALL_ROUTER_URL must be wss:// for network hosts or ws:// for "
            "loopback, without URL credentials, query, or fragment"
        ) from None
    return url


def write_private_json(path: Path, value: object) -> None:
    """Create a private file without overwriting a credential or following a link."""
    data = json.dumps(value, indent=2) + "\n"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        output.write(data)

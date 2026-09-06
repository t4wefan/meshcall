from __future__ import annotations

import hashlib
import json
from functools import cache
from pathlib import Path

AUTH_SERVICE = "meshcall.router.v1.AuthService"
PASSWORD = "test-only-password"


@cache
def password_hash() -> str:
    salt = b"meshcall-test-01"
    key = hashlib.pbkdf2_hmac("sha256", PASSWORD.encode(), salt, 600000)
    return f"pbkdf2-sha256$600000${salt.hex()}${key.hex()}"


def write_auth_file(path: Path, service: str) -> Path:
    path.write_text(
        json.dumps(
            {
                "users": [
                    {
                        "username": "issuer",
                        "password_hash": password_hash(),
                        "roles": ["client", "server"],
                        "register": [service],
                        "call": [f"{service}/*", f"{AUTH_SERVICE}/*"],
                    },
                    {
                        "username": "reader",
                        "password_hash": password_hash(),
                        "roles": ["client"],
                        "call": [f"{service}/unary"],
                    },
                ],
            }
        )
    )
    return path

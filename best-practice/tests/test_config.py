from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path

import pytest

from best_practice.config import read_credentials, router_url, write_private_json
from best_practice.init_router import initialize


def test_initialization_separates_accounts_and_preserves_existing_credentials(
    tmp_path: Path,
) -> None:
    configured = os.environ.get("MESHCALL_ROUTER_BINARY")
    if not configured:
        pytest.skip("set MESHCALL_ROUTER_BINARY for the Go hash-password check")
    directory = tmp_path / "accounts"
    initialize(directory, Path(configured))
    auth_path = directory / "router-auth.json"
    original = auth_path.read_bytes()
    users = {user["username"]: user for user in json.loads(original)["users"]}
    assert users["worker"]["roles"] == ["server"]
    assert "call" not in users["worker"]
    assert users["client"]["roles"] == ["client"]
    assert "register" not in users["client"]
    assert all("AuthService" not in rule for rule in users["client"]["call"])
    assert "register" not in users["issuer"]
    passwords = set()
    for name, user in users.items():
        account = read_credentials(directory / f"{name}.json")
        assert account.username == name and account.password is not None
        passwords.add(account.password)
        _, iterations, salt, key = user["password_hash"].split("$")
        assert (
            hashlib.pbkdf2_hmac(
                "sha256",
                account.password.encode(),
                bytes.fromhex(salt),
                int(iterations),
            ).hex()
            == key
        )
        assert account.password not in original.decode()
    assert len(passwords) == 3
    if os.name != "nt":
        assert stat.S_IMODE(directory.stat().st_mode) == 0o700
        assert all(stat.S_IMODE(p.stat().st_mode) == 0o600 for p in directory.iterdir())
    with pytest.raises(ValueError, match="already exists"):
        initialize(directory, Path(configured))
    assert auth_path.read_bytes() == original


@pytest.mark.parametrize(
    "value",
    [
        '{"password":"do-not-disclose"}',
        '{"token":"do-not-disclose","username":"client","password":"x"}',
        '{"token":"do-not-disclose","unknown":true}',
        "not-json-do-not-disclose",
    ],
)
def test_malformed_credentials_fail_without_exposing_contents(
    tmp_path: Path,
    value: str,
) -> None:
    path = tmp_path / "credentials.json"
    path.write_text(value)
    with pytest.raises(ValueError) as error:
        read_credentials(path)
    assert "do-not-disclose" not in str(error.value)


def test_credentials_are_never_overwritten_or_written_through_symlinks(
    tmp_path: Path,
) -> None:
    path = tmp_path / "credentials.json"
    write_private_json(path, {"token": "first"})
    with pytest.raises(FileExistsError):
        write_private_json(path, {"token": "replacement"})
    assert read_credentials(path).token == "first"
    if os.name != "nt":
        link = tmp_path / "link.json"
        link.symlink_to(path)
        with pytest.raises(FileExistsError):
            write_private_json(link, {"token": "replacement"})
        assert read_credentials(path).token == "first"


@pytest.mark.parametrize(
    "url",
    [
        "ws://router.example:8765",
        "ws://0.0.0.0:8765",
        "wss://client:do-not-disclose@router.example",
        "wss://router.example/?token=do-not-disclose",
        "wss://router.example/#do-not-disclose",
        "https://router.example",
        "ws://127.0.0.1:0",
        "ws://[broken",
    ],
)
def test_router_url_rejects_cleartext_network_and_inline_secrets(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
) -> None:
    monkeypatch.setenv("MESHCALL_ROUTER_URL", url)
    with pytest.raises(ValueError) as error:
        router_url()
    assert "do-not-disclose" not in str(error.value)


@pytest.mark.parametrize(
    "url",
    [
        "ws://127.0.0.1:8765",
        "ws://[::1]:8765",
        "wss://router.example/rpc",
    ],
)
def test_router_url_accepts_loopback_or_tls(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
) -> None:
    monkeypatch.setenv("MESHCALL_ROUTER_URL", url)
    assert router_url() == url

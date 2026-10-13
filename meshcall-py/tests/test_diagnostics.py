"""Advisory warnings: emitted at import, silenced explicitly, fatal in strict mode."""

from __future__ import annotations

import warnings

import pytest
from pydantic import BaseModel

from meshcall import ContractError, MeshCallWarning, lint_contract, method, service
from meshcall.contract import get_service_contract, get_service_warnings


class Result(BaseModel):
    ok: bool = True


def _codes(record: pytest.WarningsRecorder) -> list[tuple[str, str]]:
    return [
        (w.message.diagnostic.code, w.message.diagnostic.subject)
        for w in record
        if isinstance(w.message, MeshCallWarning)
    ]


def test_missing_descriptions_and_annotations_warn_with_hints() -> None:
    with pytest.warns(MeshCallWarning) as record:

        @service("shop.v1")
        class Shop:
            @method()
            async def get_item(self, item_id: str) -> Result:
                return Result()

            @method()
            async def delete_item(self, item_id: str) -> Result:
                """Remove an item for good."""
                return Result()

    codes = _codes(record)
    assert ("W001", "shop.v1") in codes
    assert ("W001", "shop.v1.get_item") in codes
    assert ("W002", "shop.v1.delete_item") in codes
    assert ("W001", "shop.v1.delete_item") not in codes
    first = next(w.message for w in record if isinstance(w.message, MeshCallWarning))
    assert first.diagnostic.hint and first.diagnostic.silence
    assert "test_diagnostics.py" in (first.diagnostic.location or "")
    assert get_service_warnings(Shop)  # kept for the server start summary


def test_explicit_empty_description_and_annotations_silence() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", MeshCallWarning)

        @service("quiet.v1", description="")
        class Quiet:
            @method(description="", destructive=True)
            async def delete_item(self, item_id: str) -> Result:
                return Result()

            @method(read_only=True)
            async def get_item(self, item_id: str) -> Result:
                """Fetch an item."""
                return Result()

    assert lint_contract(get_service_contract(Quiet)) == []


def test_contradictory_annotations_and_empty_deprecation_warn() -> None:
    with pytest.warns(MeshCallWarning) as record:

        @service("odd.v1", description="Odd cases.")
        class Odd:
            @method(read_only=True, description="Actually mutates.")
            async def delete_item(self, item_id: str) -> Result:
                return Result()

            @method(destructive=True, description="Just reads.")
            async def get_item(self, item_id: str) -> Result:
                return Result()

            @method(idempotent=True, description="Old.", deprecated="")
            async def old(self) -> Result:
                return Result()

    codes = [code for code, _ in _codes(record)]
    assert codes.count("W003") == 2
    assert "W005" in codes


def test_strict_mode_and_ignore_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MESHCALL_STRICT", "1")
    with pytest.raises(ContractError, match="W001"):

        @service("strict.v1")
        class Strict:
            @method()
            async def ping(self) -> Result:
                return Result()

    monkeypatch.setenv("MESHCALL_IGNORE_WARNINGS", "W001")
    with warnings.catch_warnings():
        warnings.simplefilter("error", MeshCallWarning)

        @service("ignored.v1")
        class Ignored:
            @method()
            async def ping(self) -> Result:
                return Result()

    assert get_service_warnings(Ignored) == ()

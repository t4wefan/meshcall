"""Noise NK and sealed frames, checked against the cacophony test vector."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric import x25519

from meshcall.e2e import (
    PROLOGUE,
    WINDOW,
    NKInitiator,
    SealedCall,
    _nonce,
    b64decode,
    e2e_public_key,
    generate_e2e_key,
    nk_respond,
)
from meshcall.protocol import (
    CallErrorFrame,
    CallOpenFrame,
    CallResultFrame,
    ErrorPayload,
    StreamItemFrame,
)

VECTOR = (
    Path(__file__).resolve().parents[2]
    / "meshcall-router/conformance/testdata/noise_nk_25519_aesgcm_sha256.json"
)


def test_noise_nk_matches_the_test_vector() -> None:
    v = json.loads(VECTOR.read_text())
    key = lambda h: x25519.X25519PrivateKey.from_private_bytes(bytes.fromhex(h))
    rs = x25519.X25519PublicKey.from_public_bytes(
        bytes.fromhex(v["init_remote_static"])
    )
    prologue = bytes.fromhex(v["init_prologue"])
    messages = v["messages"]
    initiator = NKInitiator(rs, prologue=prologue, ephemeral=key(v["init_ephemeral"]))
    msg1 = initiator.write_message_1(bytes.fromhex(messages[0]["payload"]))
    assert msg1.hex() == messages[0]["ciphertext"]
    msg2, got1, responder = nk_respond(
        key(v["resp_static"]),
        msg1,
        prologue=prologue,
        ephemeral=key(v["resp_ephemeral"]),
        payload=bytes.fromhex(messages[1]["payload"]),
    )
    assert msg2.hex() == messages[1]["ciphertext"]
    assert got1.hex() == messages[0]["payload"]
    got2, client = initiator.read_message_2(msg2)
    assert got2.hex() == messages[1]["payload"]
    assert client.hash.hex() == v["handshake_hash"] and client.id == responder.id
    counters = [0, 0]
    for i, message in enumerate(messages[2:]):
        sender = (client, responder)[i % 2]
        sealed = sender._send.encrypt(
            _nonce(counters[i % 2]), bytes.fromhex(message["payload"]), None
        )
        counters[i % 2] += 1
        assert sealed.hex() == message["ciphertext"]


def pair() -> tuple[SealedCall, SealedCall]:
    keys = generate_e2e_key()
    assert e2e_public_key(keys.private_key) == keys.public_key
    static = x25519.X25519PrivateKey.from_private_bytes(b64decode(keys.private_key))
    initiator = NKInitiator(static.public_key(), prologue=PROLOGUE)
    reply, _, instance = nk_respond(static, initiator.write_message_1())
    _, client = initiator.read_message_2(reply)
    return SealedCall(client, "s.v1.S", "m"), SealedCall(instance, "s.v1.S", "m")


def test_sealed_frames_bind_their_call_and_refuse_replays() -> None:
    out, into = pair()
    opened = out.seal(
        CallOpenFrame(
            call_id="c1",
            service="s.v1.S",
            method="m",
            payload={"secret": 1},
            metadata={"trace": "t"},
        )
    )
    assert isinstance(opened, CallOpenFrame)
    assert opened.payload is None and opened.metadata is None and opened.e2e_session
    got = into.open(opened)
    assert isinstance(got, CallOpenFrame)
    assert got.payload == {"secret": 1} and got.metadata == {"trace": "t"}
    with pytest.raises(ValueError, match="replayed"):
        into.open(opened)
    moved = out.seal(
        CallOpenFrame(call_id="c2", service="s.v1.S", method="m", payload=1)
    )
    with pytest.raises(ValueError, match="does not open"):
        into.open(moved.model_copy(update={"call_id": "c3"}))
    with pytest.raises(ValueError, match="in clear"):
        into.open(CallResultFrame(call_id="c1", payload=1))
    first = out.seal(
        StreamItemFrame(call_id="c1", direction="client", sequence=0, payload=1)
    )
    second = out.seal(
        StreamItemFrame(call_id="c1", direction="client", sequence=1, payload=2)
    )
    assert into.open(second).payload == 2  # type: ignore[union-attr]
    assert into.open(first).payload == 1  # type: ignore[union-attr]
    old = out.seal(
        StreamItemFrame(call_id="c1", direction="client", sequence=2, payload=0)
    )
    for i in range(WINDOW + 1):
        into.open(
            out.seal(
                StreamItemFrame(
                    call_id="c1", direction="client", sequence=3 + i, payload=i
                )
            )
        )
    with pytest.raises(ValueError, match="too old"):
        into.open(old)
    error = into.seal(
        CallErrorFrame(
            call_id="c1",
            error=ErrorPayload(
                code="invalid_argument",
                message="bad secret 42",
                details={"field": "secret"},
            ),
        )
    )
    assert isinstance(error, CallErrorFrame)
    assert error.error.message == "sealed" and error.error.details is None
    restored = out.open(error)
    assert isinstance(restored, CallErrorFrame)
    assert restored.error.message == "bad secret 42" and restored.error.details == {
        "field": "secret"
    }

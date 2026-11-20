"""Noise NK, IK and IKpsk2 and sealed frames, checked against the cacophony test vectors."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.asymmetric import x25519

from meshcall.e2e import (
    IKPSK2,
    NK,
    PROLOGUE,
    WINDOW,
    Handshake,
    SealedCall,
    _nonce,
    b64decode,
    e2e_public_key,
    generate_e2e_key,
)
from meshcall.protocol import (
    CallErrorFrame,
    CallOpenFrame,
    CallResultFrame,
    ErrorPayload,
    StreamItemFrame,
)

VECTORS = Path(__file__).resolve().parents[2] / "meshcall-router/conformance/testdata"


@pytest.mark.parametrize("name", ["nk", "ik", "ikpsk2"])
def test_noise_matches_the_test_vectors(name: str) -> None:
    v = json.loads((VECTORS / f"noise_{name}_25519_aesgcm_sha256.json").read_text())
    key = lambda h: x25519.X25519PrivateKey.from_private_bytes(bytes.fromhex(h))
    psk = bytes.fromhex(v["init_psks"][0]) if "init_psks" in v else None
    initiator = Handshake(
        v["protocol_name"],
        initiator=True,
        prologue=bytes.fromhex(v["init_prologue"]),
        s=key(v["init_static"]) if "init_static" in v else None,
        rs=x25519.X25519PublicKey.from_public_bytes(
            bytes.fromhex(v["init_remote_static"])
        ),
        psk=psk,
        ephemeral=key(v["init_ephemeral"]),
    )
    responder = Handshake(
        v["protocol_name"],
        initiator=False,
        prologue=bytes.fromhex(v["resp_prologue"]),
        s=key(v["resp_static"]),
        psk=psk,
        ephemeral=key(v["resp_ephemeral"]),
    )
    messages = v["messages"]
    msg1 = initiator.write_message(bytes.fromhex(messages[0]["payload"]))
    assert msg1.hex() == messages[0]["ciphertext"]
    assert responder.read_message(msg1).hex() == messages[0]["payload"]
    if "init_static" in v:
        assert responder.remote_static.public_bytes_raw() == _pub(v["init_static"])
    msg2 = responder.write_message(bytes.fromhex(messages[1]["payload"]))
    assert msg2.hex() == messages[1]["ciphertext"]
    assert initiator.read_message(msg2).hex() == messages[1]["payload"]
    client, instance = initiator.split(), responder.split()
    assert client.hash.hex() == v["handshake_hash"] and client.id == instance.id
    counters = [0, 0]
    for i, message in enumerate(messages[2:]):
        sender = (client, instance)[i % 2]
        sealed = sender._send.encrypt(
            _nonce(counters[i % 2]), bytes.fromhex(message["payload"]), None
        )
        counters[i % 2] += 1
        assert sealed.hex() == message["ciphertext"]


def _pub(private_hex: str) -> bytes:
    private = x25519.X25519PrivateKey.from_private_bytes(bytes.fromhex(private_hex))
    return private.public_key().public_bytes_raw()


def test_ikpsk2_with_another_psk_does_not_complete() -> None:
    instance_key = x25519.X25519PrivateKey.generate()
    initiator = Handshake(
        IKPSK2,
        initiator=True,
        prologue=PROLOGUE,
        s=x25519.X25519PrivateKey.generate(),
        rs=instance_key.public_key(),
        psk=b"\x01" * 32,
    )
    responder = Handshake(
        IKPSK2, initiator=False, prologue=PROLOGUE, s=instance_key, psk=b"\x02" * 32
    )
    # Message 1 opens without the PSK: it proves nothing about it ...
    responder.read_message(initiator.write_message())
    # ... message 2 does not open for an initiator holding another one.
    with pytest.raises(InvalidTag):
        initiator.read_message(responder.write_message())


def pair() -> tuple[SealedCall, SealedCall]:
    keys = generate_e2e_key()
    assert e2e_public_key(keys.private_key) == keys.public_key
    static = x25519.X25519PrivateKey.from_private_bytes(b64decode(keys.private_key))
    initiator = Handshake(NK, initiator=True, prologue=PROLOGUE, rs=static.public_key())
    responder = Handshake(NK, initiator=False, prologue=PROLOGUE, s=static)
    responder.read_message(initiator.write_message())
    initiator.read_message(responder.write_message())
    client, instance = initiator.split(), responder.split()
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

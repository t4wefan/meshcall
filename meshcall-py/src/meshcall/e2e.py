"""End-to-end sealed tunnels (docs/protocol.md → End-to-end encryption).

A ``Noise_NK_25519_AESGCM_SHA256`` handshake per client connection, then the
payloads, metadata and error messages of every call travel in a ``sealed``
field while the Router routes on the clear header. Needs the ``cryptography``
package (``pip install 'meshcall[e2e]'``).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from meshcall.errors import UnauthenticatedError
from meshcall.protocol import (
    CallErrorFrame,
    CallOpenFrame,
    CallResultFrame,
    ErrorPayload,
    Frame,
    StreamItemFrame,
)

NOISE_PROTOCOL = b"Noise_NK_25519_AESGCM_SHA256"
PROLOGUE = b"meshcall-e2e/1"
WINDOW = 1024
MAX_SESSIONS = 16
_RESERVED = "meshcall-"
#: The Router answers these services itself: calls to them are never sealed.
ROUTER_SERVICE_PREFIX = "meshcall.router."
E2E_CREDENTIAL_PREFIX = "mtun_e2e."


def _crypto() -> Any:
    try:
        from cryptography.hazmat.primitives.asymmetric import x25519
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError as exc:  # pragma: no cover - depends on the install
        raise ImportError(
            "End-to-end tunnels need the cryptography package: "
            "pip install 'meshcall[e2e]'"
        ) from exc
    return x25519, AESGCM


def is_e2e_credential(credential: str) -> bool:
    """End-to-end tunnel credentials say so ("." never occurs in a plain one)."""
    return credential.startswith(E2E_CREDENTIAL_PREFIX)


def tunnel_prologue(credential: str) -> bytes:
    """The handshake prologue binding a session to one tunnel: the hex
    SHA-256 of its credential (the instance learns it from the offer)."""
    return PROLOGUE + b"|" + credential_digest(credential).encode()


def credential_digest(credential: str) -> str:
    return hashlib.sha256(credential.encode()).hexdigest()


def b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def b64decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


@dataclass(frozen=True)
class E2EKeyPair:
    """An instance key pair for end-to-end tunnels, base64url without padding.

    Configure ``private_key`` on the instance (``mount(e2e_private_key=…)``)
    and give ``public_key`` to the issuer, which pins it in
    ``tunnels.open(e2e_key=…)``.
    """

    private_key: str
    public_key: str


def generate_e2e_key() -> E2EKeyPair:
    """A new instance key pair for end-to-end sealed tunnels."""
    x25519, _ = _crypto()
    key = x25519.X25519PrivateKey.generate()
    private = key.private_bytes_raw()
    return E2EKeyPair(
        b64encode(private), b64encode(key.public_key().public_bytes_raw())
    )


def e2e_public_key(private_key: str) -> str:
    """The public key of an instance's private key."""
    return b64encode(load_private_key(private_key).public_key().public_bytes_raw())


def load_public_key(text: str) -> Any:
    x25519, _ = _crypto()
    raw = _key_bytes(text, "an E2E key (the instance's X25519 public key)")
    return x25519.X25519PublicKey.from_public_bytes(raw)


def load_private_key(text: str) -> Any:
    x25519, _ = _crypto()
    raw = _key_bytes(text, "an E2E private key")
    return x25519.X25519PrivateKey.from_private_bytes(raw)


def _key_bytes(text: str, what: str) -> bytes:
    try:
        raw = b64decode(text)
    except (ValueError, TypeError):
        raw = b""
    if len(raw) != 32:
        raise ValueError(f"{what} is 32 bytes, base64url without padding")
    return raw


def check_public_key(text: str) -> str:
    _key_bytes(text, "an E2E key (the instance's X25519 public key)")
    return text


def check_private_key(text: str) -> str:
    _key_bytes(text, "an E2E private key")
    return text


# --- Noise NK -------------------------------------------------------------


def _hkdf2(ck: bytes, ikm: bytes) -> tuple[bytes, bytes]:
    temp = hmac.new(ck, ikm, hashlib.sha256).digest()
    a = hmac.new(temp, b"\x01", hashlib.sha256).digest()
    b = hmac.new(temp, a + b"\x02", hashlib.sha256).digest()
    return a, b


def _nonce(n: int) -> bytes:
    return b"\x00\x00\x00\x00" + n.to_bytes(8, "big")


class _Symmetric:
    def __init__(self, prologue: bytes) -> None:
        self.h = NOISE_PROTOCOL.ljust(32, b"\x00")  # shorter than HASHLEN
        self.ck = self.h
        self.k: bytes | None = None
        self.n = 0
        self.mix_hash(prologue)

    def mix_hash(self, data: bytes) -> None:
        self.h = hashlib.sha256(self.h + data).digest()

    def mix_key(self, ikm: bytes) -> None:
        self.ck, self.k = _hkdf2(self.ck, ikm)
        self.n = 0

    def encrypt_and_hash(self, plaintext: bytes) -> bytes:
        out = plaintext
        if self.k is not None:
            _, aesgcm = _crypto()
            out = aesgcm(self.k).encrypt(_nonce(self.n), plaintext, self.h)
            self.n += 1
        self.mix_hash(out)
        return out

    def decrypt_and_hash(self, ciphertext: bytes) -> bytes:
        out = ciphertext
        if self.k is not None:
            _, aesgcm = _crypto()
            out = aesgcm(self.k).decrypt(_nonce(self.n), ciphertext, self.h)
            self.n += 1
        self.mix_hash(ciphertext)
        return out

    def split(self) -> tuple[bytes, bytes]:
        return _hkdf2(self.ck, b"")


def _pub_bytes(key: Any) -> bytes:
    return key.public_key().public_bytes_raw()


class NKInitiator:
    """The client side of a handshake: message 1 towards the pinned key."""

    def __init__(
        self,
        remote_static: Any,
        *,
        prologue: bytes = PROLOGUE,
        ephemeral: Any = None,
    ) -> None:
        x25519, _ = _crypto()
        self._sym = _Symmetric(prologue)
        self._sym.mix_hash(remote_static.public_bytes_raw())
        self._e = ephemeral or x25519.X25519PrivateKey.generate()
        self._rs = remote_static

    def write_message_1(self, payload: bytes = b"") -> bytes:
        e_pub = _pub_bytes(self._e)
        self._sym.mix_hash(e_pub)
        self._sym.mix_key(self._e.exchange(self._rs))
        return e_pub + self._sym.encrypt_and_hash(payload)

    def read_message_2(self, message: bytes) -> tuple[bytes, Session]:
        x25519, _ = _crypto()
        if len(message) < 48:
            raise ValueError("short handshake message")
        re = x25519.X25519PublicKey.from_public_bytes(message[:32])
        self._sym.mix_hash(message[:32])
        self._sym.mix_key(self._e.exchange(re))
        payload = self._sym.decrypt_and_hash(message[32:])
        send, recv = self._sym.split()
        return payload, Session(self._sym.h, send, recv)


def nk_respond(
    static: Any,
    message: bytes,
    *,
    prologue: bytes = PROLOGUE,
    ephemeral: Any = None,
    payload: bytes = b"",
) -> tuple[bytes, bytes, Session]:
    """The instance side: read message 1, write message 2; (reply, received, session)."""
    x25519, _ = _crypto()
    if len(message) < 48:
        raise ValueError("short handshake message")
    sym = _Symmetric(prologue)
    sym.mix_hash(_pub_bytes(static))
    re = x25519.X25519PublicKey.from_public_bytes(message[:32])
    sym.mix_hash(message[:32])
    sym.mix_key(static.exchange(re))
    received = sym.decrypt_and_hash(message[32:])
    e = ephemeral or x25519.X25519PrivateKey.generate()
    e_pub = _pub_bytes(e)
    sym.mix_hash(e_pub)
    sym.mix_key(e.exchange(re))
    reply = e_pub + sym.encrypt_and_hash(payload)
    to_responder, to_initiator = sym.split()
    return reply, received, Session(sym.h, to_initiator, to_responder)


# --- sessions and sealed frames ---------------------------------------------


class Session:
    """One end of an E2E session: a sending key and counter, a receiving key
    with a replay window (the Router may drop or reorder frames, never
    replay them)."""

    def __init__(self, handshake_hash: bytes, send: bytes, recv: bytes) -> None:
        _, aesgcm = _crypto()
        self.hash = handshake_hash
        self.id = handshake_hash[:16].hex()
        self._send = aesgcm(send)
        self._recv = aesgcm(recv)
        self._send_n = 0
        self._top = 0  # highest counter received + 1
        self._window = 0  # bit i: counter top-1-i seen
        self._lock = threading.Lock()

    def seal(self, plaintext: bytes, ad: bytes) -> str:
        with self._lock:
            n = self._send_n
            self._send_n += 1
        return b64encode(
            n.to_bytes(8, "big") + self._send.encrypt(_nonce(n), plaintext, ad)
        )

    def open(self, sealed: str, ad: bytes) -> bytes:
        try:
            raw = b64decode(sealed)
        except (ValueError, TypeError) as exc:
            raise ValueError("malformed sealed field") from exc
        if len(raw) < 24:
            raise ValueError("malformed sealed field")
        n = int.from_bytes(raw[:8], "big")
        try:
            plaintext = self._recv.decrypt(_nonce(n), raw[8:], ad)
        except Exception as exc:
            raise ValueError("sealed field does not open") from exc
        with self._lock:
            if not self._accept(n):
                raise ValueError("sealed field replayed or too old")
        return plaintext

    def _accept(self, n: int) -> bool:
        if n >= self._top:
            shift = n + 1 - self._top
            self._window = ((self._window << shift) | 1) & ((1 << WINDOW) - 1)
            self._top = n + 1
            return True
        age = self._top - 1 - n
        if age >= WINDOW or self._window >> age & 1:
            return False
        self._window |= 1 << age
        return True


def _ad(
    kind: str,
    call_id: str,
    service: str,
    method: str,
    direction: str | None,
    sequence: int | None,
    session: str,
) -> bytes:
    seq = "" if sequence is None else str(sequence)
    fields = [
        PROLOGUE.decode(),
        kind,
        call_id,
        service,
        method,
        direction or "",
        seq,
        session,
    ]
    return "|".join(fields).encode()


def _without_reserved(metadata: dict[str, str] | None) -> dict[str, str] | None:
    if metadata is None:
        return None
    return {
        k: v for k, v in metadata.items() if k[: len(_RESERVED)].lower() != _RESERVED
    }


class SealedCall:
    """What both ends know of one sealed call: its session, service and method."""

    def __init__(self, session: Session, service: str, method: str) -> None:
        self.session = session
        self.service = service
        self.method = method

    def _ad_for(self, frame: Frame) -> bytes:
        return _ad(
            frame.kind,
            getattr(frame, "call_id", ""),
            self.service,
            self.method,
            getattr(frame, "direction", None),
            getattr(frame, "sequence", None),
            self.session.id,
        )

    def seal(self, frame: Frame) -> Frame:
        """Move ``frame``'s content into its sealed field."""
        parts: dict[str, Any] = {}
        update: dict[str, Any] = {}
        if isinstance(frame, (CallOpenFrame, CallResultFrame)):
            parts = {"payload": frame.payload, "metadata": frame.metadata}
            update = {"payload": None, "metadata": None}
        elif isinstance(frame, StreamItemFrame):
            parts = {"payload": frame.payload}
            update = {"payload": None}
        elif isinstance(frame, CallErrorFrame):
            parts = {
                "message": frame.error.message,
                "details": frame.error.details,
                "metadata": frame.metadata,
            }
            update = {
                "error": frame.error.model_copy(
                    update={"message": "sealed", "details": None}
                ),
                "metadata": None,
            }
        else:
            return frame  # windows, ends and cancels carry no content
        if isinstance(frame, CallOpenFrame):
            update["e2e_session"] = self.session.id
        sealed_frame = frame.model_copy(update=update)
        plaintext = json.dumps(
            {k: v for k, v in parts.items() if v is not None}
        ).encode()
        return sealed_frame.model_copy(
            update={"sealed": self.session.seal(plaintext, self._ad_for(sealed_frame))}
        )

    def open(self, frame: Frame) -> Frame:
        """Restore ``frame``'s content; content frames in clear are refused
        (they did not come from the other end); a clear ``call.error`` (the
        Router's own) is kept."""
        sealed = getattr(frame, "sealed", None)
        if not sealed:
            if isinstance(frame, (CallOpenFrame, CallResultFrame, StreamItemFrame)):
                raise ValueError("a frame of a sealed call arrived in clear")
            return frame
        parts = json.loads(self.session.open(sealed, self._ad_for(frame)))
        if not isinstance(parts, dict):
            raise ValueError("sealed field is not a sealed object")  # noqa: TRY004
        metadata = _without_reserved(parts.get("metadata"))
        if isinstance(frame, CallErrorFrame):
            error = ErrorPayload(
                code=frame.error.code,
                message=str(parts.get("message", "")),
                retryable=frame.error.retryable,
                details=parts.get("details"),
            )
            return frame.model_copy(
                update={"error": error, "metadata": metadata, "sealed": None}
            )
        update: dict[str, Any] = {"payload": parts.get("payload"), "sealed": None}
        if isinstance(frame, CallOpenFrame):
            # The Router's clear keys (the tunnel identity) win over sealed ones.
            update["metadata"] = {**(metadata or {}), **(frame.metadata or {})} or None
        elif not isinstance(frame, StreamItemFrame):
            update["metadata"] = metadata
        return frame.model_copy(update=update)


class SealingConnection:
    """Seals the frames one end-to-end call sends over a connection."""

    def __init__(self, connection: Any, sealed: SealedCall) -> None:
        self._connection = connection
        self._sealed = sealed

    async def send(self, frame: Frame) -> None:
        await self._connection.send(self._sealed.seal(frame))


class SessionStore:
    """An instance's sessions under each tunnel, at most 16 each (oldest dropped)."""

    def __init__(self) -> None:
        self._by_tunnel: dict[str, OrderedDict[str, Session]] = {}

    def add(self, tunnel_id: str, session: Session) -> None:
        sessions = self._by_tunnel.setdefault(tunnel_id, OrderedDict())
        sessions[session.id] = session
        while len(sessions) > MAX_SESSIONS:
            sessions.popitem(last=False)

    def find(self, tunnel_id: str, session_id: str) -> Session | None:
        return self._by_tunnel.get(tunnel_id, {}).get(session_id)

    def drop(self, tunnel_id: str) -> None:
        self._by_tunnel.pop(tunnel_id, None)


def handshake_failed(why: str, source: str) -> UnauthenticatedError:
    return UnauthenticatedError(
        f"End-to-end handshake failed: {why}",
        details={"source": source, "reason": "e2e_handshake_failed"},
    )

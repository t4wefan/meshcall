"""End-to-end sealed tunnels (docs/protocol.md → End-to-end encryption).

A Noise handshake per client connection (``NK``, or ``IK`` / ``IKpsk2`` when
the client authenticates itself), then the payloads, metadata and error
messages of every call travel in a ``sealed`` field while the Router routes
on the clear header. Needs the ``cryptography`` package
(``pip install 'meshcall[e2e]'``).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
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

NK = "Noise_NK_25519_AESGCM_SHA256"
IK = "Noise_IK_25519_AESGCM_SHA256"
IKPSK2 = "Noise_IKpsk2_25519_AESGCM_SHA256"
#: Message patterns after the responder's static key (``<- s``), which all
#: three know beforehand.
_PATTERNS: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    NK: (("e", "es"), ("e", "ee")),
    IK: (("e", "es", "s", "ss"), ("e", "ee", "se")),
    IKPSK2: (("e", "es", "s", "ss"), ("e", "ee", "se", "psk")),
}
PROLOGUE = b"meshcall-e2e/1"
MAX_PAIRING_ID = 128
#: A pairing confirmation may wait for a person at the instance.
CONFIRM_TIMEOUT_MS = 120_000
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


def peer_prologue(credential: str, instance_id: str) -> bytes:
    """The IK / IKpsk2 prologue: the tunnel, and the instance the client
    pinned the key of."""
    return tunnel_prologue(credential) + b"|" + instance_id.encode()


def confirm_ad(session_id: str) -> bytes:
    """The associated data of a pairing confirmation."""
    return PROLOGUE + b"|confirm|" + session_id.encode()


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


@dataclass(frozen=True)
class E2EPairing:
    """A one-time pairing the instance's owner handed the client out of band
    (never through the Router): its id and 32-byte PSK, base64url."""

    id: str
    psk: str = field(repr=False)


@dataclass(frozen=True)
class E2EIdentity:
    """A client's own key for end-to-end tunnels: with it the client
    authenticates itself to the instance (Noise ``IK``).

    ``private_key`` is this client's static key (``generate_e2e_key()``, kept by
    the app); ``instance_id`` the instance whose key the client pinned in
    ``e2e_key``, bound in the handshake. ``pairing`` makes the first handshake
    a pairing (``IKpsk2``); once the instance confirmed it, later handshakes
    use ``IK``.
    """

    private_key: str = field(repr=False)
    instance_id: str
    pairing: E2EPairing | None = None


@dataclass(frozen=True)
class E2EPeer:
    """The end-to-end session of a call (``ctx.e2e``), or the client an
    instance is asked to accept (``authorize_e2e_peer``).

    ``peer_key`` is the client's static public key (base64url), ``None`` for an
    ``NK`` session, whose client has none; ``pairing_id`` the pairing the
    session was made with.
    """

    protocol: str
    session: str
    tunnel: Any  # meshcall.tunnel.Tunnel
    peer_key: str | None = None
    pairing_id: str | None = None


def generate_e2e_key() -> E2EKeyPair:
    """A new key pair for end-to-end sealed tunnels (an instance's or a client's)."""
    x25519, _ = _crypto()
    key = x25519.X25519PrivateKey.generate()
    private = key.private_bytes_raw()
    return E2EKeyPair(
        b64encode(private), b64encode(key.public_key().public_bytes_raw())
    )


def generate_e2e_psk() -> str:
    """A new pairing secret: 32 random bytes, base64url without padding."""
    return b64encode(os.urandom(32))


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


def check_identity(identity: E2EIdentity) -> E2EIdentity:
    check_private_key(identity.private_key)
    if not identity.instance_id:
        raise ValueError("E2EIdentity needs the instance_id whose key you pinned")
    if identity.pairing is not None:
        check_pairing_id(identity.pairing.id)
        _key_bytes(identity.pairing.psk, "a pairing PSK")
    return identity


def check_pairing_id(pairing_id: str) -> str:
    if not 1 <= len(pairing_id.encode()) <= MAX_PAIRING_ID:
        raise ValueError(f"a pairing id is 1 to {MAX_PAIRING_ID} bytes")
    return pairing_id


def psk_bytes(text: str) -> bytes:
    return _key_bytes(text, "a pairing PSK")


# --- Noise NK, IK, IKpsk2 --------------------------------------------------


def _hkdf(ck: bytes, ikm: bytes, n: int) -> list[bytes]:
    temp = hmac.new(ck, ikm, hashlib.sha256).digest()
    out: list[bytes] = []
    prev = b""
    for i in range(1, n + 1):
        prev = hmac.new(temp, prev + bytes([i]), hashlib.sha256).digest()
        out.append(prev)
    return out


def _nonce(n: int) -> bytes:
    return b"\x00\x00\x00\x00" + n.to_bytes(8, "big")


class _Symmetric:
    def __init__(self, protocol: str, prologue: bytes) -> None:
        name = protocol.encode()
        # Every name here is at most HASHLEN, so it is padded, not hashed.
        self.h = name.ljust(32, b"\x00")
        self.ck = self.h
        self.k: bytes | None = None
        self.n = 0
        self.mix_hash(prologue)

    def mix_hash(self, data: bytes) -> None:
        self.h = hashlib.sha256(self.h + data).digest()

    def mix_key(self, ikm: bytes) -> None:
        self.ck, self.k = _hkdf(self.ck, ikm, 2)
        self.n = 0

    def mix_key_and_hash(self, ikm: bytes) -> None:
        self.ck, temp_h, self.k = _hkdf(self.ck, ikm, 3)
        self.mix_hash(temp_h)
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
        a, b = _hkdf(self.ck, b"", 2)
        return a, b


def _pub_bytes(key: Any) -> bytes:
    return key.public_key().public_bytes_raw()


class Handshake:
    """One end of a two-message Noise handshake towards (or as) a responder
    whose static key the initiator knows: NK, IK or IKpsk2.

    ``s`` is this end's static private key (the responder's; the initiator's
    for IK and IKpsk2), ``rs`` the responder's public key on the initiator,
    ``psk`` the pairing secret for IKpsk2. ``ephemeral`` is for test vectors.
    """

    def __init__(
        self,
        protocol: str,
        *,
        initiator: bool,
        prologue: bytes,
        s: Any = None,
        rs: Any = None,
        psk: bytes | None = None,
        ephemeral: Any = None,
    ) -> None:
        if protocol not in _PATTERNS:
            raise ValueError(f"unknown handshake {protocol}")
        if (protocol == IKPSK2) != (psk is not None):
            raise ValueError("IKpsk2, and only IKpsk2, takes a PSK")
        if psk is not None and len(psk) != 32:
            raise ValueError("a pairing PSK is 32 bytes")
        if protocol != NK and s is None:
            raise ValueError(f"{protocol} needs this end's static key")
        self.protocol = protocol
        self._sym = _Symmetric(protocol, prologue)
        self._initiator = initiator
        self._s = s
        self._rs = rs
        self._psk = psk
        self._e = ephemeral
        self._re: Any = None
        self._turn = 0
        responder_static = rs if initiator else _public(s)
        self._sym.mix_hash(responder_static.public_bytes_raw())

    @property
    def remote_static(self) -> Any:
        """The other end's static public key (the client's, on the responder)."""
        return self._rs

    def _dh(self, ours: Any, theirs: Any) -> bytes:
        return ours.exchange(theirs)

    def write_message(self, payload: bytes = b"") -> bytes:
        x25519, _ = _crypto()
        tokens = _PATTERNS[self.protocol][self._turn]
        self._turn += 1
        out = b""
        for token in tokens:
            if token == "e":
                if self._e is None:
                    self._e = x25519.X25519PrivateKey.generate()
                pub = _pub_bytes(self._e)
                out += pub
                self._sym.mix_hash(pub)
                if self._psk is not None:
                    self._sym.mix_key(pub)
            elif token == "s":
                out += self._sym.encrypt_and_hash(_pub_bytes(self._s))
            elif token == "psk":
                assert self._psk is not None
                self._sym.mix_key_and_hash(self._psk)
            else:
                self._sym.mix_key(self._token_dh(token))
        return out + self._sym.encrypt_and_hash(payload)

    def read_message(self, message: bytes) -> bytes:
        x25519, _ = _crypto()
        tokens = _PATTERNS[self.protocol][self._turn]
        self._turn += 1
        rest = message
        for token in tokens:
            if token == "e":
                if len(rest) < 32:
                    raise ValueError("short handshake message")
                self._re = x25519.X25519PublicKey.from_public_bytes(rest[:32])
                self._sym.mix_hash(rest[:32])
                if self._psk is not None:
                    self._sym.mix_key(rest[:32])
                rest = rest[32:]
            elif token == "s":
                size = 48 if self._sym.k is not None else 32
                if len(rest) < size:
                    raise ValueError("short handshake message")
                raw = self._sym.decrypt_and_hash(rest[:size])
                self._rs = x25519.X25519PublicKey.from_public_bytes(raw)
                rest = rest[size:]
            elif token == "psk":
                assert self._psk is not None
                self._sym.mix_key_and_hash(self._psk)
            else:
                self._sym.mix_key(self._token_dh(token))
        if self._sym.k is not None and len(rest) < 16:
            raise ValueError("short handshake message")
        return self._sym.decrypt_and_hash(rest)

    def _token_dh(self, token: str) -> bytes:
        # The first letter is the initiator's key, the second the responder's.
        first, second = token
        mine, theirs = (first, second) if self._initiator else (second, first)
        local = self._e if mine == "e" else self._s
        remote = self._re if theirs == "e" else self._rs
        return self._dh(local, remote)

    def split(self) -> Session:
        """The session once both messages are done."""
        to_responder, to_initiator = self._sym.split()
        if self._initiator:
            return Session(self._sym.h, to_responder, to_initiator, self.protocol)
        return Session(self._sym.h, to_initiator, to_responder, self.protocol)


def _public(private: Any) -> Any:
    return private.public_key()


# --- sessions and sealed frames ---------------------------------------------


class Session:
    """One end of an E2E session: a sending key and counter, a receiving key
    with a replay window (the Router may drop or reorder frames, never
    replay them)."""

    def __init__(
        self, handshake_hash: bytes, send: bytes, recv: bytes, protocol: str = NK
    ) -> None:
        _, aesgcm = _crypto()
        self.hash = handshake_hash
        self.id = handshake_hash[:16].hex()
        self.protocol = protocol
        #: The client's static public key (base64url) on the instance, for
        #: IK and IKpsk2 sessions.
        self.peer_key: str | None = None
        self.pairing_id: str | None = None
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
    """An instance's sessions under each tunnel, at most 16 each (oldest
    dropped), and as many pairing sessions waiting for their confirmation."""

    def __init__(self) -> None:
        self._by_tunnel: dict[str, OrderedDict[str, Session]] = {}
        self._pending: dict[str, OrderedDict[str, Session]] = {}

    @staticmethod
    def _put(
        table: dict[str, OrderedDict[str, Session]], tunnel_id: str, session: Session
    ) -> None:
        sessions = table.setdefault(tunnel_id, OrderedDict())
        sessions[session.id] = session
        while len(sessions) > MAX_SESSIONS:
            sessions.popitem(last=False)

    def add(self, tunnel_id: str, session: Session) -> None:
        self._put(self._by_tunnel, tunnel_id, session)

    def add_pending(self, tunnel_id: str, session: Session) -> None:
        self._put(self._pending, tunnel_id, session)

    def take_pending(self, tunnel_id: str, session_id: str) -> Session | None:
        """Remove and return a pairing session: each is confirmed at most once."""
        return self._pending.get(tunnel_id, OrderedDict()).pop(session_id, None)

    def find(self, tunnel_id: str, session_id: str) -> Session | None:
        return self._by_tunnel.get(tunnel_id, {}).get(session_id)

    def drop(self, tunnel_id: str) -> None:
        self._by_tunnel.pop(tunnel_id, None)
        self._pending.pop(tunnel_id, None)


def handshake_failed(why: str, source: str) -> UnauthenticatedError:
    return UnauthenticatedError(
        f"End-to-end handshake failed: {why}",
        details={"source": source, "reason": "e2e_handshake_failed"},
    )

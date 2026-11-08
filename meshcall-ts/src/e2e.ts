// End-to-end sealed tunnels (docs/protocol.md → End-to-end encryption).
//
// A Noise_NK_25519_AESGCM_SHA256 handshake per client connection, then the
// payloads, metadata and error messages of every call travel in a `sealed`
// field while the Router routes on the clear header. WebCrypto only (no Node
// imports), so the Node runtime and `meshcall/web` share it.

import { MeshCallError, UnauthenticatedError } from "./errors.js";
import { isReservedKey } from "./policy.js";
import type { CallErrorFrame, CallOpenFrame, ErrorPayload, Frame, Metadata } from "./protocol.js";

export const E2E_PROLOGUE = "meshcall-e2e/1";
/** How far behind the newest counter a sealed frame may arrive. */
export const E2E_WINDOW = 1024;
/** Sessions an instance keeps per tunnel (oldest dropped). */
export const E2E_MAX_SESSIONS = 16;
export const TUNNEL_HANDSHAKE_METHOD = "handshake";
/** End-to-end tunnel credentials say so ("." never occurs in a plain one). */
export const E2E_CREDENTIAL_PREFIX = "mtun_e2e.";
/** The Router answers these services itself: calls to them are never sealed. */
export const ROUTER_SERVICE_PREFIX = "meshcall.router.";

export function isE2ECredential(credential: string | undefined): boolean {
  return credential?.startsWith(E2E_CREDENTIAL_PREFIX) === true;
}

/**
 * A credential and a key must agree, before anything is sent: an end-to-end
 * tunnel never sees an unsealed payload by mistake.
 */
export function checkE2EAgreement(credential: string | undefined, e2eKey: string | undefined): void {
  if (isE2ECredential(credential) && e2eKey === undefined) {
    throw new TypeError("This is an end-to-end tunnel credential: pass the instance's e2eKey the issuer gave you");
  }
  if (e2eKey !== undefined && !isE2ECredential(credential)) {
    throw new TypeError("e2eKey needs the credential of an end-to-end tunnel (mtun_e2e.…)");
  }
}

/** The hex SHA-256 of a tunnel credential, which the handshake prologue binds. */
export async function credentialDigest(credential: string): Promise<string> {
  return hex(await sha256(text.encode(credential)));
}

/** The handshake prologue binding a session to the tunnel with this credential digest. */
export function tunnelPrologue(digest: string): Uint8Array {
  return text.encode(E2E_PROLOGUE + "|" + digest);
}

const NOISE_PROTOCOL = "Noise_NK_25519_AESGCM_SHA256";
const SEALED_NOTICE = "sealed";
// DER header of a PKCS#8 X25519 private key: WebCrypto imports no raw private keys.
const PKCS8_X25519 = Uint8Array.from([
  0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x6e, 0x04, 0x22, 0x04, 0x20,
]);
const text = new TextEncoder();

function subtle(): SubtleCrypto {
  const value = globalThis.crypto?.subtle;
  if (value === undefined) throw new Error("End-to-end tunnels need WebCrypto (crypto.subtle; a secure context in browsers)");
  return value;
}

// WebCrypto takes ArrayBuffer-backed views; copying also detaches from caller buffers.
const buf = (bytes: Uint8Array): Uint8Array<ArrayBuffer> => Uint8Array.from(bytes);

function concat(...parts: Uint8Array[]): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const part of parts) {
    out.set(part, at);
    at += part.length;
  }
  return out;
}

export function b64encode(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/u, "");
}

export function b64decode(value: string): Uint8Array<ArrayBuffer> {
  if (!/^[A-Za-z0-9_-]*$/u.test(value)) throw new RangeError("not base64url");
  const binary = atob(value.replaceAll("-", "+").replaceAll("_", "/") + "=".repeat((4 - (value.length % 4)) % 4));
  return Uint8Array.from(binary, (c) => c.charCodeAt(0));
}

function keyBytes(value: string, what: string): Uint8Array<ArrayBuffer> {
  let raw: Uint8Array<ArrayBuffer>;
  try {
    raw = typeof value === "string" ? b64decode(value) : new Uint8Array();
  } catch {
    raw = new Uint8Array();
  }
  if (raw.length !== 32) throw new TypeError(what + " is 32 bytes, base64url without padding");
  return raw;
}

/** Check an instance public key as the issuer pins it; returns it unchanged. */
export function checkE2EKey(value: string): string {
  keyBytes(value, "An E2E key (the instance's X25519 public key)");
  return value;
}

export function checkE2EPrivateKey(value: string): string {
  keyBytes(value, "An E2E private key");
  return value;
}

/**
 * An instance key pair for end-to-end tunnels, base64url without padding.
 * Configure `privateKey` on the instance (`mount(..., { e2ePrivateKey })`) and
 * give `publicKey` to the issuer, which pins it in `tunnels.open({ e2eKey })`.
 */
export interface E2EKeyPair {
  readonly privateKey: string;
  readonly publicKey: string;
}

/** A new instance key pair for end-to-end sealed tunnels. */
export async function generateE2EKey(): Promise<E2EKeyPair> {
  const pair = await subtle().generateKey({ name: "X25519" }, true, ["deriveBits"]) as CryptoKeyPair;
  const pkcs8 = new Uint8Array(await subtle().exportKey("pkcs8", pair.privateKey));
  const raw = new Uint8Array(await subtle().exportKey("raw", pair.publicKey));
  return { privateKey: b64encode(pkcs8.slice(PKCS8_X25519.length)), publicKey: b64encode(raw) };
}

/** The public key of an instance's private key. */
export async function e2ePublicKey(privateKey: string): Promise<string> {
  return b64encode(await publicBytes(await importPrivate(keyBytes(privateKey, "An E2E private key"))));
}

async function importPrivate(raw: Uint8Array): Promise<CryptoKey> {
  return subtle().importKey("pkcs8", concat(PKCS8_X25519, raw), { name: "X25519" }, true, ["deriveBits"]);
}

async function importPublic(raw: Uint8Array): Promise<CryptoKey> {
  return subtle().importKey("raw", buf(raw), { name: "X25519" }, true, []);
}

async function publicBytes(privateKey: CryptoKey): Promise<Uint8Array<ArrayBuffer>> {
  const jwk = await subtle().exportKey("jwk", privateKey);
  return b64decode(jwk.x ?? "");
}

async function dh(privateKey: CryptoKey, publicKey: CryptoKey): Promise<Uint8Array<ArrayBuffer>> {
  return new Uint8Array(await subtle().deriveBits({ name: "X25519", public: publicKey }, privateKey, 256));
}

async function sha256(data: Uint8Array): Promise<Uint8Array<ArrayBuffer>> {
  return new Uint8Array(await subtle().digest("SHA-256", buf(data)));
}

async function hmac(key: Uint8Array, data: Uint8Array): Promise<Uint8Array<ArrayBuffer>> {
  const k = await subtle().importKey("raw", buf(key), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return new Uint8Array(await subtle().sign("HMAC", k, buf(data)));
}

async function hkdf2(ck: Uint8Array, ikm: Uint8Array): Promise<[Uint8Array<ArrayBuffer>, Uint8Array<ArrayBuffer>]> {
  const temp = await hmac(ck, ikm);
  const a = await hmac(temp, Uint8Array.of(1));
  return [a, await hmac(temp, concat(a, Uint8Array.of(2)))];
}

async function aesKey(raw: Uint8Array): Promise<CryptoKey> {
  return subtle().importKey("raw", buf(raw), "AES-GCM", false, ["encrypt", "decrypt"]);
}

function nonce(n: bigint): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(12);
  new DataView(out.buffer).setBigUint64(4, n);
  return out;
}

/** AES-256-GCM with the Noise nonce (4 zero bytes, 8-byte big-endian counter). */
export async function aeadEncrypt(key: CryptoKey, n: bigint, plaintext: Uint8Array, ad: Uint8Array): Promise<Uint8Array<ArrayBuffer>> {
  return new Uint8Array(await subtle().encrypt({ name: "AES-GCM", iv: nonce(n), additionalData: buf(ad) }, key, buf(plaintext)));
}

async function aeadDecrypt(key: CryptoKey, n: bigint, ciphertext: Uint8Array, ad: Uint8Array): Promise<Uint8Array<ArrayBuffer>> {
  return new Uint8Array(await subtle().decrypt({ name: "AES-GCM", iv: nonce(n), additionalData: buf(ad) }, key, buf(ciphertext)));
}

// --- Noise NK ---------------------------------------------------------------

class SymmetricState {
  private k: CryptoKey | undefined;
  private n = 0n;

  private constructor(public h: Uint8Array, private ck: Uint8Array) {}

  public static async create(prologue: Uint8Array): Promise<SymmetricState> {
    const name = new Uint8Array(32); // shorter than HASHLEN: zero-padded
    name.set(text.encode(NOISE_PROTOCOL));
    const state = new SymmetricState(name, name);
    await state.mixHash(prologue);
    return state;
  }

  public async mixHash(data: Uint8Array): Promise<void> {
    this.h = await sha256(concat(this.h, data));
  }

  public async mixKey(ikm: Uint8Array): Promise<void> {
    const [ck, k] = await hkdf2(this.ck, ikm);
    this.ck = ck;
    this.k = await aesKey(k);
    this.n = 0n;
  }

  public async encryptAndHash(plaintext: Uint8Array): Promise<Uint8Array> {
    let out: Uint8Array = plaintext;
    if (this.k !== undefined) {
      out = await aeadEncrypt(this.k, this.n, plaintext, this.h);
      this.n += 1n;
    }
    await this.mixHash(out);
    return out;
  }

  public async decryptAndHash(ciphertext: Uint8Array): Promise<Uint8Array> {
    let out: Uint8Array = ciphertext;
    if (this.k !== undefined) {
      out = await aeadDecrypt(this.k, this.n, ciphertext, this.h);
      this.n += 1n;
    }
    await this.mixHash(ciphertext);
    return out;
  }

  public split(): Promise<[Uint8Array, Uint8Array]> {
    return hkdf2(this.ck, new Uint8Array());
  }
}

export interface NKOptions {
  readonly prologue?: Uint8Array;
  /** A fixed ephemeral private key (raw 32 bytes): test vectors only. */
  readonly ephemeral?: Uint8Array;
  readonly payload?: Uint8Array;
}

async function ephemeralKey(raw: Uint8Array | undefined): Promise<CryptoKey> {
  if (raw !== undefined) return importPrivate(raw);
  const pair = await subtle().generateKey({ name: "X25519" }, true, ["deriveBits"]) as CryptoKeyPair;
  return pair.privateKey;
}

/** The client side of a handshake: message 1 towards the pinned key, then `finish(message2)`. */
export async function nkInitiate(remoteStatic: Uint8Array, options: NKOptions = {}): Promise<{
  readonly message: Uint8Array;
  finish(message2: Uint8Array): Promise<{ readonly payload: Uint8Array; readonly session: E2ESession }>;
}> {
  const rs = await importPublic(remoteStatic);
  const sym = await SymmetricState.create(options.prologue ?? text.encode(E2E_PROLOGUE));
  await sym.mixHash(remoteStatic);
  const e = await ephemeralKey(options.ephemeral);
  const ePub = await publicBytes(e);
  await sym.mixHash(ePub);
  await sym.mixKey(await dh(e, rs));
  const message = concat(ePub, await sym.encryptAndHash(options.payload ?? new Uint8Array()));
  return {
    message,
    async finish(message2) {
      if (message2.length < 48) throw new RangeError("short handshake message");
      const re = await importPublic(message2.subarray(0, 32));
      await sym.mixHash(message2.subarray(0, 32));
      await sym.mixKey(await dh(e, re));
      const payload = await sym.decryptAndHash(message2.subarray(32));
      const [send, recv] = await sym.split();
      return { payload, session: await E2ESession.create(sym.h, send, recv) };
    },
  };
}

/** The instance side: read message 1, write message 2. */
export async function nkRespond(staticPrivate: Uint8Array, message: Uint8Array, options: NKOptions = {}): Promise<{
  readonly reply: Uint8Array; readonly received: Uint8Array; readonly session: E2ESession;
}> {
  if (message.length < 48) throw new RangeError("short handshake message");
  const s = await importPrivate(staticPrivate);
  const sym = await SymmetricState.create(options.prologue ?? text.encode(E2E_PROLOGUE));
  await sym.mixHash(await publicBytes(s));
  const re = await importPublic(message.subarray(0, 32));
  await sym.mixHash(message.subarray(0, 32));
  await sym.mixKey(await dh(s, re));
  const received = await sym.decryptAndHash(message.subarray(32));
  const e = await ephemeralKey(options.ephemeral);
  const ePub = await publicBytes(e);
  await sym.mixHash(ePub);
  await sym.mixKey(await dh(e, re));
  const reply = concat(ePub, await sym.encryptAndHash(options.payload ?? new Uint8Array()));
  const [toResponder, toInitiator] = await sym.split();
  return { reply, received, session: await E2ESession.create(sym.h, toInitiator, toResponder) };
}

// --- sessions and sealed frames ------------------------------------------------

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * One end of an E2E session: a sending key and counter, a receiving key with
 * a replay window (the Router may drop or reorder frames, never replay them).
 */
export class E2ESession {
  public readonly id: string;
  private sendN = 0n;
  private top = 0n; // highest counter received + 1
  private window = 0n; // bit i: counter top-1-i seen

  private constructor(
    public readonly hash: Uint8Array,
    /** Exposed for the test vector. */
    public readonly sendKey: CryptoKey,
    private readonly recvKey: CryptoKey,
  ) {
    this.id = hex(hash.subarray(0, 16));
  }

  public static async create(hash: Uint8Array, send: Uint8Array, recv: Uint8Array): Promise<E2ESession> {
    return new E2ESession(hash, await aesKey(send), await aesKey(recv));
  }

  public async seal(plaintext: Uint8Array, ad: Uint8Array): Promise<string> {
    const n = this.sendN;
    this.sendN += 1n;
    const counter = new Uint8Array(8);
    new DataView(counter.buffer).setBigUint64(0, n);
    return b64encode(concat(counter, await aeadEncrypt(this.sendKey, n, plaintext, ad)));
  }

  public async open(sealed: string, ad: Uint8Array): Promise<Uint8Array> {
    let raw: Uint8Array<ArrayBuffer>;
    try {
      raw = b64decode(sealed);
    } catch {
      throw new RangeError("malformed sealed field");
    }
    if (raw.length < 24) throw new RangeError("malformed sealed field");
    const n = new DataView(raw.buffer).getBigUint64(0);
    let plaintext: Uint8Array;
    try {
      plaintext = await aeadDecrypt(this.recvKey, n, raw.subarray(8), ad);
    } catch {
      throw new RangeError("sealed field does not open");
    }
    if (!this.accept(n)) throw new RangeError("sealed field replayed or too old");
    return plaintext;
  }

  private accept(n: bigint): boolean {
    const size = BigInt(E2E_WINDOW);
    if (n >= this.top) {
      const shift = n + 1n - this.top;
      this.window = shift >= size ? 1n : ((this.window << shift) | 1n) & ((1n << size) - 1n);
      this.top = n + 1n;
      return true;
    }
    const age = this.top - 1n - n;
    if (age >= size || ((this.window >> age) & 1n) === 1n) return false;
    this.window |= 1n << age;
    return true;
  }
}

function withoutReserved(metadata: unknown): Metadata | undefined {
  if (metadata === null || typeof metadata !== "object") return undefined;
  const out: Metadata = {};
  for (const [key, value] of Object.entries(metadata as Record<string, unknown>)) {
    if (!isReservedKey(key) && typeof value === "string") out[key] = value;
  }
  return out;
}

type Mutable<T> = { -readonly [K in keyof T]: T[K] };

/** What both ends know of one sealed call: its session, service and method. */
export class SealedCall {
  public constructor(
    public readonly session: E2ESession,
    public readonly service: string,
    public readonly method: string,
  ) {}

  private ad(frame: Frame): Uint8Array {
    const f = frame as { call_id?: string; direction?: string; sequence?: number };
    return text.encode([
      E2E_PROLOGUE, frame.kind, f.call_id ?? "", this.service, this.method,
      f.direction ?? "", f.sequence === undefined ? "" : String(f.sequence), this.session.id,
    ].join("|"));
  }

  /** Move `frame`'s content into its sealed field. */
  public async seal(frame: Frame): Promise<Frame> {
    const out = { ...frame } as Mutable<Frame> & Record<string, unknown>;
    const parts: Record<string, unknown> = {};
    if (frame.kind === "call.open" || frame.kind === "call.result" || frame.kind === "stream.item") {
      parts.payload = frame.payload;
      if (frame.kind !== "stream.item" && frame.metadata != null) parts.metadata = frame.metadata;
      out.payload = null;
      delete out.metadata;
      if (frame.kind === "call.open") out.e2e_session = this.session.id;
    } else if (frame.kind === "call.error") {
      parts.message = frame.error.message;
      if (frame.error.details != null) parts.details = frame.error.details;
      if (frame.metadata != null) parts.metadata = frame.metadata;
      const error: ErrorPayload = { ...frame.error, message: SEALED_NOTICE };
      delete error.details;
      out.error = error;
      delete out.metadata;
    } else {
      return frame; // windows, ends and cancels carry no content
    }
    if (parts.payload === undefined) delete parts.payload;
    out.sealed = await this.session.seal(text.encode(JSON.stringify(parts)), this.ad(out as Frame));
    return out as Frame;
  }

  /**
   * Restore `frame`'s content. Content frames in clear did not come from the
   * other end and are refused; a clear `call.error` (the Router's own) is kept.
   */
  public async open(frame: Frame): Promise<Frame> {
    const sealed = (frame as { sealed?: unknown }).sealed;
    if (typeof sealed !== "string" || sealed === "") {
      if (frame.kind === "call.open" || frame.kind === "call.result" || frame.kind === "stream.item") {
        throw new RangeError("a frame of a sealed call arrived in clear");
      }
      return frame;
    }
    const parts: unknown = JSON.parse(new TextDecoder().decode(await this.session.open(sealed, this.ad(frame))));
    if (parts === null || typeof parts !== "object" || Array.isArray(parts)) {
      throw new RangeError("sealed field is not a sealed object");
    }
    const p = parts as { payload?: unknown; metadata?: unknown; message?: unknown; details?: unknown };
    const metadata = withoutReserved(p.metadata);
    const out = { ...frame } as Mutable<Frame> & Record<string, unknown>;
    delete out.sealed;
    if (frame.kind === "call.error") {
      const error: ErrorPayload = { ...frame.error, message: typeof p.message === "string" ? p.message : "" };
      if (p.details !== null && typeof p.details === "object") error.details = p.details as Record<string, unknown>;
      else delete error.details;
      out.error = error;
      setMetadata(out, metadata);
      return out as Frame;
    }
    out.payload = p.payload ?? null;
    if (frame.kind === "call.open") {
      // The Router's clear keys (the tunnel identity) win over sealed ones.
      const merged = { ...metadata, ...(frame.metadata ?? {}) };
      setMetadata(out, Object.keys(merged).length === 0 ? undefined : merged);
    } else if (frame.kind === "call.result") {
      setMetadata(out, metadata);
    }
    return out as Frame;
  }
}

function setMetadata(frame: Record<string, unknown>, metadata: Metadata | undefined): void {
  if (metadata === undefined) delete frame.metadata;
  else frame.metadata = metadata;
}

/** An instance's sessions under each tunnel, at most 16 each (oldest dropped). */
export class E2ESessionStore {
  private readonly byTunnel = new Map<string, Map<string, E2ESession>>();

  public add(tunnelId: string, session: E2ESession): void {
    let sessions = this.byTunnel.get(tunnelId);
    if (sessions === undefined) this.byTunnel.set(tunnelId, sessions = new Map());
    sessions.set(session.id, session);
    while (sessions.size > E2E_MAX_SESSIONS) sessions.delete(sessions.keys().next().value as string);
  }

  public find(tunnelId: string, sessionId: string): E2ESession | undefined {
    return this.byTunnel.get(tunnelId)?.get(sessionId);
  }

  public drop(tunnelId: string): void {
    this.byTunnel.delete(tunnelId);
  }
}

export function handshakeFailed(why: string, source: "client" | "server"): UnauthenticatedError {
  return new UnauthenticatedError("End-to-end handshake failed: " + why, {
    details: { source, reason: "e2e_handshake_failed" },
  });
}

/** Constant-time equality of two base64url keys. */
export function sameKey(a: string, b: string): boolean {
  let x: Uint8Array;
  let y: Uint8Array;
  try {
    x = b64decode(a);
    y = b64decode(b);
  } catch {
    return false;
  }
  if (x.length !== y.length) return false;
  let diff = 0;
  for (let i = 0; i < x.length; i += 1) diff |= (x[i] as number) ^ (y[i] as number);
  return diff === 0;
}

/**
 * The client's handshake on a fresh connection to an E2E tunnel: `frame` is
 * the `TunnelHost/handshake` call to send; `finish` reads its answer.
 */
export async function clientHandshake(pinnedKey: string, callId: string, credential: string): Promise<{
  readonly frame: CallOpenFrame;
  finish(reply: Frame): Promise<E2ESession>;
}> {
  // The prologue binds the session to this credential's tunnel.
  const initiator = await nkInitiate(keyBytes(pinnedKey, "An E2E key (the instance's X25519 public key)"), {
    prologue: tunnelPrologue(await credentialDigest(credential)),
  });
  return {
    frame: {
      kind: "call.open", call_id: callId, service: "meshcall.router.v1.TunnelHost",
      method: TUNNEL_HANDSHAKE_METHOD, payload: { message: b64encode(initiator.message) },
    },
    async finish(reply) {
      if (reply.kind === "call.error") throw handshakeError(reply);
      if (reply.kind !== "call.result") throw handshakeFailed("unexpected " + reply.kind, "client");
      const message = (reply.payload as { message?: unknown } | null | undefined)?.message;
      try {
        return (await initiator.finish(b64decode(typeof message === "string" ? message : ""))).session;
      } catch {
        // Only the holder of the pinned private key can write message 2.
        throw handshakeFailed("message 2 is not from the pinned instance key", "client");
      }
    },
  };
}

function handshakeError(frame: CallErrorFrame): MeshCallError {
  if (frame.error.details?.reason === "e2e_handshake_failed") {
    // The instance's own answer: it could not open message 1.
    return new UnauthenticatedError(frame.error.message, { details: frame.error.details });
  }
  return handshakeFailed(frame.error.code + ": " + frame.error.message, "client");
}

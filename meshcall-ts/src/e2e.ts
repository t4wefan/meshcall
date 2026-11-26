// End-to-end sealed tunnels (docs/protocol.md → End-to-end encryption).
//
// A Noise handshake per client connection (NK, or IK / IKpsk2 when the client
// authenticates itself), then the payloads, metadata and error messages of
// every call travel in a `sealed` field while the Router routes on the clear
// header. WebCrypto only (no Node imports), so the Node runtime and
// `meshcall/web` share it.

import { MeshCallError, UnauthenticatedError } from "./errors.js";
import { isReservedKey } from "./policy.js";
import type { CallErrorFrame, CallOpenFrame, ErrorPayload, Frame, Metadata } from "./protocol.js";
import type { Tunnel } from "./tunnel.js";

export const E2E_PROLOGUE = "meshcall-e2e/1";
/** A pairing id is 1 to this many bytes (UTF-8). */
export const E2E_MAX_PAIRING_ID = 128;
/** The message-1 handshake call's timeout. */
export const E2E_HANDSHAKE_TIMEOUT_MS = 10_000;
/** A pairing confirmation may wait for a person at the instance. */
export const E2E_CONFIRM_TIMEOUT_MS = 120_000;
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

/** The IK / IKpsk2 prologue: the tunnel, and the instance whose key the client pinned. */
export function peerPrologue(digest: string, instanceId: string): Uint8Array {
  return text.encode(E2E_PROLOGUE + "|" + digest + "|" + instanceId);
}

/** The associated data of a pairing confirmation. */
export function confirmAd(sessionId: string): Uint8Array {
  return text.encode(E2E_PROLOGUE + "|confirm|" + sessionId);
}

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

/** A new pairing secret: 32 random bytes, base64url without padding. */
export function generateE2EPsk(): string {
  const value = globalThis.crypto?.getRandomValues?.(new Uint8Array(32));
  if (value === undefined) throw new Error("End-to-end pairing needs WebCrypto (crypto.getRandomValues)");
  return b64encode(value);
}

/**
 * A one-time pairing the instance's owner handed the client out of band
 * (never through the Router): its id and 32-byte PSK, base64url.
 */
export interface E2EPairing {
  readonly id: string;
  readonly psk: string;
}

/**
 * A client's own key for end-to-end tunnels: with it the client
 * authenticates itself to the instance (Noise IK). `privateKey` is this
 * client's static key (`generateE2EKey().privateKey`, kept by the app);
 * `instanceId` the instance whose key the client pinned in `e2eKey`, bound in
 * the handshake. `pairing` makes the first handshake a pairing (IKpsk2); once
 * the instance confirmed it, later handshakes use IK.
 */
export interface E2EIdentity {
  readonly privateKey: string;
  readonly instanceId: string;
  readonly pairing?: E2EPairing;
}

/**
 * The end-to-end session of a call (`ctx.e2e`), or the client an instance is
 * asked to accept (`authorizeE2EPeer`). `peerKey` is the client's static
 * public key (base64url), `null` for an NK session, whose client has none;
 * `pairingId` the pairing the session was made with.
 */
export interface E2EPeer {
  readonly protocol: string;
  readonly session: string;
  readonly peerKey: string | null;
  readonly pairingId: string | null;
  readonly tunnel: Tunnel;
}

export function checkPairingId(value: string): string {
  const size = typeof value === "string" ? text.encode(value).length : 0;
  if (size < 1 || size > E2E_MAX_PAIRING_ID) throw new TypeError("A pairing id is 1 to " + E2E_MAX_PAIRING_ID + " bytes");
  return value;
}

/** Check a client identity before anything is sent; returns it unchanged. */
export function checkE2EIdentity(identity: E2EIdentity): E2EIdentity {
  if (identity === null || typeof identity !== "object") throw new TypeError("e2eIdentity is { privateKey, instanceId, pairing? }");
  keyBytes(identity.privateKey, "An E2E private key");
  if (typeof identity.instanceId !== "string" || identity.instanceId === "") {
    throw new TypeError("E2EIdentity needs the instanceId whose key you pinned");
  }
  if (identity.pairing !== undefined) {
    checkPairingId(identity.pairing.id);
    keyBytes(identity.pairing.psk, "A pairing PSK");
  }
  return identity;
}

/** The raw bytes of a pairing PSK (32 bytes, base64url). */
export function pskBytes(value: string): Uint8Array<ArrayBuffer> {
  return keyBytes(value, "A pairing PSK");
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

/** Noise's HKDF: `n` outputs of HMAC-SHA256 chained from the chaining key. */
async function hkdf(ck: Uint8Array, ikm: Uint8Array, n: number): Promise<Uint8Array<ArrayBuffer>[]> {
  const temp = await hmac(ck, ikm);
  const out: Uint8Array<ArrayBuffer>[] = [];
  let previous = new Uint8Array();
  for (let i = 1; i <= n; i += 1) {
    previous = await hmac(temp, concat(previous, Uint8Array.of(i)));
    out.push(previous);
  }
  return out;
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

// --- Noise NK, IK, IKpsk2 ------------------------------------------------------

export const NK = "Noise_NK_25519_AESGCM_SHA256";
export const IK = "Noise_IK_25519_AESGCM_SHA256";
export const IKPSK2 = "Noise_IKpsk2_25519_AESGCM_SHA256";
export type E2EProtocol = typeof NK | typeof IK | typeof IKPSK2;

type Token = "e" | "s" | "es" | "se" | "ee" | "ss" | "psk";
/** Message patterns after the responder's static key (`<- s`), which all three know beforehand. */
const PATTERNS: Readonly<Record<E2EProtocol, readonly [ReadonlyArray<Token>, ReadonlyArray<Token>]>> = {
  [NK]: [["e", "es"], ["e", "ee"]],
  [IK]: [["e", "es", "s", "ss"], ["e", "ee", "se"]],
  [IKPSK2]: [["e", "es", "s", "ss"], ["e", "ee", "se", "psk"]],
};

export function isE2EProtocol(value: unknown): value is E2EProtocol {
  return value === NK || value === IK || value === IKPSK2;
}

class SymmetricState {
  private k: CryptoKey | undefined;
  private n = 0n;

  private constructor(public h: Uint8Array, private ck: Uint8Array) {}

  public static async create(protocol: string, prologue: Uint8Array): Promise<SymmetricState> {
    const name = new Uint8Array(32); // every name here is shorter than HASHLEN: zero-padded
    name.set(text.encode(protocol));
    const state = new SymmetricState(name, name);
    await state.mixHash(prologue);
    return state;
  }

  public get hasKey(): boolean { return this.k !== undefined; }

  public async mixHash(data: Uint8Array): Promise<void> {
    this.h = await sha256(concat(this.h, data));
  }

  public async mixKey(ikm: Uint8Array): Promise<void> {
    const [ck, k] = await hkdf(this.ck, ikm, 2) as [Uint8Array, Uint8Array];
    this.ck = ck;
    this.k = await aesKey(k);
    this.n = 0n;
  }

  public async mixKeyAndHash(ikm: Uint8Array): Promise<void> {
    const [ck, tempH, k] = await hkdf(this.ck, ikm, 3) as [Uint8Array, Uint8Array, Uint8Array];
    this.ck = ck;
    await this.mixHash(tempH);
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

  public async split(): Promise<[Uint8Array, Uint8Array]> {
    return await hkdf(this.ck, new Uint8Array(), 2) as [Uint8Array, Uint8Array];
  }
}

async function ephemeralKey(raw: Uint8Array | undefined): Promise<CryptoKey> {
  if (raw !== undefined) return importPrivate(raw);
  const pair = await subtle().generateKey({ name: "X25519" }, true, ["deriveBits"]) as CryptoKeyPair;
  return pair.privateKey;
}

export interface NoiseOptions {
  readonly initiator: boolean;
  readonly prologue: Uint8Array;
  /** This end's static private key (raw 32 bytes): the responder's; the initiator's for IK and IKpsk2. */
  readonly s?: Uint8Array;
  /** The responder's static public key (raw), on the initiator. */
  readonly rs?: Uint8Array;
  /** The pairing secret, IKpsk2 only. */
  readonly psk?: Uint8Array;
  /** A fixed ephemeral private key (raw 32 bytes): test vectors only. */
  readonly ephemeral?: Uint8Array;
}

/**
 * One end of a two-message Noise handshake towards (or as) a responder whose
 * static key the initiator knows: NK, IK or IKpsk2.
 */
export class NoiseHandshake {
  private turn = 0;
  private e: CryptoKey | undefined;
  private re: CryptoKey | undefined;
  private rs: CryptoKey | undefined;
  /** The other end's static public key, raw (the client's, on the instance). */
  public remoteStatic: Uint8Array | undefined;

  private constructor(
    public readonly protocol: E2EProtocol,
    private readonly sym: SymmetricState,
    private readonly initiator: boolean,
    private readonly s: CryptoKey | undefined,
    private readonly sPublic: Uint8Array | undefined,
    private readonly psk: Uint8Array | undefined,
    private readonly ephemeral: Uint8Array | undefined,
  ) {}

  public static async create(protocol: string, options: NoiseOptions): Promise<NoiseHandshake> {
    if (!isE2EProtocol(protocol)) throw new RangeError("unknown handshake " + protocol);
    if ((protocol === IKPSK2) !== (options.psk !== undefined)) throw new RangeError("IKpsk2, and only IKpsk2, takes a PSK");
    if (options.psk !== undefined && options.psk.length !== 32) throw new RangeError("a pairing PSK is 32 bytes");
    if (protocol !== NK && options.s === undefined) throw new RangeError(protocol + " needs this end's static key");
    if (options.initiator ? options.rs === undefined : options.s === undefined) {
      throw new RangeError("the responder's static key is needed");
    }
    const sym = await SymmetricState.create(protocol, options.prologue);
    const s = options.s === undefined ? undefined : await importPrivate(options.s);
    const sPublic = s === undefined ? undefined : await publicBytes(s);
    const handshake = new NoiseHandshake(
      protocol, sym, options.initiator, s, sPublic,
      options.psk === undefined ? undefined : buf(options.psk), options.ephemeral,
    );
    if (options.initiator) {
      handshake.remoteStatic = buf(options.rs as Uint8Array);
      handshake.rs = await importPublic(handshake.remoteStatic);
    }
    await sym.mixHash(options.initiator ? options.rs as Uint8Array : sPublic as Uint8Array);
    return handshake;
  }

  private tokens(): ReadonlyArray<Token> {
    const tokens = PATTERNS[this.protocol][this.turn];
    if (tokens === undefined) throw new RangeError("the handshake is complete");
    this.turn += 1;
    return tokens;
  }

  public async writeMessage(payload: Uint8Array = new Uint8Array()): Promise<Uint8Array> {
    const parts: Uint8Array[] = [];
    for (const token of this.tokens()) {
      if (token === "e") {
        this.e = await ephemeralKey(this.ephemeral);
        const pub = await publicBytes(this.e);
        parts.push(pub);
        await this.sym.mixHash(pub);
        if (this.psk !== undefined) await this.sym.mixKey(pub);
      } else if (token === "s") {
        parts.push(await this.sym.encryptAndHash(this.sPublic as Uint8Array));
      } else if (token === "psk") {
        await this.sym.mixKeyAndHash(this.psk as Uint8Array);
      } else {
        await this.sym.mixKey(await this.tokenDh(token));
      }
    }
    parts.push(await this.sym.encryptAndHash(payload));
    return concat(...parts);
  }

  public async readMessage(message: Uint8Array): Promise<Uint8Array> {
    let rest = message;
    for (const token of this.tokens()) {
      if (token === "e") {
        if (rest.length < 32) throw new RangeError("short handshake message");
        const raw = buf(rest.subarray(0, 32));
        this.re = await importPublic(raw);
        await this.sym.mixHash(raw);
        if (this.psk !== undefined) await this.sym.mixKey(raw);
        rest = rest.subarray(32);
      } else if (token === "s") {
        const size = this.sym.hasKey ? 48 : 32;
        if (rest.length < size) throw new RangeError("short handshake message");
        this.remoteStatic = buf(await this.sym.decryptAndHash(rest.subarray(0, size)));
        this.rs = await importPublic(this.remoteStatic);
        rest = rest.subarray(size);
      } else if (token === "psk") {
        await this.sym.mixKeyAndHash(this.psk as Uint8Array);
      } else {
        await this.sym.mixKey(await this.tokenDh(token));
      }
    }
    if (this.sym.hasKey && rest.length < 16) throw new RangeError("short handshake message");
    return this.sym.decryptAndHash(rest);
  }

  private tokenDh(token: "es" | "se" | "ee" | "ss"): Promise<Uint8Array<ArrayBuffer>> {
    // The first letter is the initiator's key, the second the responder's.
    const [first, second] = [token[0], token[1]];
    const [mine, theirs] = this.initiator ? [first, second] : [second, first];
    const local = mine === "e" ? this.e : this.s;
    const remote = theirs === "e" ? this.re : this.rs;
    if (local === undefined || remote === undefined) throw new RangeError("handshake key missing");
    return dh(local, remote);
  }

  /** The session once both messages are done. */
  public async split(): Promise<E2ESession> {
    const [toResponder, toInitiator] = await this.sym.split();
    return this.initiator
      ? E2ESession.create(this.sym.h, toResponder, toInitiator, this.protocol)
      : E2ESession.create(this.sym.h, toInitiator, toResponder, this.protocol);
  }
}

export interface NKOptions {
  readonly prologue?: Uint8Array;
  /** A fixed ephemeral private key (raw 32 bytes): test vectors only. */
  readonly ephemeral?: Uint8Array;
  readonly payload?: Uint8Array;
}

/** The client side of an NK handshake: message 1 towards the pinned key, then `finish(message2)`. */
export async function nkInitiate(remoteStatic: Uint8Array, options: NKOptions = {}): Promise<{
  readonly message: Uint8Array;
  finish(message2: Uint8Array): Promise<{ readonly payload: Uint8Array; readonly session: E2ESession }>;
}> {
  const handshake = await NoiseHandshake.create(NK, {
    initiator: true, prologue: options.prologue ?? text.encode(E2E_PROLOGUE), rs: remoteStatic,
    ...(options.ephemeral === undefined ? {} : { ephemeral: options.ephemeral }),
  });
  const message = await handshake.writeMessage(options.payload);
  return {
    message,
    async finish(message2) {
      const payload = await handshake.readMessage(message2);
      return { payload, session: await handshake.split() };
    },
  };
}

/** The instance side of an NK handshake: read message 1, write message 2. */
export async function nkRespond(staticPrivate: Uint8Array, message: Uint8Array, options: NKOptions = {}): Promise<{
  readonly reply: Uint8Array; readonly received: Uint8Array; readonly session: E2ESession;
}> {
  const handshake = await NoiseHandshake.create(NK, {
    initiator: false, prologue: options.prologue ?? text.encode(E2E_PROLOGUE), s: staticPrivate,
    ...(options.ephemeral === undefined ? {} : { ephemeral: options.ephemeral }),
  });
  const received = await handshake.readMessage(message);
  const reply = await handshake.writeMessage(options.payload);
  return { reply, received, session: await handshake.split() };
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
  /** The client's static public key (base64url) on the instance, for IK and IKpsk2 sessions. */
  public peerKey: string | null = null;
  /** The pairing an IKpsk2 session was made with. */
  public pairingId: string | null = null;
  private sendN = 0n;
  private top = 0n; // highest counter received + 1
  private window = 0n; // bit i: counter top-1-i seen

  private constructor(
    public readonly hash: Uint8Array,
    /** Exposed for the test vector. */
    public readonly sendKey: CryptoKey,
    private readonly recvKey: CryptoKey,
    public readonly protocol: E2EProtocol,
  ) {
    this.id = hex(hash.subarray(0, 16));
  }

  public static async create(hash: Uint8Array, send: Uint8Array, recv: Uint8Array, protocol: E2EProtocol = NK): Promise<E2ESession> {
    return new E2ESession(hash, await aesKey(send), await aesKey(recv), protocol);
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

/**
 * An instance's sessions under each tunnel, at most 16 each (oldest dropped),
 * and as many pairing sessions waiting for their confirmation.
 */
export class E2ESessionStore {
  private readonly byTunnel = new Map<string, Map<string, E2ESession>>();
  private readonly pending = new Map<string, Map<string, E2ESession>>();

  private static put(table: Map<string, Map<string, E2ESession>>, tunnelId: string, session: E2ESession): void {
    let sessions = table.get(tunnelId);
    if (sessions === undefined) table.set(tunnelId, sessions = new Map());
    sessions.set(session.id, session);
    while (sessions.size > E2E_MAX_SESSIONS) sessions.delete(sessions.keys().next().value as string);
  }

  public add(tunnelId: string, session: E2ESession): void {
    E2ESessionStore.put(this.byTunnel, tunnelId, session);
  }

  public addPending(tunnelId: string, session: E2ESession): void {
    E2ESessionStore.put(this.pending, tunnelId, session);
  }

  /** Remove and return a pairing session: each is confirmed at most once. */
  public takePending(tunnelId: string, sessionId: string): E2ESession | undefined {
    const sessions = this.pending.get(tunnelId);
    const session = sessions?.get(sessionId);
    if (session !== undefined) sessions?.delete(sessionId);
    return session;
  }

  public find(tunnelId: string, sessionId: string): E2ESession | undefined {
    return this.byTunnel.get(tunnelId)?.get(sessionId);
  }

  public drop(tunnelId: string): void {
    this.byTunnel.delete(tunnelId);
    this.pending.delete(tunnelId);
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
 * A client's end-to-end state across its connections: the pinned instance
 * key, its own identity, and whether its pairing was confirmed (later
 * handshakes then use IK). It never falls back to a weaker handshake.
 */
export class E2EClient {
  private paired = false;

  public constructor(
    private readonly pinnedKey: string,
    private credential: string,
    private readonly identity?: E2EIdentity,
  ) {}

  /** Bind the next handshakes to a new tunnel credential; a confirmed pairing stays confirmed. */
  public useCredential(credential: string): void {
    this.credential = credential;
  }

  /** Start the handshake of a fresh connection; `ephemeral` is for test vectors only. */
  public async start(callId: string, ephemeral?: Uint8Array): Promise<ClientHandshake> {
    const identity = this.identity;
    const pairing = identity !== undefined && !this.paired ? identity.pairing : undefined;
    const protocol: E2EProtocol = identity === undefined ? NK : pairing !== undefined ? IKPSK2 : IK;
    // The prologue binds the session to this credential's tunnel (and, for
    // IK, to the instance whose key the client pinned).
    const digest = await credentialDigest(this.credential);
    const handshake = await NoiseHandshake.create(protocol, {
      initiator: true,
      prologue: identity === undefined ? tunnelPrologue(digest) : peerPrologue(digest, identity.instanceId),
      rs: keyBytes(this.pinnedKey, "An E2E key (the instance's X25519 public key)"),
      ...(identity === undefined ? {} : { s: keyBytes(identity.privateKey, "An E2E private key") }),
      ...(pairing === undefined ? {} : { psk: pskBytes(pairing.psk) }),
      ...(ephemeral === undefined ? {} : { ephemeral }),
    });
    const payload: Record<string, string> = { message: b64encode(await handshake.writeMessage()) };
    if (protocol !== NK) payload.protocol = protocol;
    if (pairing !== undefined) payload.pairing_id = pairing.id;
    return new ClientHandshake(handshake, tunnelHostCall(callId, payload, E2E_HANDSHAKE_TIMEOUT_MS), pairing !== undefined, () => {
      this.paired = true;
    });
  }
}

function tunnelHostCall(callId: string, payload: Record<string, string>, timeoutMs: number): CallOpenFrame {
  return {
    kind: "call.open", call_id: callId, service: "meshcall.router.v1.TunnelHost",
    method: TUNNEL_HANDSHAKE_METHOD, payload, timeout_ms: timeoutMs,
  };
}

/**
 * One connection's handshake: send `frame`, then give its answer to
 * `receive`, which resolves with the session, or with `undefined` once
 * `frame` is the next call to send (a pairing's confirmation, whose answer
 * may take up to `timeoutMs`).
 */
export class ClientHandshake {
  private session: E2ESession | undefined;

  public constructor(
    private readonly handshake: NoiseHandshake,
    public frame: CallOpenFrame,
    private readonly pairing: boolean,
    private readonly confirmed: () => void,
  ) {}

  public get callId(): string { return this.frame.call_id; }

  public get timeoutMs(): number { return this.frame.timeout_ms ?? E2E_HANDSHAKE_TIMEOUT_MS; }

  public async receive(reply: Frame, nextCallId: string): Promise<E2ESession | undefined> {
    if (reply.kind === "call.error") throw handshakeError(reply);
    if (reply.kind !== "call.result") throw handshakeFailed("unexpected " + reply.kind, "client");
    if (this.session !== undefined) {
      // The instance accepted the pairing: from now on this client is known.
      this.confirmed();
      return this.session;
    }
    const message = (reply.payload as { message?: unknown } | null | undefined)?.message;
    let session: E2ESession;
    try {
      await this.handshake.readMessage(b64decode(typeof message === "string" ? message : ""));
      session = await this.handshake.split();
    } catch {
      // Only the holder of the pinned private key (and, pairing, of the PSK) can write message 2.
      throw handshakeFailed("message 2 is not from the pinned instance key", "client");
    }
    if (!this.pairing) return session;
    // Prove the PSK; the instance accepts this client only then.
    this.session = session;
    const confirm = await session.seal(text.encode("{}"), confirmAd(session.id));
    this.frame = tunnelHostCall(nextCallId, { session: session.id, confirm }, E2E_CONFIRM_TIMEOUT_MS);
    return undefined;
  }
}

/** A `TunnelHost/handshake` request, checked. */
export interface HandshakeRequest {
  readonly protocol: E2EProtocol;
  readonly message: Uint8Array;
  /** IKpsk2: which pairing's PSK the client used. */
  readonly pairingId: string | null;
}

/** Read a handshake request; throws for anything malformed or unknown. */
export function parseHandshakeRequest(payload: unknown): HandshakeRequest {
  const body = (payload !== null && typeof payload === "object" ? payload : {}) as {
    message?: unknown; protocol?: unknown; pairing_id?: unknown;
  };
  const protocol = body.protocol === undefined || body.protocol === null || body.protocol === "" ? NK : body.protocol;
  if (!isE2EProtocol(protocol)) throw new RangeError("unknown handshake");
  const pairingId = protocol === IKPSK2 ? checkPairingId(typeof body.pairing_id === "string" ? body.pairing_id : "") : null;
  return { protocol, message: b64decode(typeof body.message === "string" ? body.message : ""), pairingId };
}

/**
 * The instance's half of a handshake made through the tunnel whose
 * credential digest is `digest`: read message 1, write message 2. Throws when
 * message 1 does not open. `ephemeral` is for test vectors only.
 */
export async function respondHandshake(request: HandshakeRequest, options: {
  readonly privateKey: Uint8Array;
  readonly digest: string;
  readonly instanceId: string;
  readonly psk?: Uint8Array;
  readonly ephemeral?: Uint8Array;
}): Promise<{ readonly reply: Uint8Array; readonly session: E2ESession }> {
  const handshake = await NoiseHandshake.create(request.protocol, {
    initiator: false,
    // The prologue binds the tunnel the handshake came through (and, for IK,
    // this instance): one made for another does not open here.
    prologue: request.protocol === NK ? tunnelPrologue(options.digest) : peerPrologue(options.digest, options.instanceId),
    s: options.privateKey,
    ...(options.psk === undefined ? {} : { psk: options.psk }),
    ...(options.ephemeral === undefined ? {} : { ephemeral: options.ephemeral }),
  });
  await handshake.readMessage(request.message);
  const reply = await handshake.writeMessage();
  const session = await handshake.split();
  if (request.protocol !== NK && handshake.remoteStatic !== undefined) session.peerKey = b64encode(handshake.remoteStatic);
  session.pairingId = request.pairingId;
  return { reply, session };
}

function handshakeError(frame: CallErrorFrame): MeshCallError {
  if (frame.error.details?.reason === "e2e_handshake_failed") {
    // The instance's own answer: it refused the handshake.
    return new UnauthenticatedError(frame.error.message, { details: frame.error.details });
  }
  return handshakeFailed(frame.error.code + ": " + frame.error.message, "client");
}

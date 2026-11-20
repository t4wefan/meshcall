// Tunnels: instance-scoped connections through the Router (docs/protocol.md →
// Tunnels). No Node imports: `meshcall/web` shares the issuer client.

import type { CallOptions } from "./client.js";
import {
  b64decode, b64encode, checkE2EPrivateKey, confirmAd, e2ePublicKey, IK, IKPSK2, parseHandshakeRequest, pskBytes,
  respondHandshake, sameKey, TUNNEL_HANDSHAKE_METHOD, E2ESessionStore, handshakeFailed,
  type E2EPeer, type E2ESession, type HandshakeRequest,
} from "./e2e.js";
import type { Metadata } from "./protocol.js";
import type { MeshCallRpc } from "./rpc.js";

export const TUNNEL_SERVICE = "meshcall.router.v1.TunnelService";
export const TUNNEL_HOST_SERVICE = "meshcall.router.v1.TunnelHost";
export const TUNNEL_CAPABILITY = "tunnels";
export const TUNNEL_CREDENTIAL_PREFIX = "mtun_";
export const TUNNEL_ID_KEY = "meshcall-tunnel-id";
export const TUNNEL_PRINCIPAL_KEY = "meshcall-tunnel-principal";

/** One service of the target instance and the methods a tunnel allows (`"*"`: all). */
export interface TunnelScope {
  readonly service: string;
  readonly methods: ReadonlyArray<string>;
}

/** A tunnel as the Router lists it (never with its credential). */
export interface TunnelInfo {
  readonly tunnel_id: string;
  readonly instance_id: string;
  readonly principal: string;
  readonly scopes: ReadonlyArray<TunnelScope>;
  readonly expires_at_unix_ms: number;
  readonly max_connections?: number;
  /** The instance public key the issuer pinned: the tunnel carries sealed calls only. */
  readonly e2e_key?: string;
  readonly connections: number;
  readonly online: boolean;
}

/** The result of `mesh.tunnels.open`: hand `credential` to the client once. */
export interface OpenedTunnel extends TunnelInfo {
  readonly credential: string;
}

/** What an instance is asked to accept before a tunnel becomes usable. */
export interface TunnelOffer {
  readonly tunnel_id: string;
  readonly instance_id: string;
  readonly principal: string;
  readonly scopes: ReadonlyArray<TunnelScope>;
  readonly expires_at_unix_ms: number;
  /** Offered again after this instance reconnected. */
  readonly resumed: boolean;
  /**
   * The public key the issuer pinned for end-to-end sealing; absent for a
   * plain tunnel. An offer pinning a key this instance does not hold is
   * refused before any hook sees it.
   */
  readonly e2e_key?: string;
}

/** The tunnel a call arrived through, as the instance accepted it (`ctx.tunnel`). */
export interface Tunnel {
  readonly id: string;
  readonly principal: string;
  readonly scopes: ReadonlyArray<TunnelScope>;
  /** The tunnel carries end-to-end sealed calls only. */
  readonly sealed: boolean;
}

/** A tunnel this instance accepted has ended. */
export interface TunnelClosed {
  readonly tunnelId: string;
  readonly principal: string;
  readonly reason: string;
}

/** Decides an offer: return `true` to accept. */
export type TunnelOfferHook = (offer: TunnelOffer) => boolean | Promise<boolean>;
/** Told when an accepted tunnel ends (revoked, expired, ...). */
export type TunnelClosedHook = (closed: TunnelClosed) => void | Promise<void>;
/**
 * Decides whether an instance accepts a client's static key (`peer.peerKey`)
 * on a tunnel; `peer.pairingId` is set when the client is pairing and has
 * proven it holds that pairing's PSK. A hook that throws refuses.
 */
export type E2EPeerHook = (peer: E2EPeer) => boolean | Promise<boolean>;
/**
 * Returns the PSK (base64url) of an unused pairing id, or `undefined`. It
 * must not spend the pairing: that happens when the client confirms it, in
 * the `E2EPeerHook`.
 */
export type E2EPairingPskHook = (pairingId: string) => string | undefined | null | Promise<string | undefined | null>;

/** How the services of one `mount` call answer tunnel offers. */
export interface TunnelPolicy {
  readonly accept: boolean | TunnelOfferHook;
  readonly onClosed?: TunnelClosedHook;
  /** Refuse offers without `e2e_key` that scope these services. */
  readonly requireE2E?: boolean;
  /** Serve these services only in sessions whose client was authenticated (implies `requireE2E`). */
  readonly requireE2EPeer?: boolean;
}

/** How an instance answers end-to-end handshakes. */
export interface TunnelHostOptions {
  readonly e2ePrivateKey?: string;
  readonly authorizeE2EPeer?: E2EPeerHook;
  readonly e2ePairingPsk?: E2EPairingPskHook;
  /** Bound in IK / IKpsk2 handshakes. */
  readonly instanceId?: string;
}

export interface OpenTunnelRequest {
  readonly instance: string;
  /** Services of the instance; a bare name allows every method. */
  readonly scopes: ReadonlyArray<string | { readonly service: string; readonly methods?: ReadonlyArray<string> }>;
  readonly principal: string;
  /** Lifetime for new connections, 1–86400 s (default 3600). */
  readonly ttlSeconds?: number;
  readonly maxConnections?: number;
  /**
   * Pin the instance's public key (the one you provisioned it with, not one
   * learnt from the Router): every call on the tunnel is then sealed end to end.
   */
  readonly e2eKey?: string;
}

export function checkTunnelCredential(credential: string): string {
  if (!credential.startsWith(TUNNEL_CREDENTIAL_PREFIX) || /[\s]/u.test(credential)) {
    throw new TypeError("A tunnel credential starts with 'mtun_'; it is the `credential` returned by mesh.tunnels.open");
  }
  return credential;
}

/**
 * The Router normalises a service name without a version segment to `.v0`.
 * Browser-safe; contract.ts (Node only) validates names fully.
 */
export function normalizeScopeService(service: string): string {
  if (service === "*" || service === "" || /\.v\d/u.test(service)) return service;
  return `${service}.v0`;
}

/** Issuer API for the Router's `meshcall.router.v1.TunnelService` (`mesh.tunnels`). */
export class TunnelsClient {
  public constructor(private readonly rpc: MeshCallRpc) {}

  /**
   * Open a tunnel to one instance; resolves once the instance accepted it.
   * Rejects with `PermissionDeniedError` (`reason: "tunnel_rejected"`) when the
   * instance refuses and `UnavailableError` (`instance_unknown`) when it is not
   * registered.
   */
  public open(request: OpenTunnelRequest, options?: CallOptions): Promise<OpenedTunnel> {
    // The Router checks the request (`invalid_argument`), so every runtime fails alike.
    const ttl = request.ttlSeconds ?? 3600;
    return this.rpc.unary(TUNNEL_SERVICE, "open", {
      instance_id: request.instance,
      scopes: request.scopes.map((scope) => typeof scope === "string"
        ? { service: normalizeScopeService(scope), methods: ["*"] }
        : { service: normalizeScopeService(scope.service), methods: [...(scope.methods ?? ["*"])] }),
      principal: request.principal,
      ttl_seconds: ttl,
      ...(request.maxConnections === undefined ? {} : { max_connections: request.maxConnections }),
      // The Router checks it (invalid_argument), as in every runtime.
      ...(request.e2eKey === undefined ? {} : { e2e_key: request.e2eKey }),
    }, options);
  }

  /** Close a tunnel and every connection on it; `false` if it is not yours or gone. */
  public async revoke(tunnelId: string, options?: CallOptions): Promise<boolean> {
    const result = await this.rpc.unary<{ tunnel_id: string }, { revoked: boolean }>(
      TUNNEL_SERVICE, "revoke", { tunnel_id: tunnelId }, { idempotent: true, ...options },
    );
    return result.revoked;
  }

  /** The tunnels this identity opened that are still open. */
  public async list(options?: CallOptions): Promise<TunnelInfo[]> {
    const result = await this.rpc.unary<Record<string, never>, { tunnels: TunnelInfo[] }>(
      TUNNEL_SERVICE, "list", {}, { idempotent: true, ...options },
    );
    return result.tunnels ?? [];
  }
}

/** Server side: answers the Router's TunnelHost calls and keeps accepted tunnels. */
export class TunnelHost {
  private readonly tunnels = new Map<string, Tunnel>();
  /** Tunnel id → credential digest, which the handshake prologue binds. */
  private readonly digests = new Map<string, string>();
  private readonly privateKey: Uint8Array | undefined;
  private publicKey: Promise<string> | undefined;
  /** Sessions this instance handshook, per tunnel. */
  public readonly sessions = new E2ESessionStore();

  public constructor(private readonly policies: ReadonlyMap<string, TunnelPolicy>, private readonly options: TunnelHostOptions = {}) {
    const e2ePrivateKey = options.e2ePrivateKey;
    if (e2ePrivateKey !== undefined) this.privateKey = b64decode(checkE2EPrivateKey(e2ePrivateKey));
    if (e2ePrivateKey !== undefined) {
      this.publicKey = e2ePublicKey(e2ePrivateKey);
      this.publicKey.catch(() => undefined); // reported when an offer awaits it
    }
  }

  /** This instance answers end-to-end handshakes, so sealed calls must be opened. */
  public get e2e(): boolean { return this.privateKey !== undefined; }

  public get enabled(): boolean { return this.policies.size > 0; }

  /**
   * The accepted tunnel a call names, or `null`. The instance's own table is
   * authoritative: an id it never accepted (a forged key) is no tunnel.
   */
  public lookup(metadata: Readonly<Metadata> | null | undefined): Tunnel | null {
    const id = metadata?.[TUNNEL_ID_KEY];
    if (id === undefined) return null;
    const tunnel = this.tunnels.get(id);
    return tunnel !== undefined && metadata?.[TUNNEL_PRINCIPAL_KEY] === tunnel.principal ? tunnel : null;
  }

  public async handle(method: string, payload: unknown, metadata?: Readonly<Metadata> | null): Promise<unknown> {
    if (method === "offer") {
      const offer = payload as TunnelOffer;
      if (typeof offer?.tunnel_id !== "string" || typeof offer.principal !== "string" || !Array.isArray(offer.scopes)) {
        throw new TypeError("Invalid tunnel offer");
      }
      const pinned = typeof offer.e2e_key === "string" && offer.e2e_key !== "" ? offer.e2e_key : undefined;
      const digest = (payload as { credential_digest?: unknown }).credential_digest;
      if (pinned !== undefined) {
        // An E2E tunnel pins a key: only the instance holding it may accept.
        if (this.publicKey === undefined || !sameKey(pinned, await this.publicKey) || typeof digest !== "string" || digest === "") {
          return { accept: false, reason: "e2e_key_mismatch" };
        }
      } else if (offer.scopes.some((scope) => this.requiresE2E(scope.service))) {
        return { accept: false, reason: "e2e_required" };
      }
      const accept = await this.decide(offer);
      if (accept) {
        this.tunnels.set(offer.tunnel_id, {
          id: offer.tunnel_id, principal: offer.principal, scopes: offer.scopes, sealed: pinned !== undefined,
        });
        if (pinned !== undefined) this.digests.set(offer.tunnel_id, digest as string);
      }
      return { accept };
    }
    if (method === TUNNEL_HANDSHAKE_METHOD) {
      if (payload !== null && typeof payload === "object" && "confirm" in payload) return this.confirm(metadata, payload);
      return this.handshake(metadata, payload);
    }
    if (method === "closed") {
      const { tunnel_id: tunnelId, reason } = (payload ?? {}) as { tunnel_id?: string; reason?: string };
      if (tunnelId !== undefined) {
        this.sessions.drop(tunnelId);
        this.digests.delete(tunnelId);
      }
      const tunnel = tunnelId === undefined ? undefined : this.tunnels.get(tunnelId);
      if (tunnel !== undefined) {
        this.tunnels.delete(tunnel.id);
        const called = new Set<TunnelClosedHook>();
        for (const scope of tunnel.scopes) {
          const hook = this.policies.get(scope.service)?.onClosed;
          if (hook !== undefined && !called.has(hook)) {
            called.add(hook);
            await hook({ tunnelId: tunnel.id, principal: tunnel.principal, reason: reason ?? "" });
          }
        }
      }
      return {};
    }
    throw new TypeError("Unknown " + TUNNEL_HOST_SERVICE + " method " + method);
  }

  private sealedTunnel(metadata: Readonly<Metadata> | null | undefined): { tunnel: Tunnel; digest: string; privateKey: Uint8Array } {
    const tunnel = this.lookup(metadata);
    const digest = tunnel === null ? undefined : this.digests.get(tunnel.id);
    if (tunnel === null || !tunnel.sealed || this.privateKey === undefined || digest === undefined) {
      throw handshakeFailed("not an end-to-end tunnel of this instance", "server");
    }
    return { tunnel, digest, privateKey: this.privateKey };
  }

  /**
   * Answer a client's message 1 on an E2E tunnel this instance accepted; the
   * session is kept under that tunnel (a pairing's waits for its confirmation).
   */
  private async handshake(metadata: Readonly<Metadata> | null | undefined, payload: unknown): Promise<unknown> {
    const { tunnel, digest, privateKey } = this.sealedTunnel(metadata);
    // Every refusal reads the same: a client cannot probe what failed.
    const failed = () => handshakeFailed("message 1 does not open with this instance's key on this tunnel", "server");
    let request: HandshakeRequest;
    try {
      request = parseHandshakeRequest(payload);
    } catch {
      throw failed();
    }
    let psk: Uint8Array | undefined;
    if (request.protocol === IKPSK2) {
      const lookup = this.options.e2ePairingPsk;
      if (lookup === undefined || request.pairingId === null) throw failed();
      let found: unknown;
      try {
        found = await lookup(request.pairingId);
      } catch {
        found = undefined; // a failing hook refuses
      }
      if (typeof found !== "string") throw failed();
      try {
        psk = pskBytes(found);
      } catch {
        throw failed();
      }
    }
    let answer: Awaited<ReturnType<typeof respondHandshake>>;
    try {
      answer = await respondHandshake(request, {
        privateKey, digest, instanceId: this.options.instanceId ?? "", ...(psk === undefined ? {} : { psk }),
      });
    } catch {
      throw failed();
    }
    if (request.protocol === IKPSK2) {
      // Message 1 does not prove the client knows the PSK: wait for its
      // confirmation before anyone trusts this key.
      this.sessions.addPending(tunnel.id, answer.session);
    } else {
      if (request.protocol === IK && !await this.authorized(tunnel, answer.session)) throw failed();
      this.sessions.add(tunnel.id, answer.session);
    }
    return { message: b64encode(answer.reply) };
  }

  /** A pairing client proves it holds the PSK; only then is the application asked to accept it. */
  private async confirm(metadata: Readonly<Metadata> | null | undefined, payload: object): Promise<unknown> {
    const { tunnel } = this.sealedTunnel(metadata);
    const failed = () => handshakeFailed("pairing not confirmed", "server");
    const { session: id, confirm } = payload as { session?: unknown; confirm?: unknown };
    const session = this.sessions.takePending(tunnel.id, typeof id === "string" ? id : "");
    if (session === undefined) throw failed();
    let opened: string;
    try {
      opened = new TextDecoder().decode(await session.open(typeof confirm === "string" ? confirm : "", confirmAd(session.id)));
    } catch {
      throw failed();
    }
    if (opened !== "{}" || !await this.authorized(tunnel, session)) throw failed();
    this.sessions.add(tunnel.id, session);
    return {};
  }

  private async authorized(tunnel: Tunnel, session: E2ESession): Promise<boolean> {
    const hook = this.options.authorizeE2EPeer;
    if (hook === undefined) return true;
    try {
      return await hook({
        protocol: session.protocol, session: session.id, peerKey: session.peerKey, pairingId: session.pairingId, tunnel,
      }) === true;
    } catch {
      return false; // a failing hook refuses
    }
  }

  /** Whether `service` is served only to authenticated end-to-end clients. */
  public requiresPeer(service: string): boolean {
    return this.policies.get(service)?.requireE2EPeer === true;
  }

  private requiresE2E(service: string): boolean {
    const policy = this.policies.get(service);
    return policy?.requireE2E === true || policy?.requireE2EPeer === true;
  }

  // Every scoped service must have opted in, and every hook must agree.
  private async decide(offer: TunnelOffer): Promise<boolean> {
    for (const scope of offer.scopes) {
      const policy = this.policies.get(scope.service);
      if (policy === undefined) return false;
      if (typeof policy.accept === "function" ? !await policy.accept(offer) : !policy.accept) return false;
    }
    return true;
  }
}

// Tunnels: instance-scoped connections through the Router (docs/protocol.md →
// Tunnels). No Node imports: `meshcall/web` shares the issuer client.

import type { CallOptions } from "./client.js";
import {
  b64decode, b64encode, checkE2EKey, checkE2EPrivateKey, e2ePublicKey, E2ESessionStore, handshakeFailed, nkRespond,
  sameKey, TUNNEL_HANDSHAKE_METHOD,
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

/** How the services of one `mount` call answer tunnel offers. */
export interface TunnelPolicy {
  readonly accept: boolean | TunnelOfferHook;
  readonly onClosed?: TunnelClosedHook;
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
      ...(request.e2eKey === undefined ? {} : { e2e_key: checkE2EKey(request.e2eKey) }),
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
  private readonly privateKey: Uint8Array | undefined;
  private publicKey: Promise<string> | undefined;
  /** Sessions this instance handshook, per tunnel. */
  public readonly sessions = new E2ESessionStore();

  public constructor(private readonly policies: ReadonlyMap<string, TunnelPolicy>, e2ePrivateKey?: string) {
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
      // An E2E tunnel pins a key: only the instance holding it may accept.
      const pinnedOk = pinned === undefined || (this.publicKey !== undefined && sameKey(pinned, await this.publicKey));
      const accept = pinnedOk && await this.decide(offer);
      if (accept) {
        this.tunnels.set(offer.tunnel_id, {
          id: offer.tunnel_id, principal: offer.principal, scopes: offer.scopes, sealed: pinned !== undefined,
        });
      }
      return { accept };
    }
    if (method === TUNNEL_HANDSHAKE_METHOD) return this.handshake(metadata, payload);
    if (method === "closed") {
      const { tunnel_id: tunnelId, reason } = (payload ?? {}) as { tunnel_id?: string; reason?: string };
      if (tunnelId !== undefined) this.sessions.drop(tunnelId);
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

  /**
   * Answer a client's Noise NK message 1 on an E2E tunnel this instance
   * accepted; the session is kept under that tunnel.
   */
  private async handshake(metadata: Readonly<Metadata> | null | undefined, payload: unknown): Promise<unknown> {
    const tunnel = this.lookup(metadata);
    if (tunnel === null || !tunnel.sealed || this.privateKey === undefined) {
      throw handshakeFailed("not an end-to-end tunnel of this instance", "server");
    }
    let answer: Awaited<ReturnType<typeof nkRespond>>;
    try {
      const message = (payload as { message?: unknown } | null | undefined)?.message;
      answer = await nkRespond(this.privateKey, b64decode(typeof message === "string" ? message : ""));
    } catch {
      throw handshakeFailed("message 1 does not open with this instance's key", "server");
    }
    this.sessions.add(tunnel.id, answer.session);
    return { message: b64encode(answer.reply) };
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

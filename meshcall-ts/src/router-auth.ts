import type { CallOptions } from "./client.js";
import { normalizeServiceName } from "./contract.js";
import type { BalanceKind, StreamKind } from "./protocol.js";
import type { MeshCallRpc } from "./rpc.js";

export type RouterCredentials =
  | { readonly username: string; readonly password: string; readonly token?: never }
  | { readonly token: string; readonly username?: never; readonly password?: never };

export const RouterCredentials = {
  bearer(token: string): RouterCredentials {
    const creds: RouterCredentials = { token };
    authorizationHeader(creds);
    return creds;
  },
  basic(username: string, password: string): RouterCredentials {
    const creds: RouterCredentials = { username, password };
    authorizationHeader(creds);
    return creds;
  },
} as const;

export function authorizationHeader(auth: RouterCredentials): string {
  if (auth.token !== undefined) {
    if (!auth.token || /[\r\n]/u.test(auth.token) || auth.username !== undefined || auth.password !== undefined) {
      throw new TypeError("Set either a token or username/password");
    }
    return "Bearer " + auth.token;
  }
  if (!auth.username || !auth.password || /[:\r\n]/u.test(auth.username)) {
    throw new TypeError("A username and password are required");
  }
  return "Basic " + Buffer.from(auth.username + ":" + auth.password, "utf8").toString("base64");
}

export interface RouterScope {
  readonly service: string;
  readonly methods?: ReadonlyArray<string>;
  readonly register?: boolean;
}

export function normalizeScopeService(service: string): string {
  if (service === "*" || service === "") return service;
  try {
    return normalizeServiceName(service).name;
  } catch {
    return service;
  }
}

export interface IssuedToken {
  readonly token: string;
  readonly token_id: string;
  readonly expires_at_unix_ms: number;
  readonly scopes: ReadonlyArray<RouterScope>;
}

export interface RouterIdentity {
  readonly username: string;
  readonly roles: ReadonlyArray<"client" | "server">;
  readonly register: ReadonlyArray<string>;
  readonly call: ReadonlyArray<string>;
  readonly expires_at_unix_ms: number | null;
}

export const ROUTER_AUTH_SERVICE = "meshcall.router.v1.AuthService";
export const REGISTRY_SERVICE = "meshcall.router.v1.Registry";

export interface RegistryBalance {
  readonly kind: BalanceKind;
  readonly key?: string | null;
}

export interface RegistryInstance {
  readonly id: string;
  readonly inflight: number;
  readonly max_inflight?: number | null;
  readonly breaker: string;
  readonly draining: boolean;
}

export interface RegistryMethod {
  readonly name: string;
  readonly description?: string | null;
  readonly stream: StreamKind;
  readonly balance: RegistryBalance;
  readonly annotations?: Readonly<Record<string, unknown>> | null;
  readonly parameters?: ReadonlyArray<string>;
  readonly duplex_reason?: string | null;
  readonly subscription?: Readonly<Record<string, unknown>> | null;
  readonly instances?: ReadonlyArray<string>;
}

export interface RegistryService {
  readonly name: string;
  readonly version?: string | null;
  readonly description?: string | null;
  readonly targeted: boolean;
  readonly instances: ReadonlyArray<RegistryInstance>;
  readonly methods: ReadonlyArray<RegistryMethod>;
  readonly contract?: Readonly<Record<string, unknown>> | null;
}

export class RouterAuthClient {
  public constructor(private readonly client: MeshCallRpc) {}

  public whoami(options?: CallOptions): Promise<RouterIdentity> {
    return this.client.unary(ROUTER_AUTH_SERVICE, "whoami", {}, options);
  }

  public issueToken(
    request: ReadonlyArray<RouterScope> | { readonly scopes: ReadonlyArray<RouterScope>; readonly ttl_seconds?: number },
    options?: CallOptions,
  ): Promise<IssuedToken> {
    const rawScopes = Array.isArray(request)
      ? request
      : (request as { readonly scopes: ReadonlyArray<RouterScope> }).scopes;
    const ttlSeconds = Array.isArray(request)
      ? 900
      : ((request as { readonly ttl_seconds?: number }).ttl_seconds ?? 900);
    const scopes = rawScopes.map((scope) => ({
      ...scope,
      service: normalizeScopeService(scope.service),
    }));
    return this.client.unary(ROUTER_AUTH_SERVICE, "issue_token", {
      scopes,
      ttl_seconds: ttlSeconds,
    }, options);
  }

  public async revokeToken(tokenId: string, options?: CallOptions): Promise<boolean> {
    const result = await this.client.unary<{ token_id: string }, { revoked: boolean }>(
      ROUTER_AUTH_SERVICE, "revoke_token", { token_id: tokenId }, options,
    );
    return result.revoked;
  }
}

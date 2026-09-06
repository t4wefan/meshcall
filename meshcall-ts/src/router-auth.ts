import type { MeshCallClient, CallOptions } from "./client.js";

export type RouterCredentials =
  | { readonly username: string; readonly password: string; readonly token?: never }
  | { readonly token: string; readonly username?: never; readonly password?: never };

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

export class RouterAuthClient {
  public constructor(private readonly client: MeshCallClient) {}

  public whoami(options?: CallOptions): Promise<RouterIdentity> {
    return this.client.unary(ROUTER_AUTH_SERVICE, "whoami", {}, options);
  }

  public issueToken(
    request: { readonly scopes: ReadonlyArray<RouterScope>; readonly ttl_seconds?: number },
    options?: CallOptions,
  ): Promise<IssuedToken> {
    return this.client.unary(ROUTER_AUTH_SERVICE, "issue_token", {
      ...request, ttl_seconds: request.ttl_seconds ?? 900,
    }, options);
  }

  public async revokeToken(tokenId: string, options?: CallOptions): Promise<boolean> {
    const result = await this.client.unary<{ token_id: string }, { revoked: boolean }>(
      ROUTER_AUTH_SERVICE, "revoke_token", { token_id: tokenId }, options,
    );
    return result.revoked;
  }
}

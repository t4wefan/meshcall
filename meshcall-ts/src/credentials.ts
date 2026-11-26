// Credential sources (docs/protocol.md → Credential sources): a function the
// runtime asks for the credential before every connection it opens, so an
// expiring token or tunnel credential is replaced without rebuilding the Mesh.

import { MeshCallError, UnauthenticatedError, UnavailableError } from "./errors.js";
import { checkTunnelCredential, TUNNEL_CREDENTIAL_PREFIX } from "./tunnel.js";

/** Returns the credential to connect with; called before every connection. */
export type CredentialSource = () => string | Promise<string>;

/** A fixed credential or a source of fresh ones. */
export type CredentialOption = string | CredentialSource;

/**
 * A credential a source returned that cannot be used as configured (a tunnel
 * credential given as a token, a plain credential where an end-to-end key is
 * pinned): a configuration error, never retried.
 */
export class CredentialSourceError extends TypeError {
  public constructor(message: string) {
    super(message);
    this.name = "CredentialSourceError";
  }
}

/**
 * Ask `source` for a credential and check it; a failing source fails this
 * connection attempt as an unreachable Router would.
 */
export async function askCredentialSource(
  source: CredentialSource,
  check: (credential: string) => void = () => undefined,
): Promise<string> {
  let credential: unknown;
  try {
    credential = await source();
  } catch (error) {
    if (error instanceof MeshCallError || error instanceof CredentialSourceError) throw error;
    throw new UnavailableError("The credential source failed: " + String(error), {
      retryable: true, details: { source: "client", reason: "credential_source" },
    });
  }
  if (typeof credential !== "string" || credential === "") {
    throw new UnavailableError("The credential source returned no credential", {
      retryable: true, details: { source: "client", reason: "credential_source" },
    });
  }
  try {
    check(credential);
  } catch (error) {
    throw new CredentialSourceError((error as Error).message);
  }
  return credential;
}

/** The Router refused the credential itself (HTTP 401 or close 4401), not the instance a handshake. */
export function isRouterRefusal(error: unknown): error is UnauthenticatedError {
  return error instanceof UnauthenticatedError && error.details?.source === "router";
}

/**
 * The source a Mesh gives its transport for a `token` or `tunnel` option that
 * is a function: what it returns is checked to be that kind of credential.
 */
export function kindedSource(source: CredentialSource, kind: "token" | "tunnel"): CredentialSource {
  return async () => {
    const credential = await source();
    if (typeof credential !== "string" || credential === "") return credential;
    try {
      if (kind === "tunnel") checkTunnelCredential(credential);
      else if (credential.startsWith(TUNNEL_CREDENTIAL_PREFIX)) {
        throw new TypeError("The token source returned a tunnel credential: pass the source as { tunnel }");
      }
    } catch (error) {
      throw new CredentialSourceError((error as Error).message);
    }
    return credential;
  };
}

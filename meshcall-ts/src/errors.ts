/** Protocol error codes; see docs/protocol.md → Errors. */
export const ErrorCode = {
  INVALID_ARGUMENT: "invalid_argument",
  METHOD_NOT_FOUND: "method_not_found",
  UNAUTHENTICATED: "unauthenticated",
  PERMISSION_DENIED: "permission_denied",
  DEADLINE_EXCEEDED: "deadline_exceeded",
  CANCELLED: "cancelled",
  UNAVAILABLE: "unavailable",
  RESOURCE_EXHAUSTED: "resource_exhausted",
  PROTOCOL_ERROR: "protocol_error",
  INTERNAL: "internal",
} as const;
export type ErrorCode = (typeof ErrorCode)[keyof typeof ErrorCode];

export interface MeshCallErrorOptions {
  retryable?: boolean;
  details?: Record<string, unknown> | null;
}

/** One field-level validation finding; never carries the input value. */
export interface Violation {
  /** Dotted path of the offending field; `""` for the root. */
  path: string;
  /** Short validator keyword: `type`, `missing`, `required`, … */
  code: string;
  message: string;
}

/**
 * An RPC error: a stable `code`, a message and optional `details`.
 *
 * Errors received from a peer are built through {@link MeshCallError.fromCode},
 * which picks the subclass for the protocol code so callers can write
 * `instanceof DeadlineExceededError` instead of comparing strings.
 * Application-defined codes stay plain `MeshCallError`.
 */
export class MeshCallError extends Error {
  public readonly code: string;
  public readonly retryable: boolean;
  public readonly details: Record<string, unknown> | null;

  public constructor(code: string, message: string, options: MeshCallErrorOptions = {}) {
    super(message);
    this.name = "MeshCallError";
    this.code = code;
    this.retryable = options.retryable ?? false;
    this.details = options.details ?? null;
  }

  public static fromCode(code: string, message: string, options: MeshCallErrorOptions = {}): MeshCallError {
    const factory = BY_CODE.get(code);
    return factory === undefined ? new MeshCallError(code, message, options) : factory(message, options);
  }

  /** `details.source`: `"client"`, `"router"` or `"server"`. */
  public get source(): string | undefined {
    const value = this.details?.source;
    return typeof value === "string" ? value : undefined;
  }

  /** `details.reason` when the sender supplied one. */
  public get reason(): string | undefined {
    const value = this.details?.reason;
    return typeof value === "string" ? value : undefined;
  }
}

export class InvalidArgumentError extends MeshCallError {
  public constructor(message: string, options: MeshCallErrorOptions = {}) {
    super(ErrorCode.INVALID_ARGUMENT, message, options);
    this.name = "InvalidArgumentError";
  }

  /** Field-level findings when the server validated the request. */
  public get violations(): Violation[] {
    const raw = this.details?.violations;
    if (!Array.isArray(raw)) return [];
    return raw.flatMap((item) => {
      if (typeof item !== "object" || item === null) return [];
      const record = item as Record<string, unknown>;
      return [{
        path: String(record.path ?? ""), code: String(record.code ?? ""), message: String(record.message ?? ""),
      }];
    });
  }
}

export class MethodNotFoundError extends MeshCallError {
  public constructor(message: string, options: MeshCallErrorOptions = {}) {
    super(ErrorCode.METHOD_NOT_FOUND, message, options);
    this.name = "MethodNotFoundError";
  }
}

export class UnauthenticatedError extends MeshCallError {
  public constructor(message: string, options: MeshCallErrorOptions = {}) {
    super(ErrorCode.UNAUTHENTICATED, message, options);
    this.name = "UnauthenticatedError";
  }
}

export class PermissionDeniedError extends MeshCallError {
  public constructor(message: string, options: MeshCallErrorOptions = {}) {
    super(ErrorCode.PERMISSION_DENIED, message, options);
    this.name = "PermissionDeniedError";
  }
}

export class DeadlineExceededError extends MeshCallError {
  public constructor(message: string, options: MeshCallErrorOptions = {}) {
    super(ErrorCode.DEADLINE_EXCEEDED, message, options);
    this.name = "DeadlineExceededError";
  }
}

/** The call was cancelled by the caller or a peer. */
export class CallCancelledError extends MeshCallError {
  public constructor(message: string, options: MeshCallErrorOptions = {}) {
    super(ErrorCode.CANCELLED, message, options);
    this.name = "CallCancelledError";
  }
}

export class UnavailableError extends MeshCallError {
  public constructor(message: string, options: MeshCallErrorOptions = {}) {
    super(ErrorCode.UNAVAILABLE, message, options);
    this.name = "UnavailableError";
  }
}

export class ResourceExhaustedError extends MeshCallError {
  public constructor(message: string, options: MeshCallErrorOptions = {}) {
    super(ErrorCode.RESOURCE_EXHAUSTED, message, options);
    this.name = "ResourceExhaustedError";
  }
}

export class ProtocolError extends MeshCallError {
  public constructor(message: string, options: MeshCallErrorOptions = {}) {
    super(ErrorCode.PROTOCOL_ERROR, message, options);
    this.name = "ProtocolError";
  }
}

export class InternalError extends MeshCallError {
  public constructor(message: string, options: MeshCallErrorOptions = {}) {
    super(ErrorCode.INTERNAL, message, options);
    this.name = "InternalError";
  }
}

type Factory = (message: string, options: MeshCallErrorOptions) => MeshCallError;
const BY_CODE = new Map<string, Factory>([
  [ErrorCode.INVALID_ARGUMENT, (m, o) => new InvalidArgumentError(m, o)],
  [ErrorCode.METHOD_NOT_FOUND, (m, o) => new MethodNotFoundError(m, o)],
  [ErrorCode.UNAUTHENTICATED, (m, o) => new UnauthenticatedError(m, o)],
  [ErrorCode.PERMISSION_DENIED, (m, o) => new PermissionDeniedError(m, o)],
  [ErrorCode.DEADLINE_EXCEEDED, (m, o) => new DeadlineExceededError(m, o)],
  [ErrorCode.CANCELLED, (m, o) => new CallCancelledError(m, o)],
  [ErrorCode.UNAVAILABLE, (m, o) => new UnavailableError(m, o)],
  [ErrorCode.RESOURCE_EXHAUSTED, (m, o) => new ResourceExhaustedError(m, o)],
  [ErrorCode.PROTOCOL_ERROR, (m, o) => new ProtocolError(m, o)],
  [ErrorCode.INTERNAL, (m, o) => new InternalError(m, o)],
]);

/** WebSocket close status for credentials that are not, or no longer, valid. */
export const CLOSE_UNAUTHENTICATED = 4401;
/** Close reasons that name an error reason (docs/protocol.md → Tunnels). */
const CLOSE_REASONS = new Set(["tunnel_revoked", "tunnel_closed"]);

/**
 * @internal The error a Router close status stands for, or `undefined` for a
 * plain loss: 4401 the credentials are not (or no longer) valid, a revoked
 * tunnel saying `tunnel_revoked`; 1008 the Router refused the connection's
 * request (a registration, a role) and says why.
 */
export function closeError(code: number, reason: string): MeshCallError | undefined {
  if (code === CLOSE_UNAUTHENTICATED) {
    return new UnauthenticatedError("The Router closed the connection: " + (reason || "unauthenticated"), {
      details: { source: "router", ...(CLOSE_REASONS.has(reason) ? { reason } : {}) },
    });
  }
  if (code === 1008) {
    return new PermissionDeniedError("The Router refused the connection: " + (reason || "policy violation"), {
      details: { source: "router", reason: "policy_violation" },
    });
  }
  // Credentials in hello refused like their header counterparts: 4000 + the
  // HTTP status (403, 429, 503). The last two are temporary.
  if (code === 4403 || code === 4429 || code === 4503) return handshakeError(code - 4000, reason);
  return undefined;
}

/** @internal The error an HTTP status refusing a WebSocket upgrade stands for. */
export function handshakeError(status: number, body = ""): MeshCallError | undefined {
  const text = " (HTTP " + String(status) + ")" + (body.trim() === "" ? "" : ": " + body.trim());
  const details = { source: "router" };
  if (status === 401) return new UnauthenticatedError("The Router refused the credentials" + text, { details });
  if (status === 403) return new PermissionDeniedError("The Router refused the connection" + text, { details });
  if (status === 429) return new ResourceExhaustedError("The Router refused the connection" + text, { retryable: true, details });
  if (status === 503) return new UnavailableError("The Router is busy" + text, { retryable: true, details });
  return undefined;
}

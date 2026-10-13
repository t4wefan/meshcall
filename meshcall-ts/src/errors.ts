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

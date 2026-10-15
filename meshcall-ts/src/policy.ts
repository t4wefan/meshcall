/**
 * Platform-neutral call policies (no Node imports): reconnect and retry
 * backoff, deadline grace, metadata rules. Shared by the WebSocket client
 * and the HTTP web client.
 */
import type { Metadata } from "./protocol.js";

export const RESERVED_METADATA_PREFIX = "meshcall-";

/**
 * Exponential backoff with jitter for re-establishing a lost connection.
 * `maxAttempts` undefined means keep trying until the owner closes or, for
 * clients, the call's timeout expires.
 */
export interface ReconnectPolicy {
  readonly enabled?: boolean;
  readonly initialDelayMs?: number;
  readonly maxDelayMs?: number;
  readonly multiplier?: number;
  readonly jitter?: number;
  readonly maxAttempts?: number;
}

export const DEFAULT_RECONNECT: Required<Omit<ReconnectPolicy, "maxAttempts">> = {
  enabled: true, initialDelayMs: 100, maxDelayMs: 30_000, multiplier: 2, jitter: 0.2,
};

/**
 * Client-side retry for failed unary calls. Only `retryable` errors with a
 * code in `retryCodes` are retried, and a method not declared `idempotent`
 * only when the call carries an `idempotencyKey`. Attempts share the call's
 * `timeoutMs`. Streams are never retried.
 */
export interface RetryPolicy {
  readonly enabled?: boolean;
  readonly maxAttempts?: number;
  readonly initialDelayMs?: number;
  readonly maxDelayMs?: number;
  readonly multiplier?: number;
  readonly jitter?: number;
  readonly retryCodes?: ReadonlyArray<string>;
}

export const DEFAULT_RETRY: Required<RetryPolicy> = {
  enabled: true, maxAttempts: 3, initialDelayMs: 100, maxDelayMs: 2000, multiplier: 2, jitter: 0.2,
  retryCodes: ["unavailable", "resource_exhausted"],
};

export function retryDelayMs(policy: Required<RetryPolicy>, attempt: number): number {
  const base = Math.min(policy.maxDelayMs, policy.initialDelayMs * policy.multiplier ** (attempt - 1));
  const spread = base * policy.jitter;
  return Math.max(0, base + (Math.random() * 2 - 1) * spread);
}

export function* reconnectDelays(policy: ReconnectPolicy): Generator<number> {
  const p = { ...DEFAULT_RECONNECT, ...policy };
  if (p.initialDelayMs <= 0 || p.maxDelayMs < p.initialDelayMs || p.multiplier < 1
    || p.jitter < 0 || p.jitter > 1 || (p.maxAttempts !== undefined && p.maxAttempts < 1)) {
    throw new RangeError("Invalid reconnect policy");
  }
  let delay = p.initialDelayMs;
  for (let attempt = 0; p.maxAttempts === undefined || attempt < p.maxAttempts; attempt += 1) {
    const spread = delay * p.jitter;
    yield Math.max(0, delay + (Math.random() * 2 - 1) * spread);
    delay = Math.min(p.maxDelayMs, delay * p.multiplier);
  }
}

/** Extra time a client waits past `timeoutMs` for the peer's own deadline error. */
export function clientDeadlineGraceMs(timeoutMs: number): number {
  return Math.max(1000, timeoutMs * 0.1);
}

export function validateMetadata(metadata: Metadata | undefined | null): Metadata | null {
  if (metadata === undefined || metadata === null) return null;
  for (const [key, value] of Object.entries(metadata)) {
    if (key.length === 0) throw new RangeError("metadata keys must not be empty");
    if (key.startsWith(RESERVED_METADATA_PREFIX)) {
      throw new RangeError("metadata key " + JSON.stringify(key) + " uses the reserved prefix");
    }
    if (typeof value !== "string") {
      throw new TypeError("metadata value for " + JSON.stringify(key) + " must be a string");
    }
  }
  return { ...metadata };
}

import { randomUUID } from "node:crypto";

import type WebSocket from "ws";

import { UnavailableError } from "./errors.js";
import type { Frame, Metadata } from "./protocol.js";
import type { FrameWriter } from "./transport.js";

export const DEFAULT_PING_INTERVAL_MS = 15_000;
export const DEFAULT_PING_TIMEOUT_MS = 10_000;
export const DEFAULT_IDLE_TIMEOUT_MS = 45_000;
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

export interface LivenessOptions {
  /** Connecting peers ping this often; `null` disables pinging. */
  readonly pingIntervalMs?: number | null;
  readonly pingTimeoutMs?: number;
  /** Accepting peers drop a connection silent for this long; `null` disables. */
  readonly idleTimeoutMs?: number | null;
}

/**
 * Protocol-level keepalive for one connection.
 *
 * A connecting peer (client, or service instance talking to a Router) pings
 * and expects a pong; an accepting peer (Router or direct server) only watches
 * for silence. Either failure closes the socket, which the owner already
 * handles as a connection loss.
 */
export class Liveness {
  private pingTimer: ReturnType<typeof setTimeout> | undefined;
  private pongTimer: ReturnType<typeof setTimeout> | undefined;
  private idleTimer: ReturnType<typeof setTimeout> | undefined;
  private pendingNonce: string | undefined;
  private stopped = false;

  public constructor(
    private readonly socket: WebSocket,
    private readonly writer: FrameWriter,
    private readonly options: LivenessOptions,
  ) {}

  public start(): void {
    if (this.stopped) return;
    this.schedulePing();
    this.touch();
  }

  /** Feed every received frame; returns true when the frame was keepalive. */
  public observe(frame: Frame): boolean {
    this.touch();
    if (frame.kind === "ping") {
      void this.writer.send({ kind: "pong", nonce: frame.nonce }).catch(() => undefined);
      return true;
    }
    if (frame.kind === "pong") {
      if (frame.nonce === this.pendingNonce) {
        this.pendingNonce = undefined;
        if (this.pongTimer !== undefined) clearTimeout(this.pongTimer);
        this.pongTimer = undefined;
        this.schedulePing();
      }
      return true;
    }
    return false;
  }

  public stop(): void {
    this.stopped = true;
    for (const timer of [this.pingTimer, this.pongTimer, this.idleTimer]) {
      if (timer !== undefined) clearTimeout(timer);
    }
    this.pingTimer = this.pongTimer = this.idleTimer = undefined;
  }

  private touch(): void {
    const idle = this.options.idleTimeoutMs ?? null;
    if (idle === null || this.stopped) return;
    if (this.idleTimer !== undefined) clearTimeout(this.idleTimer);
    this.idleTimer = setTimeout(() => this.fail("idle timeout"), idle);
    this.idleTimer.unref?.();
  }

  private schedulePing(): void {
    const interval = this.options.pingIntervalMs ?? null;
    if (interval === null || this.stopped || this.pingTimer !== undefined) return;
    this.pingTimer = setTimeout(() => {
      this.pingTimer = undefined;
      if (this.stopped) return;
      const nonce = randomUUID();
      this.pendingNonce = nonce;
      this.pongTimer = setTimeout(
        () => this.fail("keepalive timeout"),
        this.options.pingTimeoutMs ?? DEFAULT_PING_TIMEOUT_MS,
      );
      this.pongTimer.unref?.();
      void this.writer.send({ kind: "ping", nonce }).catch(() => this.fail("keepalive send failed"));
    }, interval);
    this.pingTimer.unref?.();
  }

  private fail(reason: string): void {
    if (this.stopped) return;
    this.stop();
    this.writer.close(new UnavailableError("Connection was lost: " + reason, { retryable: true }));
    this.socket.close(1001, reason);
    // A peer that is truly gone never completes the close handshake.
    const force = setTimeout(() => this.socket.terminate(), 1000);
    force.unref?.();
    this.socket.once("close", () => clearTimeout(force));
  }
}

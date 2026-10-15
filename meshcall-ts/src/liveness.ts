import { randomUUID } from "node:crypto";

import type WebSocket from "ws";

import { UnavailableError } from "./errors.js";
import type { Frame } from "./protocol.js";
import type { FrameWriter } from "./transport.js";

export const DEFAULT_PING_INTERVAL_MS = 15_000;
export const DEFAULT_PING_TIMEOUT_MS = 10_000;
export const DEFAULT_IDLE_TIMEOUT_MS = 45_000;
export {
  clientDeadlineGraceMs, DEFAULT_RECONNECT, DEFAULT_RETRY, reconnectDelays, RESERVED_METADATA_PREFIX, retryDelayMs,
  validateMetadata, type ReconnectPolicy, type RetryPolicy,
} from "./policy.js";

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

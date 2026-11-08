import WebSocket from "ws";

import { CallCancelledError, closeError, type MeshCallError, ResourceExhaustedError, UnavailableError } from "./errors.js";
import { encodeFrame, type Frame } from "./protocol.js";

/** Largest encoded frame, sent or received, unless configured smaller; also the Router's ceiling. */
export const DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024;

/** Whether `error` is a frame `FrameWriter.send` did not send because it was over the size limit. */
export function isFrameTooLarge(error: unknown): error is ResourceExhaustedError {
  return error instanceof ResourceExhaustedError && error.details?.reason === "frame_too_large";
}

interface PendingFrame {
  kind: Frame["kind"];
  encoded: string;
  resolve: () => void;
  reject: (error: unknown) => void;
}

/** Control priority and round-robin scheduling shared by direct/routed sessions. */
export class FrameWriter {
  private readonly controls: PendingFrame[] = [];
  private readonly calls = new Map<string, PendingFrame[]>();
  private readonly order: string[] = [];
  private active: PendingFrame | undefined;
  private draining = false;
  private closed: Error | undefined;
  /** What the peer's close status said, once the socket has closed. */
  private closeFailure: MeshCallError | undefined;
  private settleGone!: () => void;
  /** Settles when the socket has closed or this writer was closed. */
  public readonly gone = new Promise<void>((resolve) => { this.settleGone = resolve; });

  public constructor(
    private readonly socket: WebSocket,
    private readonly maxFrameSize: number,
    private readonly source: "client" | "server",
  ) {
    socket.once("close", (code: number, reason: Buffer) => {
      this.closeFailure = closeError(code, reason.toString());
      this.settleGone();
    });
  }

  public async send(frame: Frame): Promise<void> {
    if (this.closed !== undefined) throw this.closed;
    if (this.socket.readyState !== WebSocket.OPEN) throw await this.closing();
    if (frame.kind === "call.cancel" && this.cancelQueuedCall(frame.call_id)) {
      return;
    }
    const encoded = encodeFrame(frame);
    const size = Buffer.byteLength(encoded);
    if (size > this.maxFrameSize) {
      // The peer would close the connection on receiving it; only this
      // frame's call fails (docs/protocol.md → Frame size).
      throw new ResourceExhaustedError(`Not sent: ${frame.kind} is ${size} bytes, limit ${this.maxFrameSize}`, {
        details: { source: this.source, reason: "frame_too_large" },
      });
    }
    const completion = new Promise<void>((resolve, reject) => {
      const pending = { kind: frame.kind, encoded, resolve, reject };
      if (!("call_id" in frame) || isControl(frame)) {
        this.controls.push(pending);
      } else {
        let queue = this.calls.get(frame.call_id);
        if (queue === undefined) {
          queue = [];
          this.calls.set(frame.call_id, queue);
          this.order.push(frame.call_id);
        }
        queue.push(pending);
      }
    });
    if (!this.draining) {
      this.draining = true;
      queueMicrotask(() => void this.drain());
    }
    await completion;
  }

  private cancelQueuedCall(callId: string): boolean {
    const queue = this.calls.get(callId);
    if (queue === undefined) return false;
    const unopened = queue.some((pending) => pending.kind === "call.open");
    this.calls.delete(callId);
    const index = this.order.indexOf(callId);
    if (index >= 0) this.order.splice(index, 1);
    const error = new CallCancelledError("Call was cancelled before sending");
    for (const pending of queue) pending.reject(error);
    // A priority cancellation cannot precede an unsent call.open: remove both.
    // Already transmitted calls still need the cancellation frame forwarded.
    return unopened;
  }

  /**
   * The error a frame fails with on a socket that is no longer open. A closing
   * socket's close status is about to say why (a 4401 reason), so wait for it
   * rather than report a retryable loss.
   */
  private async closing(): Promise<Error> {
    if (this.socket.readyState === WebSocket.CLOSING) await this.gone;
    return this.closeFailure ?? this.closed ?? new UnavailableError("Connection is closed");
  }

  public close(error: Error = this.closeFailure ?? new UnavailableError("Connection is closed")): void {
    this.settleGone();
    if (this.closed !== undefined) return;
    this.closed = error;
    this.active?.reject(error);
    for (const pending of this.controls.splice(0)) pending.reject(error);
    for (const queue of this.calls.values()) {
      for (const pending of queue) pending.reject(error);
    }
    this.calls.clear();
    this.order.length = 0;
  }

  private next(): PendingFrame | undefined {
    const control = this.controls.shift();
    if (control !== undefined) return control;
    const callId = this.order.shift();
    if (callId === undefined) return undefined;
    const queue = this.calls.get(callId)!;
    const pending = queue.shift();
    if (queue.length === 0) this.calls.delete(callId);
    else this.order.push(callId);
    return pending;
  }

  private async drain(): Promise<void> {
    try {
      while (this.closed === undefined) {
        const pending = this.next();
        if (pending === undefined) break;
        this.active = pending;
        await new Promise<void>((resolve, reject) => {
          this.socket.send(pending.encoded, (error) =>
            error == null ? resolve() : reject(error),
          );
        });
        pending.resolve();
        this.active = undefined;
      }
    } catch {
      this.close(await this.closing());
      this.socket.close();
    } finally {
      this.active = undefined;
      this.draining = false;
    }
  }
}

function isControl(frame: Frame): boolean {
  return frame.kind === "call.cancel" || frame.kind === "stream.window"
    || frame.kind === "ping" || frame.kind === "pong";
}

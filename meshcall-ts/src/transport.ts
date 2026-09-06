import WebSocket from "ws";

import { MeshCallError } from "./errors.js";
import { encodeFrame, type Frame } from "./protocol.js";

export const DEFAULT_MAX_FRAME_SIZE = 1024 * 1024;

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

  public constructor(
    private readonly socket: WebSocket,
    private readonly maxFrameSize = DEFAULT_MAX_FRAME_SIZE,
  ) {}

  public async send(frame: Frame): Promise<void> {
    if (this.closed !== undefined) throw this.closed;
    if (this.socket.readyState !== WebSocket.OPEN) {
      throw new MeshCallError("unavailable", "Connection is closed");
    }
    if (frame.kind === "call.cancel" && this.cancelQueuedCall(frame.call_id)) {
      return;
    }
    const encoded = encodeFrame(frame);
    if (Buffer.byteLength(encoded) > this.maxFrameSize) {
      throw new MeshCallError("resource_exhausted", "Encoded frame exceeds the byte limit");
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
    const error = new MeshCallError("cancelled", "Call was cancelled before sending");
    for (const pending of queue) pending.reject(error);
    // A priority cancellation cannot precede an unsent call.open: remove both.
    // Already transmitted calls still need the cancellation frame forwarded.
    return unopened;
  }

  public close(error = new MeshCallError("unavailable", "Connection is closed")): void {
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
      this.close();
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

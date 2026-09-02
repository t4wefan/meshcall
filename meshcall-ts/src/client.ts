import { randomUUID } from "node:crypto";

import WebSocket, { type RawData } from "ws";

import { MeshCallError } from "./errors.js";
import {
  type CallCancelFrame,
  type CallErrorFrame,
  type CallOpenFrame,
  type CallResultFrame,
  decodeFrame,
  encodeFrame,
  type Frame,
  type HelloAckFrame,
  PROTOCOL_VERSION,
} from "./protocol.js";

export interface CallOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

interface PendingCall {
  resolve: (value: unknown) => void;
  reject: (error: unknown) => void;
  timer?: ReturnType<typeof setTimeout>;
  signal?: AbortSignal;
  abortListener?: () => void;
}

export class MeshCallClient {
  private socket: WebSocket | undefined;
  private connectPromise: Promise<void> | undefined;
  private readonly calls = new Map<string, PendingCall>();

  public constructor(
    private readonly url: string,
    private readonly peerId: string = randomUUID().replaceAll("-", ""),
  ) {}

  public async connect(): Promise<void> {
    if (this.socket?.readyState === WebSocket.OPEN) {
      return;
    }
    if (this.connectPromise !== undefined) {
      return this.connectPromise;
    }
    this.connectPromise = this.open();
    try {
      await this.connectPromise;
    } finally {
      this.connectPromise = undefined;
    }
  }

  public async unary<Request, Response>(
    service: string,
    method: string,
    request: Request,
    options: CallOptions = {},
  ): Promise<Response> {
    if (options.timeoutMs !== undefined && options.timeoutMs <= 0) {
      throw new RangeError("timeoutMs must be positive");
    }
    if (options.signal?.aborted === true) {
      throw new MeshCallError("cancelled", "Call was cancelled");
    }
    await this.connect();
    const callId = randomUUID().replaceAll("-", "");
    const result = new Promise<Response>((resolve, reject) => {
      const pending: PendingCall = {
        resolve: (value) => resolve(value as Response),
        reject,
      };
      if (options.timeoutMs !== undefined) {
        pending.timer = setTimeout(() => {
          this.cancelPending(
            callId,
            new MeshCallError("deadline_exceeded", "Call deadline exceeded"),
            "deadline_exceeded",
          );
        }, options.timeoutMs);
      }
      if (options.signal !== undefined) {
        pending.signal = options.signal;
        pending.abortListener = () => {
          this.cancelPending(
            callId,
            new MeshCallError("cancelled", "Call was cancelled"),
            "caller_cancelled",
          );
        };
        options.signal.addEventListener("abort", pending.abortListener, { once: true });
      }
      this.calls.set(callId, pending);
    });

    const frame: CallOpenFrame = {
      kind: "call.open",
      call_id: callId,
      service,
      method,
      payload: request,
      deadline_unix_ms:
        options.timeoutMs === undefined ? null : Date.now() + options.timeoutMs,
    };
    try {
      await this.send(frame);
    } catch (error) {
      this.rejectPending(callId, error);
    }
    return result;
  }

  public async close(): Promise<void> {
    const socket = this.socket;
    this.socket = undefined;
    if (socket !== undefined) {
      socket.removeAllListeners();
      await new Promise<void>((resolve) => {
        socket.once("close", () => resolve());
        socket.close();
      });
    }
    this.rejectAll(new MeshCallError("unavailable", "Client is closed"));
  }

  private async open(): Promise<void> {
    const socket = new WebSocket(this.url);
    await waitForOpen(socket);
    socket.send(
      encodeFrame({
        kind: "hello",
        protocol: PROTOCOL_VERSION,
        role: "client",
        peer_id: this.peerId,
      }),
    );
    const response = await waitForFrame(socket);
    if (response.kind !== "hello.ack") {
      socket.close();
      throw new MeshCallError("protocol_error", "Expected hello.ack");
    }
    const hello = response as HelloAckFrame;
    if (hello.protocol !== PROTOCOL_VERSION) {
      socket.close();
      throw new MeshCallError(
        "protocol_error",
        `Unsupported protocol: ${hello.protocol}`,
      );
    }
    this.socket = socket;
    socket.on("message", (data) => this.handleMessage(data));
    socket.on("close", () => {
      if (this.socket === socket) {
        this.socket = undefined;
      }
      this.rejectAll(new MeshCallError("unavailable", "Connection was lost"));
    });
    socket.on("error", (error) => this.rejectAll(error));
  }

  private handleMessage(data: RawData): void {
    let frame: Frame;
    try {
      frame = decodeFrame(data);
    } catch (error) {
      this.rejectAll(error);
      this.socket?.close();
      return;
    }
    if (frame.kind === "ping") {
      void this.send({ kind: "pong", nonce: frame.nonce });
      return;
    }
    if (frame.kind === "call.result") {
      this.resolvePending(frame);
    } else if (frame.kind === "call.error") {
      this.rejectCall(frame);
    }
  }

  private resolvePending(frame: CallResultFrame): void {
    const pending = this.takePending(frame.call_id);
    pending?.resolve(frame.payload);
  }

  private rejectCall(frame: CallErrorFrame): void {
    const pending = this.takePending(frame.call_id);
    pending?.reject(
      new MeshCallError(frame.error.code, frame.error.message, {
        retryable: frame.error.retryable ?? false,
        details: frame.error.details ?? null,
      }),
    );
  }

  private cancelPending(callId: string, error: MeshCallError, reason: string): void {
    const pending = this.takePending(callId);
    if (pending === undefined) {
      return;
    }
    const frame: CallCancelFrame = {
      kind: "call.cancel",
      call_id: callId,
      reason,
    };
    void this.send(frame).catch(() => undefined);
    pending.reject(error);
  }

  private rejectPending(callId: string, error: unknown): void {
    this.takePending(callId)?.reject(error);
  }

  private rejectAll(error: unknown): void {
    for (const callId of this.calls.keys()) {
      this.rejectPending(callId, error);
    }
  }

  private takePending(callId: string): PendingCall | undefined {
    const pending = this.calls.get(callId);
    if (pending === undefined) {
      return undefined;
    }
    this.calls.delete(callId);
    if (pending.timer !== undefined) {
      clearTimeout(pending.timer);
    }
    if (pending.signal !== undefined && pending.abortListener !== undefined) {
      pending.signal.removeEventListener("abort", pending.abortListener);
    }
    return pending;
  }

  private async send(frame: Frame): Promise<void> {
    const socket = this.socket;
    if (socket === undefined || socket.readyState !== WebSocket.OPEN) {
      throw new MeshCallError("unavailable", "Client is not connected");
    }
    await new Promise<void>((resolve, reject) => {
      socket.send(encodeFrame(frame), (error) => {
        if (error == null) {
          resolve();
        } else {
          reject(error);
        }
      });
    });
  }
}

async function waitForOpen(socket: WebSocket): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
}

async function waitForFrame(socket: WebSocket): Promise<Frame> {
  return new Promise<Frame>((resolve, reject) => {
    const onMessage = (data: RawData): void => {
      cleanup();
      try {
        resolve(decodeFrame(data));
      } catch (error) {
        reject(error);
      }
    };
    const onError = (error: Error): void => {
      cleanup();
      reject(error);
    };
    const onClose = (): void => {
      cleanup();
      reject(new MeshCallError("unavailable", "Connection closed during handshake"));
    };
    const cleanup = (): void => {
      socket.off("message", onMessage);
      socket.off("error", onError);
      socket.off("close", onClose);
    };
    socket.on("message", onMessage);
    socket.on("error", onError);
    socket.on("close", onClose);
  });
}

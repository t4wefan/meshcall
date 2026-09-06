import { randomUUID } from "node:crypto";

import WebSocket, { type RawData } from "ws";

import { closeSocket, createSocket, type WebSocketEndpoint } from "./endpoint.js";
import { MeshCallError } from "./errors.js";
import { AsyncQueue, CreditWindow, INITIAL_STREAM_CREDIT } from "./flow.js";
import { DEFAULT_MAX_FRAME_SIZE, FrameWriter } from "./transport.js";
import {
  type CallCancelFrame,
  type CallErrorFrame,
  type CallOpenFrame,
  type CallResultFrame,
  decodeFrame,
  type Frame,
  type StreamEndFrame,
  type StreamItemFrame,
  type StreamWindowFrame,
  PROTOCOL_VERSION,
} from "./protocol.js";

import type { RouterCredentials } from "./router-auth.js";

export { INITIAL_STREAM_CREDIT } from "./flow.js";

export interface CallOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

export interface MeshCallClientOptions {
  readonly auth?: RouterCredentials;
  readonly maxFrameSize?: number;
  readonly handshakeTimeoutMs?: number;
}

interface InputState {
  readonly credit: CreditWindow;
  sequence: number;
  closed: boolean;
}

interface PendingCall {
  resolve: (value: unknown) => void;
  reject: (error: unknown) => void;
  timer?: ReturnType<typeof setTimeout>;
  signal?: AbortSignal;
  abortListener?: () => void;
  input?: InputState;
  closed: boolean;
}

/** A server-to-client stream with explicit cancellation. */
export class MeshCallServerStream<Item>
  implements AsyncIterableIterator<Item>
{
  public constructor(private readonly open: Promise<ServerStreamState<Item>>) {}

  public [Symbol.asyncIterator](): AsyncIterableIterator<Item> {
    return this;
  }

  public async next(): Promise<IteratorResult<Item>> {
    return (await this.open).next();
  }

  public async cancel(reason = "client_closed"): Promise<void> {
    await (await this.open).cancel(reason);
  }

  public async return(): Promise<IteratorResult<Item>> {
    await this.cancel("iterator_closed");
    return { done: true, value: undefined };
  }
}

export class MeshCallClient {
  private socket: WebSocket | undefined;
  private connectingSocket: WebSocket | undefined;
  private writer: FrameWriter | undefined;
  private connectPromise: Promise<void> | undefined;
  private readonly calls = new Map<string, PendingCall>();
  private readonly streams = new Map<string, ServerStreamState<unknown>>();

  public constructor(
    private readonly url: WebSocketEndpoint,
    private readonly peerId: string = randomUUID().replaceAll("-", ""),
    private readonly options: MeshCallClientOptions = {},
  ) {
    for (const value of [options.maxFrameSize, options.handshakeTimeoutMs]) {
      if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
        throw new RangeError("Limits must be positive integers");
      }
    }
  }

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
    this.validateOptions(options);
    this.ensureNotAborted(options);
    await this.connect();
    this.ensureNotAborted(options);

    const callId = newCallId();
    const { result } = this.registerCall<Response>(callId, options);
    try {
      await this.send(this.openFrame(callId, service, method, request, options));
    } catch (error) {
      this.rejectPending(callId, error);
    }
    return result;
  }

  /**
   * Sends an async iterable to the server and resolves with its final result.
   * The server grants input credit with stream.window frames.
   */
  public async clientStream<Request, Item, Response>(
    service: string,
    method: string,
    request: Request,
    items: AsyncIterable<Item>,
    options: CallOptions = {},
  ): Promise<Response> {
    this.validateOptions(options);
    this.ensureNotAborted(options);
    await this.connect();
    this.ensureNotAborted(options);

    const callId = newCallId();
    const input: InputState = {
      credit: new CreditWindow(),
      sequence: 0,
      closed: false,
    };
    const { pending, result } = this.registerCall<Response>(
      callId,
      options,
      input,
    );
    try {
      await this.send(this.openFrame(callId, service, method, request, options));
    } catch (error) {
      this.rejectPending(callId, error);
      return result;
    }

    const stop = new AbortController();
    let pumpSettled = false;
    const pump = this.pumpInput(callId, pending, items, stop.signal).finally(
      () => {
        pumpSettled = true;
      },
    );
    // The server normally sends call.result after stream.end. If it fails
    // early, stop the producer without making its late rejection unhandled.
    pump.catch(() => undefined);
    try {
      return await result;
    } finally {
      if (!pumpSettled) {
        stop.abort();
      }
    }
  }

  /** Returns a lazy async iterator for server-produced items. */
  public serverStream<Request, Item>(
    service: string,
    method: string,
    request: Request,
    options: CallOptions = {},
  ): MeshCallServerStream<Item> {
    this.validateOptions(options);
    return new MeshCallServerStream(
      this.openServerStream(service, method, request, options),
    );
  }

  public async close(): Promise<void> {
    const sockets = new Set([this.socket, this.connectingSocket]);
    this.socket = undefined;
    this.connectingSocket = undefined;
    this.writer?.close();
    this.writer = undefined;
    this.rejectAll(new MeshCallError("unavailable", "Client is closed"));
    await Promise.all([...sockets].map((socket) => socket === undefined ? undefined : closeSocket(socket)));
  }

  private async openServerStream<Item>(
    service: string,
    method: string,
    request: unknown,
    options: CallOptions,
  ): Promise<ServerStreamState<Item>> {
    this.ensureNotAborted(options);
    await this.connect();
    this.ensureNotAborted(options);

    const callId = newCallId();
    const stream = new ServerStreamState<Item>(
      callId,
      (frame) => this.send(frame),
      () => this.streams.delete(callId),
    );
    this.streams.set(callId, stream as ServerStreamState<unknown>);
    try {
      await this.send(this.openFrame(callId, service, method, request, options));
      stream.grant(INITIAL_STREAM_CREDIT);
      await this.send({
        kind: "stream.window",
        call_id: callId,
        direction: "server",
        credit: INITIAL_STREAM_CREDIT,
      });
      stream.arm(options);
      return stream;
    } catch (error) {
      stream.fail(error);
      throw error;
    }
  }

  private registerCall<Response>(
    callId: string,
    options: CallOptions,
    input?: InputState,
  ): { pending: PendingCall; result: Promise<Response> } {
    let resolveResult: (value: Response) => void = () => undefined;
    let rejectResult: (error: unknown) => void = () => undefined;
    const result = new Promise<Response>((resolve, reject) => {
      resolveResult = resolve;
      rejectResult = reject;
    });
    const pending: PendingCall = {
      resolve: (value) => resolveResult(value as Response),
      reject: rejectResult,
      closed: false,
      ...(input === undefined ? {} : { input }),
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
      options.signal.addEventListener("abort", pending.abortListener, {
        once: true,
      });
    }
    this.calls.set(callId, pending);
    return { pending, result };
  }

  private async pumpInput(
    callId: string,
    pending: PendingCall,
    items: AsyncIterable<unknown>,
    stop: AbortSignal,
  ): Promise<void> {
    const input = pending.input;
    if (input === undefined) {
      throw new Error("Client stream is missing input state");
    }
    const iterator = items[Symbol.asyncIterator]();
    try {
      while (!stop.aborted && !pending.closed) {
        const next = await iterator.next();
        if (next.done === true || stop.aborted || pending.closed) {
          break;
        }
        await input.credit.acquire();
        if (stop.aborted || pending.closed) {
          break;
        }
        await this.send({
          kind: "stream.item",
          call_id: callId,
          direction: "client",
          sequence: input.sequence,
          payload: next.value,
        });
        input.sequence += 1;
      }
      if (!stop.aborted && !pending.closed && !input.closed) {
        input.closed = true;
        await this.send({
          kind: "stream.end",
          call_id: callId,
          direction: "client",
        });
      }
    } catch (error) {
      if (!pending.closed) {
        this.cancelPending(callId, error, "input_producer_failed");
      }
      throw error;
    } finally {
      await iterator.return?.();
    }
  }

  private openFrame(
    callId: string,
    service: string,
    method: string,
    request: unknown,
    options: CallOptions,
  ): CallOpenFrame {
    return {
      kind: "call.open",
      call_id: callId,
      service,
      method,
      payload: request,
      deadline_unix_ms:
        options.timeoutMs === undefined ? null : Date.now() + options.timeoutMs,
    };
  }

  private async open(): Promise<void> {
    const maxSize = this.options.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE;
    const timeout = this.options.handshakeTimeoutMs ?? 10_000;
    const socket = createSocket(this.url, maxSize, timeout, this.options.auth);
    const writer = new FrameWriter(socket, maxSize);
    this.connectingSocket = socket;
    try {
      await new Promise<void>((resolve, reject) => {
        let ready = false;
        const fail = (error: unknown) => {
          clearTimeout(timer);
          reject(error);
          writer.close();
          socket.close();
        };
        const timer = setTimeout(() =>
          fail(new MeshCallError("deadline_exceeded", "MeshCall handshake timed out")), timeout);
        socket.once("open", () => {
          void writer.send({
            kind: "hello", protocol: PROTOCOL_VERSION, role: "client", peer_id: this.peerId,
          }).catch(fail);
        });
        socket.on("message", (data) => {
          if (ready) {
            this.handleMessage(data);
            return;
          }
          try {
            const frame = decodeFrame(data);
            if (frame.kind !== "hello.ack" || frame.protocol !== PROTOCOL_VERSION) {
              throw new MeshCallError("protocol_error", "Expected compatible hello.ack");
            }
            ready = true;
            this.socket = socket;
            this.writer = writer;
            clearTimeout(timer);
            resolve();
          } catch (error) {
            fail(error);
          }
        });
        socket.on("close", () => {
          clearTimeout(timer);
          writer.close();
          const error = new MeshCallError("unavailable", "Connection was lost");
          if (this.socket === socket) {
            this.socket = undefined;
            this.writer = undefined;
            this.rejectAll(error);
          }
          reject(error);
        });
        socket.on("error", (error) => {
          if (this.socket === socket) this.rejectAll(error);
          fail(error);
        });
      });
    } catch (error) {
      await closeSocket(socket);
      throw error;
    } finally {
      if (this.connectingSocket === socket) this.connectingSocket = undefined;
    }
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
      void this.send({ kind: "pong", nonce: frame.nonce }).catch(() => undefined);
      return;
    }
    if (frame.kind === "stream.window") {
      this.handleWindow(frame);
    } else if (frame.kind === "stream.item") {
      this.handleItem(frame);
    } else if (frame.kind === "stream.end") {
      this.handleEnd(frame);
    } else if (frame.kind === "call.result") {
      this.resolvePending(frame);
    } else if (frame.kind === "call.error") {
      this.rejectCall(frame);
    }
  }

  private handleWindow(frame: StreamWindowFrame): void {
    const pending = this.calls.get(frame.call_id);
    if (pending !== undefined) {
      if (frame.direction !== "client" || pending.input === undefined) {
        this.rejectPending(
          frame.call_id,
          new MeshCallError("protocol_error", "Invalid input stream window"),
        );
        return;
      }
      try {
        pending.input.credit.grant(frame.credit);
      } catch (error) {
        this.rejectPending(frame.call_id, error);
      }
      return;
    }
    this.streams.get(frame.call_id)?.fail(
      new MeshCallError("protocol_error", "Unexpected output stream window"),
    );
  }

  private handleItem(frame: StreamItemFrame): void {
    const stream = this.streams.get(frame.call_id);
    if (stream !== undefined) {
      stream.receiveItem(frame);
      return;
    }
    if (this.calls.has(frame.call_id)) {
      this.rejectPending(
        frame.call_id,
        new MeshCallError("protocol_error", "Unexpected output stream item"),
      );
    }
  }

  private handleEnd(frame: StreamEndFrame): void {
    const stream = this.streams.get(frame.call_id);
    if (stream !== undefined) {
      stream.receiveEnd(frame);
      return;
    }
    if (this.calls.has(frame.call_id)) {
      this.rejectPending(
        frame.call_id,
        new MeshCallError("protocol_error", "Unexpected output stream end"),
      );
    }
  }

  private resolvePending(frame: CallResultFrame): void {
    const stream = this.streams.get(frame.call_id);
    if (stream !== undefined) {
      stream.receiveResult();
      return;
    }
    const pending = this.takePending(
      frame.call_id,
      new MeshCallError("cancelled", "Call is complete"),
    );
    pending?.resolve(frame.payload);
  }

  private rejectCall(frame: CallErrorFrame): void {
    const error = new MeshCallError(frame.error.code, frame.error.message, {
      retryable: frame.error.retryable ?? false,
      details: frame.error.details ?? null,
    });
    const stream = this.streams.get(frame.call_id);
    if (stream !== undefined) {
      stream.fail(error);
      return;
    }
    this.rejectPending(frame.call_id, error);
  }

  private cancelPending(callId: string, error: unknown, reason: string): void {
    const pending = this.takePending(callId, error);
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
    const pending = this.takePending(callId, error);
    pending?.reject(error);
  }

  private rejectAll(error: unknown): void {
    for (const callId of [...this.calls.keys()]) {
      this.rejectPending(callId, error);
    }
    for (const stream of [...this.streams.values()]) {
      stream.fail(error);
    }
  }

  private takePending(callId: string, closeError: unknown): PendingCall | undefined {
    const pending = this.calls.get(callId);
    if (pending === undefined) {
      return undefined;
    }
    this.calls.delete(callId);
    pending.closed = true;
    pending.input?.credit.close(closeError);
    if (pending.timer !== undefined) {
      clearTimeout(pending.timer);
    }
    if (pending.signal !== undefined && pending.abortListener !== undefined) {
      pending.signal.removeEventListener("abort", pending.abortListener);
    }
    return pending;
  }

  private validateOptions(options: CallOptions): void {
    if (options.timeoutMs !== undefined && (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0)) {
      throw new RangeError("timeoutMs must be positive");
    }
  }

  private ensureNotAborted(options: CallOptions): void {
    if (options.signal?.aborted === true) {
      throw new MeshCallError("cancelled", "Call was cancelled");
    }
  }

  private async send(frame: Frame): Promise<void> {
    const writer = this.writer;
    if (writer === undefined) {
      throw new MeshCallError("unavailable", "Client is not connected");
    }
    await writer.send(frame);
  }
}

class ServerStreamState<Item> {
  private readonly queue = new AsyncQueue<Item>();
  private allowance = 0;
  private expectedSequence = 0;
  private ended = false;
  private terminal = false;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private signal: AbortSignal | undefined;
  private abortListener: (() => void) | undefined;

  public constructor(
    private readonly callId: string,
    private readonly send: (frame: Frame) => Promise<void>,
    private readonly remove: () => void,
  ) {}

  public grant(credit: number): void {
    this.allowance += credit;
  }

  public arm(options: CallOptions): void {
    if (this.terminal) {
      return;
    }
    if (options.signal?.aborted === true) {
      this.abort(new MeshCallError("cancelled", "Call was cancelled"), "caller_cancelled");
      return;
    }
    if (options.timeoutMs !== undefined) {
      this.timer = setTimeout(() => {
        this.abort(
          new MeshCallError("deadline_exceeded", "Call deadline exceeded"),
          "deadline_exceeded",
        );
      }, options.timeoutMs);
    }
    if (options.signal !== undefined) {
      this.signal = options.signal;
      this.abortListener = () => {
        this.abort(
          new MeshCallError("cancelled", "Call was cancelled"),
          "caller_cancelled",
        );
      };
      options.signal.addEventListener("abort", this.abortListener, {
        once: true,
      });
    }
  }

  public async next(): Promise<IteratorResult<Item>> {
    const result = await this.queue.next();
    if (!result.done && !this.ended && !this.terminal) {
      this.allowance += 1;
      try {
        await this.send({
          kind: "stream.window",
          call_id: this.callId,
          direction: "server",
          credit: 1,
        });
      } catch (error) {
        this.abort(error, "window_send_failed");
        throw error;
      }
    }
    return result;
  }

  public receiveItem(frame: StreamItemFrame): void {
    if (this.terminal || frame.direction !== "server") {
      this.fail(new MeshCallError("protocol_error", "Invalid output stream item"));
      return;
    }
    if (this.ended || this.allowance <= 0) {
      this.fail(
        new MeshCallError("protocol_error", "Output stream exceeded granted credit"),
      );
      return;
    }
    if (frame.sequence !== this.expectedSequence) {
      this.fail(
        new MeshCallError(
          "protocol_error",
          `Expected server sequence ${this.expectedSequence}, got ${frame.sequence}`,
        ),
      );
      return;
    }
    this.allowance -= 1;
    this.expectedSequence += 1;
    this.queue.push(frame.payload as Item);
  }

  public receiveEnd(frame: StreamEndFrame): void {
    if (this.terminal || frame.direction !== "server" || this.ended) {
      this.fail(new MeshCallError("protocol_error", "Invalid output stream end"));
      return;
    }
    this.ended = true;
    this.queue.close();
  }

  public receiveResult(): void {
    if (this.terminal) {
      return;
    }
    if (!this.ended) {
      this.fail(
        new MeshCallError(
          "protocol_error",
          "Server result arrived before output stream end",
        ),
      );
      return;
    }
    this.terminal = true;
    this.cleanup();
    this.remove();
    this.queue.close();
  }

  public fail(error: unknown): void {
    if (this.terminal) {
      return;
    }
    this.terminal = true;
    this.cleanup();
    this.remove();
    this.queue.fail(error);
  }

  public async cancel(reason: string): Promise<void> {
    if (this.terminal) {
      return;
    }
    this.terminal = true;
    this.cleanup();
    this.remove();
    this.queue.close();
    await this.send({
      kind: "call.cancel",
      call_id: this.callId,
      reason,
    }).catch(() => undefined);
  }

  private abort(error: unknown, reason: string): void {
    if (this.terminal) {
      return;
    }
    this.terminal = true;
    this.cleanup();
    this.remove();
    this.queue.fail(error);
    void this.send({
      kind: "call.cancel",
      call_id: this.callId,
      reason,
    }).catch(() => undefined);
  }

  private cleanup(): void {
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    if (this.signal !== undefined && this.abortListener !== undefined) {
      this.signal.removeEventListener("abort", this.abortListener);
      this.signal = undefined;
      this.abortListener = undefined;
    }
  }
}

function newCallId(): string {
  return randomUUID().replaceAll("-", "");
}

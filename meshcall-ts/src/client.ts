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
  type StreamEndFrame,
  type StreamItemFrame,
  type StreamWindowFrame,
  PROTOCOL_VERSION,
} from "./protocol.js";

export const INITIAL_STREAM_CREDIT = 16;

export interface CallOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
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
}

export class MeshCallClient {
  private socket: WebSocket | undefined;
  private connectPromise: Promise<void> | undefined;
  private readonly calls = new Map<string, PendingCall>();
  private readonly streams = new Map<string, ServerStreamState<unknown>>();

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
    this.validateOptions(options);
    this.ensureNotAborted(options);
    await this.connect();

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
    const socket = this.socket;
    this.socket = undefined;
    if (socket !== undefined) {
      socket.removeAllListeners();
      if (socket.readyState === WebSocket.OPEN) {
        await new Promise<void>((resolve) => {
          socket.once("close", () => resolve());
          socket.close();
        });
      }
    }
    this.rejectAll(new MeshCallError("unavailable", "Client is closed"));
  }

  private async openServerStream<Item>(
    service: string,
    method: string,
    request: unknown,
    options: CallOptions,
  ): Promise<ServerStreamState<Item>> {
    this.ensureNotAborted(options);
    await this.connect();

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
    if (options.timeoutMs !== undefined && options.timeoutMs <= 0) {
      throw new RangeError("timeoutMs must be positive");
    }
  }

  private ensureNotAborted(options: CallOptions): void {
    if (options.signal?.aborted === true) {
      throw new MeshCallError("cancelled", "Call was cancelled");
    }
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

class CreditWindow {
  private credit = 0;
  private closed = false;
  private failure: unknown;
  private readonly waiters: Array<{
    resolve: () => void;
    reject: (error: unknown) => void;
  }> = [];

  public acquire(): Promise<void> {
    if (this.credit > 0) {
      this.credit -= 1;
      return Promise.resolve();
    }
    if (this.closed) {
      return Promise.reject(this.failure);
    }
    return new Promise<void>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  public grant(credit: number): void {
    if (!Number.isInteger(credit) || credit <= 0) {
      throw new MeshCallError("protocol_error", "Stream credit must be positive");
    }
    if (this.closed) {
      return;
    }
    this.credit += credit;
    while (this.credit > 0 && this.waiters.length > 0) {
      this.credit -= 1;
      this.waiters.shift()?.resolve();
    }
  }

  public close(error: unknown): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.failure = error;
    for (const waiter of this.waiters.splice(0)) {
      waiter.reject(error);
    }
  }
}

class AsyncQueue<Item> {
  private readonly values: Item[] = [];
  private readonly waiters: Array<{
    resolve: (result: IteratorResult<Item>) => void;
    reject: (error: unknown) => void;
  }> = [];
  private ended = false;
  private failed = false;
  private failure: unknown;

  public push(value: Item): void {
    if (this.ended) {
      return;
    }
    const waiter = this.waiters.shift();
    if (waiter !== undefined) {
      waiter.resolve({ done: false, value });
    } else {
      this.values.push(value);
    }
  }

  public close(): void {
    if (this.ended) {
      return;
    }
    this.ended = true;
    this.flush();
  }

  public fail(error: unknown): void {
    if (this.failed) {
      return;
    }
    this.ended = true;
    this.failed = true;
    this.failure = error;
    this.flush();
  }

  public next(): Promise<IteratorResult<Item>> {
    if (this.values.length > 0) {
      const value = this.values.shift() as Item;
      return Promise.resolve({ done: false, value });
    }
    if (this.failed) {
      return Promise.reject(this.failure);
    }
    if (this.ended) {
      return Promise.resolve({ done: true, value: undefined as never });
    }
    return new Promise<IteratorResult<Item>>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  private flush(): void {
    while (this.values.length > 0 && this.waiters.length > 0) {
      const value = this.values.shift() as Item;
      this.waiters.shift()?.resolve({ done: false, value });
    }
    if (this.values.length > 0 || this.waiters.length === 0) {
      return;
    }
    const waiters = this.waiters.splice(0);
    if (this.failed) {
      for (const waiter of waiters) {
        waiter.reject(this.failure);
      }
    } else if (this.ended) {
      for (const waiter of waiters) {
        waiter.resolve({ done: true, value: undefined as never });
      }
    }
  }
}

function newCallId(): string {
  return randomUUID().replaceAll("-", "");
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

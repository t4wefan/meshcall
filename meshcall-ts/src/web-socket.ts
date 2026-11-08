// WebSocket transport for `meshcall/web` (docs/protocol.md → Frames). No Node
// imports: it uses the platform's `WebSocket`, so browsers get every call
// shape, duplex included. Browsers cannot set headers on a WebSocket, so the
// credentials travel in the `hello` frame's `authorization` field.

import type { CallOptions, DuplexCallOptions } from "./client.js";
import {
  CallCancelledError, DeadlineExceededError, MeshCallError, ProtocolError, ResourceExhaustedError,
  UnauthenticatedError, UnavailableError,
} from "./errors.js";
import { AsyncQueue, CreditWindow, INITIAL_STREAM_CREDIT } from "./flow.js";
import type { RpcLogger } from "./log.js";
import { clientDeadlineGraceMs, retryUnary, validateMetadata, type RetryPolicy } from "./policy.js";
import type { Metadata } from "./protocol.js";
import { mergeCallOptions, type MeshCallRpc, type RpcDuplex, type RpcStream } from "./rpc.js";
import { checkReceived } from "./validation.js";

const PROTOCOL = "meshcall/1";
const DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024;
/** WebSocket close status for credentials in `hello` that fail. */
const CLOSE_UNAUTHENTICATED = 4401;

export interface WebSocketTransportOptions {
  /** Sent as `hello.authorization` (`Bearer …` or `Basic …`). */
  readonly authorization?: string;
  readonly retry?: RetryPolicy | false;
  /** Extra time past `timeoutMs` before failing locally. Defaults to `max(1s, 10%)`. */
  readonly deadlineGraceMs?: number;
  /** Keepalive ping cadence; `null` disables. Default 15 s. */
  readonly pingIntervalMs?: number | null;
  readonly pingTimeoutMs?: number;
  readonly handshakeTimeoutMs?: number;
  readonly maxFrameSize?: number;
  readonly logger?: RpcLogger;
  /** Defaults to the global `WebSocket`. */
  readonly WebSocket?: typeof WebSocket;
}

type Frame = Record<string, unknown> & { kind: string; call_id?: string };

function randomId(): string {
  const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
  if (c?.randomUUID !== undefined) return c.randomUUID().replaceAll("-", "");
  return Array.from({ length: 32 }, () => Math.floor(Math.random() * 16).toString(16)).join("");
}

function errorFrom(frame: Frame): MeshCallError {
  const error = frame.error as { code?: string; message?: string; retryable?: boolean; details?: Record<string, unknown> } | undefined;
  return MeshCallError.fromCode(error?.code ?? "internal", error?.message ?? "", {
    retryable: error?.retryable === true, details: error?.details ?? null,
  });
}

/** One WebSocket connection: the handshake, keepalive and per-call dispatch. */
class Connection {
  public readonly calls = new Map<string, WsCall>();
  public readonly ready: Promise<void>;
  private readonly socket: WebSocket;
  private closed = false;
  private ping: ReturnType<typeof setInterval> | undefined;
  private pong: ReturnType<typeof setTimeout> | undefined;
  private nonce: string | undefined;

  public constructor(url: string, private readonly options: WebSocketTransportOptions, private readonly onGone: () => void) {
    const Socket = options.WebSocket ?? globalThis.WebSocket;
    if (Socket === undefined) throw new UnavailableError("No WebSocket implementation is available", { retryable: false });
    this.socket = new Socket(url);
    this.ready = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new DeadlineExceededError("Router handshake timed out", { details: { source: "client" } }));
        this.close();
      }, options.handshakeTimeoutMs ?? 10_000);
      this.socket.onopen = () => {
        this.send({
          kind: "hello", protocol: PROTOCOL, role: "client", peer_id: randomId(),
          ...(options.authorization === undefined ? {} : { authorization: options.authorization }),
        });
      };
      this.socket.onmessage = (event: MessageEvent) => {
        let frame: Frame;
        try {
          frame = JSON.parse(String(event.data)) as Frame;
          if (typeof frame?.kind !== "string") throw new Error("frame without a kind");
        } catch {
          this.close();
          return;
        }
        if (frame.kind === "hello.ack") {
          clearTimeout(timer);
          if (frame.protocol !== PROTOCOL) {
            reject(new ProtocolError("Unsupported protocol " + String(frame.protocol)));
            this.close();
            return;
          }
          this.startPing();
          resolve();
          return;
        }
        this.receive(frame);
      };
      this.socket.onclose = (event: CloseEvent) => {
        clearTimeout(timer);
        const error = event.code === CLOSE_UNAUTHENTICATED
          ? new UnauthenticatedError("The Router refused the credentials: " + event.reason)
          : new UnavailableError("Connection was lost", { retryable: true });
        reject(error);
        this.gone(error);
      };
      this.socket.onerror = () => undefined; // onclose follows with the details
    });
    this.ready.catch(() => undefined);
  }

  public send(frame: Frame): void {
    const data = JSON.stringify(frame);
    if (new TextEncoder().encode(data).length > (this.options.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE)) {
      throw new ResourceExhaustedError("Frame exceeds the maximum frame size", {
        retryable: false, details: { source: "client", reason: "frame_too_large" },
      });
    }
    if (this.closed || this.socket.readyState !== 1) throw new UnavailableError("Connection was lost", { retryable: true });
    this.socket.send(data);
  }

  public get isOpen(): boolean { return !this.closed; }

  public close(): void {
    if (this.socket.readyState <= 1) this.socket.close(1000);
    this.gone(new UnavailableError("Connection closed", { retryable: true }));
  }

  private receive(frame: Frame): void {
    if (frame.kind === "ping") {
      try { this.send({ kind: "pong", nonce: frame.nonce }); } catch { /* closing */ }
      return;
    }
    if (frame.kind === "pong") {
      if (frame.nonce === this.nonce) {
        clearTimeout(this.pong);
        this.nonce = undefined;
      }
      return;
    }
    if (typeof frame.call_id === "string") this.calls.get(frame.call_id)?.receive(frame);
  }

  private startPing(): void {
    const interval = this.options.pingIntervalMs === undefined ? 15_000 : this.options.pingIntervalMs;
    if (interval === null) return;
    this.ping = setInterval(() => {
      if (this.nonce !== undefined) return;
      this.nonce = randomId();
      try {
        this.send({ kind: "ping", nonce: this.nonce });
      } catch {
        return;
      }
      this.pong = setTimeout(() => this.close(), this.options.pingTimeoutMs ?? 10_000);
    }, interval);
  }

  private gone(error: MeshCallError): void {
    if (this.closed) return;
    this.closed = true;
    clearInterval(this.ping);
    clearTimeout(this.pong);
    for (const call of [...this.calls.values()]) call.fail(error);
    this.onGone();
  }
}

type Shape = "unary" | "server_stream" | "client_stream" | "duplex";

/** One call's state: its terminal frame, output items, input credit and timers. */
class WsCall {
  public readonly id = randomId();
  public trailers: Readonly<Metadata> = {};
  public readonly result: Promise<unknown>;
  private settle!: { resolve: (payload: unknown) => void; reject: (error: unknown) => void };
  private readonly output = new AsyncQueue<unknown>();
  private readonly inputCredit = new CreditWindow();
  private inputSequence = 0;
  private consumed = 0;
  private terminal = false;
  private deadline: ReturnType<typeof setTimeout> | undefined;
  private idle: ReturnType<typeof setTimeout> | undefined;
  private readonly onAbort = () => this.cancel("cancelled", new CallCancelledError("Call was cancelled"));

  public constructor(
    private readonly connection: Connection,
    private readonly shape: Shape,
    private readonly options: CallOptions,
    budgetMs: number | undefined,
  ) {
    this.result = new Promise((resolve, reject) => { this.settle = { resolve, reject }; });
    this.result.catch(() => undefined);
    connection.calls.set(this.id, this);
    if (budgetMs !== undefined) {
      this.deadline = setTimeout(() => this.cancel("deadline_exceeded",
        new DeadlineExceededError("Call deadline exceeded", { details: { source: "client" } })), budgetMs);
    }
    if (options.signal?.aborted === true) queueMicrotask(this.onAbort);
    options.signal?.addEventListener("abort", this.onAbort, { once: true });
  }

  public open(service: string, method: string, request: unknown): void {
    if (this.terminal) return; // cancelled before it was sent
    const o = this.options;
    this.send({
      kind: "call.open", call_id: this.id, service, method, payload: request ?? null,
      ...(o.timeoutMs === undefined ? {} : { timeout_ms: Math.max(1, Math.round(o.timeoutMs)) }),
      ...(o.idleTimeoutMs === undefined ? {} : { idle_timeout_ms: Math.max(1, Math.round(o.idleTimeoutMs)) }),
      ...(o.metadata === undefined ? {} : { metadata: validateMetadata(o.metadata) }),
      ...(o.idempotencyKey === undefined ? {} : { idempotency_key: o.idempotencyKey }),
      ...(o.instance === undefined ? {} : { instance_id: o.instance }),
    });
    if (this.shape === "server_stream" || this.shape === "duplex") {
      this.send({ kind: "stream.window", call_id: this.id, direction: "server", credit: INITIAL_STREAM_CREDIT });
      this.armIdle();
    }
  }

  public receive(frame: Frame): void {
    if (this.terminal) return;
    switch (frame.kind) {
      case "stream.item":
        if (frame.direction !== "server") return;
        try {
          checkReceived(this.options.itemSchema, frame.payload, "output_item");
        } catch (error) {
          this.cancel("invalid_output_item", error as MeshCallError);
          return;
        }
        this.armIdle();
        this.output.push(frame.payload);
        return;
      case "stream.window":
        if (frame.direction === "client" && typeof frame.credit === "number") this.inputCredit.grant(frame.credit);
        return;
      case "stream.end":
        return; // the terminal frame follows
      case "call.result":
        this.finish();
        this.trailers = (frame.metadata as Metadata | null | undefined) ?? {};
        this.output.close();
        this.settle.resolve(frame.payload);
        return;
      case "call.error":
        this.trailers = (frame.metadata as Metadata | null | undefined) ?? {};
        this.fail(errorFrom(frame));
    }
  }

  /** Consumer side of the output: grant more credit as items are read. */
  public async next(): Promise<IteratorResult<unknown>> {
    const item = await this.output.next();
    if (!item.done && !this.terminal && ++this.consumed >= INITIAL_STREAM_CREDIT / 2) {
      const credit = this.consumed;
      this.consumed = 0;
      try { this.send({ kind: "stream.window", call_id: this.id, direction: "server", credit }); } catch { /* reported by the call */ }
    }
    return item;
  }

  public async sendInput(item: unknown): Promise<void> {
    await this.inputCredit.acquire();
    this.send({ kind: "stream.item", call_id: this.id, direction: "client", sequence: this.inputSequence++, payload: item ?? null });
  }

  public closeInput(): void {
    if (!this.terminal) this.send({ kind: "stream.end", call_id: this.id, direction: "client" });
  }

  public fail(error: unknown): void {
    if (this.terminal) return;
    this.finish();
    this.output.fail(error);
    this.inputCredit.close(error);
    this.settle.reject(error);
  }

  public cancel(reason: string, error: MeshCallError): void {
    if (this.terminal) return;
    try { this.send({ kind: "call.cancel", call_id: this.id, reason }); } catch { /* the connection is gone */ }
    this.fail(error);
  }

  private send(frame: Frame): void {
    try {
      this.connection.send(frame);
    } catch (error) {
      this.fail(error);
      throw error;
    }
  }

  private armIdle(): void {
    if (this.options.idleTimeoutMs === undefined) return;
    clearTimeout(this.idle);
    this.idle = setTimeout(() => this.cancel("deadline_exceeded", new DeadlineExceededError(
      "No stream item within the idle timeout", { details: { source: "client", reason: "idle" } },
    )), this.options.idleTimeoutMs);
  }

  private finish(): void {
    this.terminal = true;
    clearTimeout(this.deadline);
    clearTimeout(this.idle);
    this.options.signal?.removeEventListener("abort", this.onAbort);
    this.connection.calls.delete(this.id);
  }
}

/** Output items of a server stream or duplex call. */
class WsStream<Input, Output> implements RpcDuplex<Input, Output> {
  private call: WsCall | undefined;
  private failure: unknown;

  public constructor(private readonly opened: Promise<WsCall>) {
    opened.then((call) => { this.call = call; }, (error: unknown) => { this.failure = error; });
  }

  public get trailers(): Readonly<Metadata> { return this.call?.trailers ?? {}; }

  public [Symbol.asyncIterator](): AsyncIterableIterator<Output> { return this; }

  public async next(): Promise<IteratorResult<Output>> {
    return (await this.opened).next() as Promise<IteratorResult<Output>>;
  }

  public async send(item: Input): Promise<void> {
    await (await this.opened).sendInput(item);
  }

  public async closeSend(): Promise<void> {
    (await this.opened).closeInput();
  }

  public async cancel(reason = "client_closed"): Promise<void> {
    const call = await this.opened.catch(() => undefined);
    call?.cancel(reason, new CallCancelledError("Call was cancelled"));
  }

  public async return(): Promise<IteratorResult<Output>> {
    await this.cancel("iterator_closed");
    return { done: true, value: undefined };
  }

  public async [Symbol.asyncDispose](): Promise<void> {
    await this.cancel("iterator_closed");
  }
}

/**
 * @internal WebSocket transport used by `meshcall/web`'s `Mesh` for `ws://`
 * and `wss://` Routers. Connects lazily, reconnects on the next call after a
 * loss, and retries unary calls under the retry policy.
 */
export class WebSocketTransport implements MeshCallRpc {
  private root: WebSocketTransport = this;
  private connection: Connection | undefined;
  private connecting: Promise<Connection> | undefined;
  private cursor = 0;
  private defaultCallOptions: CallOptions = {};

  public constructor(private readonly urls: ReadonlyArray<string>, private readonly options: WebSocketTransportOptions = {}) {
    if (urls.length === 0) throw new RangeError("WebSocketTransport needs at least one URL");
  }

  public withOptions(options: CallOptions): this {
    const clone = Object.create(WebSocketTransport.prototype) as this;
    Object.assign(clone, {
      urls: this.urls, options: this.options, root: this.root,
      defaultCallOptions: mergeCallOptions(this.defaultCallOptions, options),
    });
    return clone;
  }

  public async connect(): Promise<void> {
    await this.root.ensureConnection();
  }

  public async close(): Promise<void> {
    if (this.root !== this) return;
    const connection = this.connection ?? await this.connecting?.catch(() => undefined);
    this.connection = undefined;
    this.connecting = undefined;
    connection?.close();
  }

  public async [Symbol.asyncDispose](): Promise<void> { await this.close(); }

  public unary<Request, Response>(service: string, method: string, request: Request, options: CallOptions = {}): Promise<Response> {
    const merged = mergeCallOptions(this.defaultCallOptions, options);
    return retryUnary(this.options.retry, merged, async (timeoutMs) => {
      const call = await this.start("unary", service, method, request, timeoutMs === undefined ? merged : { ...merged, timeoutMs });
      const payload = await call.result;
      checkReceived(merged.responseSchema, payload, "response");
      return payload as Response;
    });
  }

  public serverStream<Request, Item>(service: string, method: string, request: Request, options: CallOptions = {}): RpcStream<Item> {
    const merged = mergeCallOptions(this.defaultCallOptions, options);
    return new WsStream<never, Item>(this.start("server_stream", service, method, request, merged));
  }

  public async clientStream<Request, Item, Response>(
    service: string, method: string, request: Request, items: AsyncIterable<Item>, options: CallOptions = {},
  ): Promise<Response> {
    const merged = mergeCallOptions(this.defaultCallOptions, options);
    const call = await this.start("client_stream", service, method, request, merged);
    try {
      for await (const item of items) await call.sendInput(item);
      call.closeInput();
    } catch (error) {
      // A send that failed because the call ended: its terminal error says why.
      // Anything else (the caller's iterable threw) ends the call here.
      call.cancel("client_error", error instanceof MeshCallError ? error : new CallCancelledError("Input stream failed"));
      await call.result.catch((terminal: unknown) => { if (terminal !== error) throw terminal; });
      throw error;
    }
    const payload = await call.result;
    checkReceived(merged.responseSchema, payload, "response");
    return payload as Response;
  }

  public duplex<Request, InputItem, OutputItem>(
    service: string, method: string, request: Request, options: DuplexCallOptions = {},
  ): RpcDuplex<InputItem, OutputItem> {
    const merged = mergeCallOptions(this.defaultCallOptions, options);
    return new WsStream<InputItem, OutputItem>(this.start("duplex", service, method, request, merged));
  }

  private async start(shape: Shape, service: string, method: string, request: unknown, options: CallOptions): Promise<WsCall> {
    const budget = options.timeoutMs === undefined
      ? undefined : options.timeoutMs + (this.options.deadlineGraceMs ?? clientDeadlineGraceMs(options.timeoutMs));
    const started = performance.now();
    const connecting = this.root.ensureConnection();
    const connection = budget === undefined ? await connecting : await Promise.race([
      connecting,
      new Promise<never>((_, reject) => setTimeout(() => reject(new DeadlineExceededError(
        "Call deadline exceeded while connecting", { details: { source: "client" } })), budget)),
    ]);
    const call = new WsCall(connection, shape, options, budget === undefined ? undefined : Math.max(1, budget - (performance.now() - started)));
    call.open(service, method, request);
    return call;
  }

  private ensureConnection(): Promise<Connection> {
    if (this.connection?.isOpen === true) return Promise.resolve(this.connection);
    this.connecting ??= this.dial().finally(() => { this.connecting = undefined; });
    return this.connecting;
  }

  /** Try each URL once, starting with the last one that worked. */
  private async dial(): Promise<Connection> {
    let last: unknown;
    for (let i = 0; i < this.urls.length; i += 1) {
      const index = (this.cursor + i) % this.urls.length;
      const connection = new Connection(this.urls[index]!, this.options, () => {
        if (this.connection === connection) this.connection = undefined;
      });
      try {
        await connection.ready;
        this.cursor = index;
        this.connection = connection;
        return connection;
      } catch (error) {
        last = error;
        if (error instanceof UnauthenticatedError) throw error; // the same credentials fail everywhere
      }
    }
    throw last;
  }
}

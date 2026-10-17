import { randomUUID } from "node:crypto";

import WebSocket, { type RawData } from "ws";

import type { JsonSchema } from "./contract.js";
import { closeSocket, createSocket, type WebSocketEndpoint } from "./endpoint.js";
import { CallCancelledError, DeadlineExceededError, MeshCallError, ProtocolError, UnavailableError } from "./errors.js";
import { AsyncQueue, CreditWindow, INITIAL_STREAM_CREDIT } from "./flow.js";
import {
  clientDeadlineGraceMs,
  DEFAULT_PING_INTERVAL_MS,
  DEFAULT_PING_TIMEOUT_MS,
  DEFAULT_RETRY,
  Liveness,
  reconnectDelays,
  type ReconnectPolicy,
  retryDelayMs,
  type RetryPolicy,
  validateMetadata,
} from "./liveness.js";
import { ConsoleRpcLogger, type RpcLogger } from "./logging.js";
import { mergeCallOptions, type MeshCallRpc, type RpcStream } from "./rpc.js";
import { MeshCallSubscription } from "./subscription.js";
import { DEFAULT_MAX_FRAME_SIZE, FrameWriter, isFrameTooLarge } from "./transport.js";
import { checkReceived } from "./validation.js";
import {
  type CallCancelFrame,
  type CallErrorFrame,
  type CallOpenFrame,
  type CallResultFrame,
  decodeFrame,
  type Frame,
  FrameDecodeError,
  type Metadata,
  type StreamEndFrame,
  type StreamItemFrame,
  type StreamWindowFrame,
  PROTOCOL_VERSION,
} from "./protocol.js";

import type { RouterCredentials } from "./router-auth.js";

export { INITIAL_STREAM_CREDIT } from "./flow.js";
export { mergeCallOptions } from "./rpc.js";
export { MeshCallSubscription } from "./subscription.js";

export interface CallOptions {
  /** Total budget for the call, including connecting. */
  timeoutMs?: number;
  /** Longest allowed gap between consecutive stream items. */
  idleTimeoutMs?: number;
  /** Request metadata delivered to the service handler. */
  metadata?: Metadata;
  /**
   * Lets the Router join or replay duplicate unary calls and lets the retry
   * policy re-send a non-idempotent method safely. 1–128 bytes.
   */
  idempotencyKey?: string;
  /** The method's own semantics; generated clients set this from the contract. */
  idempotent?: boolean;
  /** Address one service instance by id; required for targeted services. */
  instance?: string;
  /**
   * Contract schemas the result / each output item must satisfy; generated
   * clients set them so a server drifting from the client's contract fails
   * as `protocol_error` here instead of surfacing as a bad value.
   */
  responseSchema?: JsonSchema;
  itemSchema?: JsonSchema;
  signal?: AbortSignal;
}

/**
 * Options for a duplex call: it is never retried or deduplicated, so it
 * takes no idempotency key and no `idempotent` flag, and has no result.
 */
export type DuplexCallOptions = Omit<CallOptions, "idempotencyKey" | "idempotent" | "responseSchema">;

/**
 * Options for a subscription (docs/protocol.md → Subscriptions): `cursor` and
 * `resume` come from the contract; generated clients fill them in.
 */
export type SubscriptionOptions = Omit<CallOptions, "idempotent" | "responseSchema"> & {
  readonly cursor?: string;
  readonly resume?: string;
};

export interface MeshCallClientOptions {
  readonly auth?: RouterCredentials;
  readonly maxFrameSize?: number;
  readonly handshakeTimeoutMs?: number;
  /**
   * How long past `timeoutMs` to wait for the peer's own deadline error
   * before failing locally. Defaults to `max(1s, 10%)`.
   */
  readonly deadlineGraceMs?: number;
  /** Keepalive ping cadence; `null` disables pinging. */
  readonly pingIntervalMs?: number | null;
  readonly pingTimeoutMs?: number;
  /**
   * How a lost connection is re-established. In-flight calls fail with a
   * retryable `unavailable`; the next call reconnects lazily with backoff,
   * waiting at most its own `timeoutMs`.
   */
  readonly reconnect?: ReconnectPolicy;
  /** Wrap every outgoing call; the first interceptor is outermost. */
  readonly interceptors?: ReadonlyArray<ClientInterceptor>;
  /** Built-in retry for unary calls; `false` disables. */
  readonly retry?: RetryPolicy | false;
  /** Where the client reports what callers cannot see, such as a resubscription. */
  readonly logger?: RpcLogger;
}

/**
 * What a client interceptor sees for one outgoing call. `options` may be
 * replaced to add metadata or adjust timeouts before the call is sent.
 * `trailers` is filled for unary and client-stream calls once `next`
 * resolves; streams expose trailers on the returned object.
 */
export interface ClientCallInfo {
  readonly service: string;
  readonly method: string;
  readonly stream: "unary" | "server_stream" | "client_stream" | "duplex";
  options: CallOptions;
  trailers: Readonly<Metadata>;
}

/**
 * Runs the rest of the client chain and resolves with what the call returns:
 * the response for unary and client-stream calls, or the `MeshCallServerStream`
 * (`MeshCallDuplexCall`) for server streams (duplex calls). It may be called
 * more than once to retry.
 */
export type ClientCallNext = (info: ClientCallInfo) => Promise<unknown>;

export type ClientInterceptor = (info: ClientCallInfo, next: ClientCallNext) => Promise<unknown>;

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
  implements RpcStream<Item>
{
  private state: ServerStreamState<Item> | undefined;

  public constructor(private readonly open: Promise<ServerStreamState<Item>>) {
    open.then((state) => { this.state = state; }, () => undefined);
  }

  /** @internal */
  public get opened(): Promise<ServerStreamState<Item>> {
    return this.open;
  }

  public [Symbol.asyncIterator](): AsyncIterableIterator<Item> {
    return this;
  }

  public async next(): Promise<IteratorResult<Item>> {
    return (await this.open).next();
  }

  /** Server trailers; populated once the stream has terminated. */
  public get trailers(): Readonly<Metadata> {
    return this.state?.trailers ?? {};
  }

  public async cancel(reason = "client_closed"): Promise<void> {
    await (await this.open).cancel(reason);
  }

  public async return(): Promise<IteratorResult<Item>> {
    await this.cancel("iterator_closed");
    return { done: true, value: undefined };
  }

  public async [Symbol.asyncDispose](): Promise<void> {
    await this.cancel("iterator_closed");
  }
}

/**
 * A duplex call: `send` input items while iterating output items. The output
 * ends when the call ends successfully; a failure is thrown from the
 * iteration. Read output concurrently with sending: a caller that only sends
 * stalls once the server's output credit runs out.
 */
export class MeshCallDuplexCall<Input, Output> extends MeshCallServerStream<Output> {
  public async send(item: Input): Promise<void> {
    await (await this.opened).sendInput(item);
  }

  /** Half-close the input; the server's iteration of it then ends. */
  public async closeSend(): Promise<void> {
    await (await this.opened).closeInput();
  }
}

export class MeshCallClient implements MeshCallRpc {
  private root: MeshCallClient;
  private ownsRoot = true;
  private defaultCallOptions: CallOptions = {};
  private socket: WebSocket | undefined;
  private connectingSocket: WebSocket | undefined;
  private writer: FrameWriter | undefined;
  private liveness: Liveness | undefined;
  private connectPromise: Promise<void> | undefined;
  private everConnected = false;
  private closed = false;
  private readonly calls = new Map<string, PendingCall>();
  private readonly streams = new Map<string, ServerStreamState<unknown>>();
  /** Trailers of settled non-stream calls, consumed once by the call entry point. */
  private readonly trailers = new Map<string, Readonly<Metadata>>();

  private readonly endpoints: ReadonlyArray<WebSocketEndpoint>;
  /** Index of the endpoint in use (or to try first); advances on failure. */
  private cursor = 0;

  /**
   * `url` may be a list of endpoints: they are tried in order starting from
   * the last one that worked, so several independent Routers give failover
   * without any Router-side coordination.
   */
  public constructor(
    url: WebSocketEndpoint | ReadonlyArray<WebSocketEndpoint>,
    private readonly peerId: string = randomUUID().replaceAll("-", ""),
    private readonly options: MeshCallClientOptions = {},
  ) {
    this.root = this;
    this.endpoints = Array.isArray(url) ? [...(url as ReadonlyArray<WebSocketEndpoint>)] : [url as WebSocketEndpoint];
    if (this.endpoints.length === 0) throw new RangeError("At least one endpoint is required");
    for (const value of [options.maxFrameSize, options.handshakeTimeoutMs, options.pingTimeoutMs]) {
      if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
        throw new RangeError("Limits must be positive integers");
      }
    }
    for (const value of [options.deadlineGraceMs, options.pingIntervalMs]) {
      if (value !== undefined && value !== null && (!Number.isFinite(value) || value < 0)) {
        throw new RangeError("Durations must be non-negative");
      }
    }
    reconnectDelays(options.reconnect ?? {}).next(); // validate eagerly
  }

  /**
   * Return a shallow view sharing this client's connection with overridden per-call options.
   */
  public withOptions(options: CallOptions): this {
    const clone = Object.create(MeshCallClient.prototype) as this;
    clone.root = this.root;
    clone.ownsRoot = false;
    clone.defaultCallOptions = mergeCallOptions(this.defaultCallOptions, options);
    Object.assign(clone, {
      endpoints: this.root.endpoints,
      peerId: this.root.peerId,
      options: this.root.options,
    });
    return clone;
  }

  /**
   * Connect if not connected. After a loss this retries with backoff per the
   * reconnect policy; concurrent callers share one attempt sequence.
   */
  public async connect(): Promise<void> {
    if (this.root !== this) {
      return this.root.connect();
    }
    if (this.socket?.readyState === WebSocket.OPEN) {
      return;
    }
    if (this.connectPromise !== undefined) {
      return this.connectPromise;
    }
    this.closed = false;
    this.connectPromise = this.openWithRetry();
    try {
      await this.connectPromise;
    } finally {
      this.connectPromise = undefined;
    }
  }

  /** The endpoint of the current (or most recently used) connection. */
  public get endpoint(): WebSocketEndpoint {
    if (this.root !== this) return this.root.endpoint;
    return this.endpoints[this.cursor % this.endpoints.length] as WebSocketEndpoint;
  }

  private async openWithRetry(): Promise<void> {
    const policy = this.options.reconnect ?? {};
    const retry = (policy.enabled ?? true) && this.everConnected;
    const delays = retry ? reconnectDelays(policy) : undefined;
    for (;;) {
      try {
        await this.openAnyEndpoint();
        this.everConnected = true;
        return;
      } catch (error) {
        const next = delays?.next();
        if (next === undefined || next.done === true || this.closed) {
          throw error instanceof MeshCallError ? error
            : new UnavailableError("Could not connect: " + String(error), { retryable: true });
        }
        await new Promise((resolve) => setTimeout(resolve, next.value));
      }
    }
  }

  /** `connect()` bounded by the caller's local budget. */
  private async connectWithin(options: CallOptions, startedAt: number): Promise<void> {
    const budget = this.localBudgetMs(options, startedAt);
    if (budget === undefined) {
      await this.connect();
      return;
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    const expired = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(localDeadlineError()), budget);
    });
    try {
      await Promise.race([this.connect(), expired]);
    } finally {
      clearTimeout(timer);
    }
  }

  public async unary<Request, Response>(
    service: string,
    method: string,
    request: Request,
    options: CallOptions = {},
  ): Promise<Response> {
    const merged = mergeCallOptions(this.defaultCallOptions, options);
    if (this.root !== this) {
      return this.root.unary(service, method, request, merged);
    }
    this.validateOptions(merged);
    const info: ClientCallInfo = { service, method, stream: "unary", options: merged, trailers: {} };
    return this.runChain(info, async (info) => {
      this.ensureNotAborted(info.options);
      const startedAt = performance.now();
      await this.connectWithin(info.options, startedAt);
      this.ensureNotAborted(info.options);

      const callId = newCallId();
      const { result } = this.registerCall<Response>(callId, info.options, startedAt);
      try {
        await this.send(this.openFrame(callId, info.service, info.method, request, info.options));
      } catch (error) {
        this.rejectPending(callId, error);
      }
      try {
        const payload = await result;
        checkReceived(info.options.responseSchema, payload, "response");
        return payload;
      } finally {
        info.trailers = this.trailerOf(callId);
      }
    }) as Promise<Response>;
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
    const merged = mergeCallOptions(this.defaultCallOptions, options);
    if (this.root !== this) {
      return this.root.clientStream(service, method, request, items, merged);
    }
    this.validateOptions(merged);
    const info: ClientCallInfo = { service, method, stream: "client_stream", options: merged, trailers: {} };
    return this.runChain(info, async (info) => {
      this.ensureNotAborted(info.options);
      const startedAt = performance.now();
      await this.connectWithin(info.options, startedAt);
      this.ensureNotAborted(info.options);

      const callId = newCallId();
      const input: InputState = {
        credit: new CreditWindow(),
        sequence: 0,
        closed: false,
      };
      const { pending, result } = this.registerCall<Response>(
        callId,
        info.options,
        startedAt,
        input,
      );
      try {
        await this.send(this.openFrame(callId, info.service, info.method, request, info.options));
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
        const payload = await result;
        checkReceived(info.options.responseSchema, payload, "response");
        return payload;
      } finally {
        info.trailers = this.trailerOf(callId);
        if (!pumpSettled) {
          stop.abort();
        }
      }
    }) as Promise<Response>;
  }

  /** Returns a lazy async iterator for server-produced items. */
  public serverStream<Request, Item>(
    service: string,
    method: string,
    request: Request,
    options: CallOptions = {},
  ): MeshCallServerStream<Item> {
    const merged = mergeCallOptions(this.defaultCallOptions, options);
    if (this.root !== this) {
      return this.root.serverStream(service, method, request, merged);
    }
    this.validateOptions(merged);
    const info: ClientCallInfo = { service, method, stream: "server_stream", options: merged, trailers: {} };
    const innermost = async (info: ClientCallInfo): Promise<MeshCallServerStream<Item>> =>
      new MeshCallServerStream<Item>(
        this.openServerStream(info.service, info.method, request, info.options),
      );
    if (this.options.interceptors === undefined || this.options.interceptors.length === 0) {
      return new MeshCallServerStream(this.openServerStream(service, method, request, merged));
    }
    return new MeshCallServerStream(this.runChain(info, innermost).then((stream) => {
      if (!(stream instanceof MeshCallServerStream)) {
        throw new TypeError("Client interceptors must return the stream produced by next");
      }
      return (stream as MeshCallServerStream<Item>).opened;
    }));
  }

  /**
   * Opens a subscription (docs/protocol.md → Subscriptions): a server stream
   * reopened after retryable failures, resuming after the last item's
   * `cursor` via the `resume` request property. No default timeouts.
   */
  public subscribe<Request extends object, Item>(
    service: string,
    method: string,
    request: Request,
    options: SubscriptionOptions = {},
  ): MeshCallSubscription<Item> {
    const { idempotent: _i, responseSchema: _r, ...baseSub } = this.defaultCallOptions;
    const merged = mergeCallOptions(baseSub, options);
    if (this.root !== this) {
      return this.root.subscribe(service, method, request, merged);
    }
    const { cursor, resume, ...callOptions } = merged;
    this.validateOptions(callOptions);
    return new MeshCallSubscription<Item>(
      (payload) => this.serverStream<Record<string, unknown>, Item>(service, method, payload, callOptions),
      request as Record<string, unknown>,
      { ...(cursor === undefined ? {} : { cursor }), ...(resume === undefined ? {} : { resume }) },
      this.options.reconnect ?? {},
      this.options.logger ?? new ConsoleRpcLogger(),
      service + "." + method,
    );
  }

  /**
   * Opens a duplex call (docs/protocol.md → Duplex calls). Never retried; an
   * idempotency key is refused by the server.
   */
  public duplex<Request, Input, Output>(
    service: string,
    method: string,
    request: Request,
    options: DuplexCallOptions = {},
  ): MeshCallDuplexCall<Input, Output> {
    const { idempotencyKey: _k, idempotent: _i, responseSchema: _r, ...baseDuplex } = this.defaultCallOptions;
    const merged = mergeCallOptions(baseDuplex, options);
    if (this.root !== this) {
      return this.root.duplex(service, method, request, merged);
    }
    this.validateOptions(merged);
    const info: ClientCallInfo = { service, method, stream: "duplex", options: merged, trailers: {} };
    const innermost = async (info: ClientCallInfo): Promise<MeshCallDuplexCall<Input, Output>> =>
      new MeshCallDuplexCall<Input, Output>(
        this.openServerStream(info.service, info.method, request, info.options, true),
      );
    if (this.options.interceptors === undefined || this.options.interceptors.length === 0) {
      return new MeshCallDuplexCall(this.openServerStream(service, method, request, merged, true));
    }
    return new MeshCallDuplexCall(this.runChain(info, innermost).then((call) => {
      if (!(call instanceof MeshCallDuplexCall)) {
        throw new TypeError("Client interceptors must return the call produced by next");
      }
      return (call as MeshCallDuplexCall<Input, Output>).opened;
    }));
  }

  /**
   * Invoke the interceptor chain; the first interceptor is outermost. The
   * built-in retry policy wraps everything so user interceptors see each
   * attempt.
   */
  private runChain(info: ClientCallInfo, innermost: ClientCallNext): Promise<unknown> {
    let next = innermost;
    for (const interceptor of [...(this.options.interceptors ?? [])].reverse()) {
      const inner = next;
      next = (info) => interceptor(info, inner);
    }
    if (this.options.retry !== false && info.stream === "unary") {
      const policy = { ...DEFAULT_RETRY, ...(this.options.retry ?? {}) };
      if (policy.enabled) {
        const inner = next;
        next = (info) => retryCall(policy, info, inner);
      }
    }
    return next(info);
  }

  private trailerOf(callId: string): Readonly<Metadata> {
    const trailers = this.trailers.get(callId) ?? {};
    this.trailers.delete(callId);
    return trailers;
  }

  public async close(): Promise<void> {
    if (!this.ownsRoot) return;
    this.closed = true;
    const sockets = new Set([this.socket, this.connectingSocket]);
    this.socket = undefined;
    this.connectingSocket = undefined;
    this.liveness?.stop();
    this.liveness = undefined;
    this.writer?.close();
    this.writer = undefined;
    this.rejectAll(new UnavailableError("Client is closed"));
    await Promise.all([...sockets].map((socket) => socket === undefined ? undefined : closeSocket(socket)));
  }

  public async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  private async openServerStream<Item>(
    service: string,
    method: string,
    request: unknown,
    options: CallOptions,
    duplex = false,
  ): Promise<ServerStreamState<Item>> {
    this.ensureNotAborted(options);
    const startedAt = performance.now();
    await this.connectWithin(options, startedAt);
    this.ensureNotAborted(options);

    const callId = newCallId();
    const stream = new ServerStreamState<Item>(
      callId,
      (frame) => this.send(frame),
      () => this.streams.delete(callId),
      options.itemSchema,
      duplex,
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
      stream.arm(options, this.localBudgetMs(options, startedAt));
      return stream;
    } catch (error) {
      stream.fail(error);
      throw error;
    }
  }

  /** Milliseconds left in the client-side budget, which started before connecting. */
  private localBudgetMs(options: CallOptions, startedAt: number): number | undefined {
    if (options.timeoutMs === undefined) return undefined;
    const grace = this.options.deadlineGraceMs ?? clientDeadlineGraceMs(options.timeoutMs);
    return Math.max(0, options.timeoutMs + grace - (performance.now() - startedAt));
  }

  private registerCall<Response>(
    callId: string,
    options: CallOptions,
    startedAt: number,
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
    const budget = this.localBudgetMs(options, startedAt);
    if (budget !== undefined) {
      pending.timer = setTimeout(() => {
        this.cancelPending(callId, localDeadlineError(), "deadline_exceeded");
      }, budget);
    }
    if (options.signal !== undefined) {
      pending.signal = options.signal;
      pending.abortListener = () => {
        this.cancelPending(
          callId,
          new CallCancelledError("Call was cancelled"),
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
    const frame: CallOpenFrame = {
      kind: "call.open",
      call_id: callId,
      service,
      method,
      // JSON.stringify drops undefined; the wire requires payload to exist.
      payload: request === undefined ? null : request,
    };
    if (options.timeoutMs !== undefined) frame.timeout_ms = Math.max(1, Math.round(options.timeoutMs));
    if (options.instance !== undefined) frame.instance_id = options.instance;
    if (options.idleTimeoutMs !== undefined) {
      frame.idle_timeout_ms = Math.max(1, Math.round(options.idleTimeoutMs));
    }
    const metadata = validateMetadata(options.metadata);
    if (metadata !== null) frame.metadata = metadata;
    if (options.idempotencyKey !== undefined) frame.idempotency_key = options.idempotencyKey;
    return frame;
  }

  /** One pass over the endpoints, starting from the last good one. */
  private async openAnyEndpoint(): Promise<void> {
    let last: unknown;
    for (let offset = 0; offset < this.endpoints.length; offset += 1) {
      const index = (this.cursor + offset) % this.endpoints.length;
      try {
        await this.open(this.endpoints[index] as WebSocketEndpoint);
        this.cursor = index;
        return;
      } catch (error) {
        last = error;
      }
    }
    throw last;
  }

  private async open(endpoint: WebSocketEndpoint): Promise<void> {
    const maxSize = this.options.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE;
    const timeout = this.options.handshakeTimeoutMs ?? 10_000;
    const socket = createSocket(endpoint, maxSize, timeout, this.options.auth);
    const writer = new FrameWriter(socket, maxSize, "client");
    const liveness = new Liveness(socket, writer, {
      pingIntervalMs: this.options.pingIntervalMs === undefined
        ? DEFAULT_PING_INTERVAL_MS : this.options.pingIntervalMs,
      pingTimeoutMs: this.options.pingTimeoutMs ?? DEFAULT_PING_TIMEOUT_MS,
    });
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
          fail(new DeadlineExceededError("MeshCall handshake timed out")), timeout);
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
              throw new ProtocolError("Expected compatible hello.ack");
            }
            ready = true;
            this.socket = socket;
            this.writer = writer;
            this.liveness = liveness;
            liveness.start();
            clearTimeout(timer);
            resolve();
          } catch (error) {
            fail(error);
          }
        });
        socket.on("close", () => {
          clearTimeout(timer);
          liveness.stop();
          writer.close();
          const error = new UnavailableError("Connection was lost", { retryable: true });
          if (this.socket === socket) {
            this.socket = undefined;
            this.writer = undefined;
            this.liveness = undefined;
            this.rejectAll(error);
            // Start the next connect from the following endpoint: the one
            // that just died is the least likely to come back first.
            if (this.endpoints.length > 1) this.cursor = (this.cursor + 1) % this.endpoints.length;
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
      // A malformed frame that still names a call fails only that call.
      const callId = error instanceof FrameDecodeError ? error.callId : undefined;
      if (callId !== undefined && (this.calls.has(callId) || this.streams.has(callId))) {
        const failure = new ProtocolError(String((error as Error).message));
        this.rejectPending(callId, failure);
        this.streams.get(callId)?.fail(failure);
        return;
      }
      this.rejectAll(error);
      this.socket?.close(1002, "Invalid MeshCall frame");
      return;
    }
    if (this.liveness?.observe(frame) === true) {
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
          new ProtocolError("Invalid input stream window"),
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
    this.streams.get(frame.call_id)?.receiveWindow(frame);
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
        new ProtocolError("Unexpected output stream item"),
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
        new ProtocolError("Unexpected output stream end"),
      );
    }
  }

  private resolvePending(frame: CallResultFrame): void {
    const stream = this.streams.get(frame.call_id);
    if (stream !== undefined) {
      stream.receiveResult(frame.metadata ?? {});
      return;
    }
    const pending = this.takePending(
      frame.call_id,
      new CallCancelledError("Call is complete"),
    );
    if (pending !== undefined) {
      this.trailers.set(frame.call_id, frame.metadata ?? {});
      pending.resolve(frame.payload);
    }
  }

  private rejectCall(frame: CallErrorFrame): void {
    const error = MeshCallError.fromCode(frame.error.code, frame.error.message, {
      retryable: frame.error.retryable ?? false,
      details: frame.error.details ?? null,
    });
    const stream = this.streams.get(frame.call_id);
    if (stream !== undefined) {
      stream.fail(error, frame.metadata ?? {});
      return;
    }
    if (this.calls.has(frame.call_id)) this.trailers.set(frame.call_id, frame.metadata ?? {});
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
    for (const [name, value] of [["timeoutMs", options.timeoutMs], ["idleTimeoutMs", options.idleTimeoutMs]] as const) {
      if (value !== undefined && (!Number.isFinite(value) || value <= 0)) {
        throw new RangeError(name + " must be positive");
      }
    }
    validateMetadata(options.metadata);
    if (options.instance !== undefined && options.instance === "") throw new RangeError("instance must not be empty");
    if (options.idempotencyKey !== undefined) {
      const bytes = Buffer.byteLength(options.idempotencyKey);
      if (bytes < 1 || bytes > 128) throw new RangeError("idempotencyKey must be 1..128 bytes");
    }
  }

  private ensureNotAborted(options: CallOptions): void {
    if (options.signal?.aborted === true) {
      throw new CallCancelledError("Call was cancelled");
    }
  }

  private async send(frame: Frame): Promise<void> {
    const writer = this.writer;
    if (writer === undefined) {
      throw new UnavailableError("Client is not connected");
    }
    await writer.send(frame);
  }
}

async function retryCall(policy: Required<RetryPolicy>, info: ClientCallInfo, next: ClientCallNext): Promise<unknown> {
  const options = info.options;
  const mayRetry = options.idempotent === true || options.idempotencyKey !== undefined;
  const deadline = options.timeoutMs === undefined ? undefined : performance.now() + options.timeoutMs;
  for (let attempt = 1; ; attempt += 1) {
    try {
      return await next(info);
    } catch (error) {
      const give = (e: MeshCallError): never => {
        if (attempt > 1) {
          throw MeshCallError.fromCode(e.code, e.message, { retryable: e.retryable, details: { ...(e.details ?? {}), attempts: attempt } });
        }
        throw e;
      };
      if (!(error instanceof MeshCallError) || !mayRetry || !error.retryable
        || !policy.retryCodes.includes(error.code) || attempt >= policy.maxAttempts) {
        if (error instanceof MeshCallError) give(error);
        throw error;
      }
      const delay = retryDelayMs(policy, attempt);
      if (deadline !== undefined) {
        const remaining = deadline - performance.now();
        if (remaining <= delay) give(error);
        info.options = { ...info.options, timeoutMs: remaining - delay };
      }
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

function localDeadlineError(reason?: "idle"): MeshCallError {
  return new MeshCallError(
    "deadline_exceeded",
    reason === "idle" ? "Output stream idle timeout" : "Call deadline exceeded",
    { details: reason === undefined ? { source: "client" } : { source: "client", reason } },
  );
}

class ServerStreamState<Item> {
  private readonly queue = new AsyncQueue<Item>();
  private allowance = 0;
  private expectedSequence = 0;
  private ended = false;
  private terminal = false;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private idleTimer: ReturnType<typeof setTimeout> | undefined;
  private idleTimeoutMs: number | undefined;
  private signal: AbortSignal | undefined;
  private abortListener: (() => void) | undefined;
  public trailers: Readonly<Metadata> = {};
  /** The input side of a duplex call; absent for a server stream. */
  private readonly input: { credit: CreditWindow; sequence: number; closed: boolean } | undefined;

  public constructor(
    private readonly callId: string,
    private readonly send: (frame: Frame) => Promise<void>,
    private readonly remove: () => void,
    private readonly itemSchema?: JsonSchema,
    duplex = false,
  ) {
    this.input = duplex ? { credit: new CreditWindow(), sequence: 0, closed: false } : undefined;
  }

  public receiveWindow(frame: StreamWindowFrame): void {
    if (this.input === undefined || frame.direction !== "client") {
      this.fail(new ProtocolError("Unexpected output stream window"));
      return;
    }
    try {
      this.input.credit.grant(frame.credit);
    } catch (error) {
      this.fail(error);
    }
  }

  public async sendInput(item: unknown): Promise<void> {
    const input = this.input;
    if (input === undefined) throw new ProtocolError("Call has no input stream");
    if (input.closed) throw new ProtocolError("Input stream is closed");
    await input.credit.acquire();
    if (this.terminal) throw new CallCancelledError("Call is closed");
    // Claim the sequence before awaiting so concurrent sends stay in order.
    const sequence = input.sequence;
    input.sequence += 1;
    try {
      await this.send({ kind: "stream.item", call_id: this.callId, direction: "client", sequence, payload: item });
    } catch (error) {
      if (isFrameTooLarge(error)) this.abort(error, "frame_too_large"); // the call is open at the peer
      throw error;
    }
  }

  public async closeInput(): Promise<void> {
    const input = this.input;
    if (input === undefined) throw new ProtocolError("Call has no input stream");
    if (input.closed || this.terminal) return;
    input.closed = true;
    await this.send({ kind: "stream.end", call_id: this.callId, direction: "client" });
  }

  public grant(credit: number): void {
    this.allowance += credit;
  }

  public arm(options: CallOptions, budgetMs: number | undefined): void {
    if (this.terminal) {
      return;
    }
    if (options.signal?.aborted === true) {
      this.abort(new CallCancelledError("Call was cancelled"), "caller_cancelled");
      return;
    }
    if (budgetMs !== undefined) {
      this.timer = setTimeout(() => {
        this.abort(localDeadlineError(), "deadline_exceeded");
      }, budgetMs);
    }
    this.idleTimeoutMs = options.idleTimeoutMs;
    this.armIdle();
    if (options.signal !== undefined) {
      this.signal = options.signal;
      this.abortListener = () => {
        this.abort(
          new CallCancelledError("Call was cancelled"),
          "caller_cancelled",
        );
      };
      options.signal.addEventListener("abort", this.abortListener, {
        once: true,
      });
    }
  }

  /** (Re)start the gap timer; measured between arrivals, not between reads. */
  private armIdle(): void {
    if (this.idleTimer !== undefined) {
      clearTimeout(this.idleTimer);
      this.idleTimer = undefined;
    }
    if (this.idleTimeoutMs === undefined || this.ended || this.terminal) return;
    this.idleTimer = setTimeout(() => {
      this.abort(localDeadlineError("idle"), "deadline_exceeded");
    }, this.idleTimeoutMs);
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
      this.fail(new ProtocolError("Invalid output stream item"));
      return;
    }
    if (this.ended || this.allowance <= 0) {
      this.fail(
        new ProtocolError("Output stream exceeded granted credit"),
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
    this.armIdle();
    try {
      checkReceived(this.itemSchema, frame.payload, "output_item");
    } catch (error) {
      this.fail(error);
      return;
    }
    this.queue.push(frame.payload as Item);
  }

  public receiveEnd(frame: StreamEndFrame): void {
    if (this.terminal || frame.direction !== "server" || this.ended) {
      this.fail(new ProtocolError("Invalid output stream end"));
      return;
    }
    this.ended = true;
    this.armIdle();
    // The consumer sees the end once the terminal frame (with trailers) arrives.
  }

  public receiveResult(trailers: Readonly<Metadata>): void {
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
    this.trailers = trailers;
    this.cleanup();
    this.remove();
    this.queue.close();
  }

  public fail(error: unknown, trailers: Readonly<Metadata> = {}): void {
    if (this.terminal) {
      return;
    }
    this.terminal = true;
    this.trailers = trailers;
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
    this.input?.credit.close(new CallCancelledError("Call is closed"));
    for (const timer of [this.timer, this.idleTimer]) {
      if (timer !== undefined) clearTimeout(timer);
    }
    this.timer = undefined;
    this.idleTimer = undefined;
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

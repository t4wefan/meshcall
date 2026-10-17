/**
 * `meshcall/web`: a MeshCall client over the Router's HTTP v1 API
 * (docs/protocol.md → HTTP calls) using `fetch`. No Node imports: it runs in
 * browsers, Deno, Bun, edge runtimes and Node 18+. Browsers cannot set
 * `Authorization` on a WebSocket, which is why the web client speaks HTTP.
 *
 *   import { MeshCallHttpClient } from "meshcall/web";
 *   const rpc = new MeshCallHttpClient("https://router.example", { token: "mk_…" });
 *   const chat = new ChatClient(rpc); // any generated client without duplex methods
 *
 * Unary, server-stream, client-stream and subscription calls behave like
 * `MeshCallClient`'s: the same options, errors, trailers, local schema
 * validation, unary retry policy and subscription resume. A client stream's
 * items are collected and sent as one NDJSON body. Duplex is WebSocket-only.
 * Calling from another origin needs `--http-cors-origin` on the Router.
 */
import type { CallOptions, SubscriptionOptions } from "./client.js";
import { CallCancelledError, DeadlineExceededError, MeshCallError, ProtocolError, UnavailableError } from "./errors.js";
import type { RpcLogger } from "./log.js";
import {
  clientDeadlineGraceMs, DEFAULT_RETRY, retryDelayMs, validateMetadata, type ReconnectPolicy, type RetryPolicy,
} from "./policy.js";
import type { Metadata } from "./protocol.js";
import { mergeCallOptions, type MeshCallRpc, type RpcStream } from "./rpc.js";
import { MeshCallSubscription } from "./subscription.js";
import { checkReceived } from "./validation.js";

export type { CallOptions, SubscriptionOptions } from "./client.js";
export {
  CallCancelledError, DeadlineExceededError, ErrorCode, InternalError, InvalidArgumentError, MeshCallError,
  MethodNotFoundError, PermissionDeniedError, ProtocolError, ResourceExhaustedError, UnauthenticatedError,
  UnavailableError, type Violation,
} from "./errors.js";
export type { MeshCallRpc, RpcStream } from "./rpc.js";
export { MeshCallSubscription } from "./subscription.js";

export interface MeshCallHttpClientOptions {
  /** An API key (`mk_…`) or temporary token (`mt_…`), sent as a Bearer token. */
  readonly token?: string;
  /** Extra headers on every request. */
  readonly headers?: Readonly<Record<string, string>>;
  /** Defaults to the global `fetch`. */
  readonly fetch?: typeof fetch;
  /** Built-in retry for unary calls; `false` disables. */
  readonly retry?: RetryPolicy | false;
  /** Backoff between subscription resubscriptions. */
  readonly reconnect?: ReconnectPolicy;
  /** Extra time past `timeoutMs` before failing locally. Defaults to `max(1s, 10%)`. */
  readonly deadlineGraceMs?: number;
  readonly logger?: RpcLogger;
}

const consoleLogger: RpcLogger = {
  bind() { return consoleLogger; },
  log(level, message, ...args) { (level === "error" ? console.error : console.warn)(message, ...args); },
  debug() {},
  info() {},
  warning(message, ...args) { console.warn(message, ...args); },
  error(message, ...args) { console.error(message, ...args); },
};

/** Local deadline and caller signal folded into one AbortController. */
class CallControl {
  public readonly controller = new AbortController();
  private timer: ReturnType<typeof setTimeout> | undefined;
  private failure: MeshCallError | undefined;
  private readonly onAbort = () => this.abort(new CallCancelledError("Call was cancelled"));

  public constructor(private readonly options: CallOptions, budgetMs: number | undefined) {
    if (options.signal?.aborted === true) this.abort(new CallCancelledError("Call was cancelled"));
    options.signal?.addEventListener("abort", this.onAbort, { once: true });
    if (budgetMs !== undefined) {
      this.timer = setTimeout(() => this.abort(new DeadlineExceededError("Call deadline exceeded", {
        details: { source: "client" },
      })), budgetMs);
    }
  }

  public abort(error: MeshCallError): void {
    this.failure ??= error;
    this.controller.abort();
  }

  /** The error to report for a failed request: ours if we aborted it. */
  public error(cause: unknown): unknown {
    if (this.failure !== undefined) return this.failure;
    if (cause instanceof MeshCallError) return cause;
    return new UnavailableError("HTTP request failed: " + String(cause), { retryable: true });
  }

  public done(): void {
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.options.signal?.removeEventListener("abort", this.onAbort);
  }
}

function trailersOf(headers: Headers): Metadata {
  const trailers: Record<string, string> = {};
  headers.forEach((value, name) => {
    if (name.toLowerCase().startsWith("meshcall-trailer-")) trailers[name.slice("meshcall-trailer-".length)] = value;
  });
  return trailers;
}

function errorOf(body: unknown, status: number): MeshCallError {
  const error = (body as { error?: { code?: unknown; message?: unknown; retryable?: unknown; details?: unknown } } | null)?.error;
  if (error !== undefined && typeof error.code === "string") {
    return MeshCallError.fromCode(error.code, typeof error.message === "string" ? error.message : "", {
      retryable: error.retryable === true,
      details: (error.details ?? null) as Record<string, unknown> | null,
    });
  }
  return new UnavailableError("HTTP " + status + " without a MeshCall error", { retryable: status >= 500 });
}

export class MeshCallHttpClient implements MeshCallRpc {
  private readonly base: string;
  private readonly fetcher: typeof fetch;
  private defaultCallOptions: CallOptions = {};

  /** `url` is the Router's address, e.g. `https://router.example` (ws:// also accepted). */
  public constructor(url: string, private readonly options: MeshCallHttpClientOptions = {}) {
    this.base = url.replace(/^ws(s?):/u, "http$1:").replace(/\/+$/u, "");
    this.fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  public withOptions(options: CallOptions): this {
    const clone = Object.create(MeshCallHttpClient.prototype) as this;
    Object.assign(clone, {
      base: this.base,
      fetcher: this.fetcher,
      options: this.options,
      defaultCallOptions: mergeCallOptions(this.defaultCallOptions, options),
    });
    return clone;
  }

  public async unary<Request, Response>(
    service: string, method: string, request: Request, options: CallOptions = {},
  ): Promise<Response> {
    options = mergeCallOptions(this.defaultCallOptions, options);
    const policy = { ...DEFAULT_RETRY, ...(this.options.retry === false ? { enabled: false } : this.options.retry ?? {}) };
    const mayRetry = policy.enabled && (options.idempotent === true || options.idempotencyKey !== undefined);
    const deadline = options.timeoutMs === undefined ? undefined : performance.now() + options.timeoutMs;
    for (let attempt = 1; ; attempt += 1) {
      const remaining = deadline === undefined ? undefined : deadline - performance.now();
      const attemptOptions = remaining === undefined ? options : { ...options, timeoutMs: Math.max(1, remaining) };
      try {
        const { body } = await this.call(service, method, JSON.stringify(request ?? null), "application/json", attemptOptions);
        checkReceived(options.responseSchema, body, "response");
        return body as Response;
      } catch (error) {
        const delay = retryDelayMs(policy, attempt);
        const retry = mayRetry && error instanceof MeshCallError && error.retryable
          && policy.retryCodes.includes(error.code) && attempt < policy.maxAttempts
          && (deadline === undefined || deadline - performance.now() > delay);
        if (!retry) {
          if (attempt > 1 && error instanceof MeshCallError) {
            throw MeshCallError.fromCode(error.code, error.message, {
              retryable: error.retryable, details: { ...(error.details ?? {}), attempts: attempt },
            });
          }
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }

  public async clientStream<Request, Item, Response>(
    service: string, method: string, request: Request, items: AsyncIterable<Item>, options: CallOptions = {},
  ): Promise<Response> {
    options = mergeCallOptions(this.defaultCallOptions, options);
    const lines = [JSON.stringify(request ?? null)];
    for await (const item of items) lines.push(JSON.stringify(item));
    const { body } = await this.call(service, method, lines.join("\n") + "\n", "application/x-ndjson", options);
    checkReceived(options.responseSchema, body, "response");
    return body as Response;
  }

  public serverStream<Request, Item>(
    service: string, method: string, request: Request, options: CallOptions = {},
  ): RpcStream<Item> {
    options = mergeCallOptions(this.defaultCallOptions, options);
    return new HttpServerStream<Item>((control) => this.open(
      service, method, JSON.stringify(request ?? null), "application/json", options, control,
    ), options, this.budget(options));
  }

  public subscribe<Request extends object, Item>(
    service: string, method: string, request: Request, options: SubscriptionOptions = {},
  ): MeshCallSubscription<Item> {
    const { cursor, resume, ...callOptions } = options;
    const merged = mergeCallOptions(this.defaultCallOptions, callOptions);
    return new MeshCallSubscription<Item>(
      (payload) => this.serverStream<Record<string, unknown>, Item>(service, method, payload, merged),
      request as Record<string, unknown>,
      { ...(cursor === undefined ? {} : { cursor }), ...(resume === undefined ? {} : { resume }) },
      this.options.reconnect ?? {},
      this.options.logger ?? consoleLogger,
      service + "." + method,
    );
  }

  private budget(options: CallOptions): number | undefined {
    if (options.timeoutMs === undefined) return undefined;
    return options.timeoutMs + (this.options.deadlineGraceMs ?? clientDeadlineGraceMs(options.timeoutMs));
  }

  /** A call answered with one JSON document (unary, client stream). */
  private async call(
    service: string, method: string, body: string, contentType: string, options: CallOptions,
  ): Promise<{ body: unknown; trailers: Metadata }> {
    const control = new CallControl(options, this.budget(options));
    try {
      const response = await this.open(service, method, body, contentType, options, control);
      const text = await response.text();
      let parsed: unknown = null;
      try {
        parsed = text.trim() === "" ? null : JSON.parse(text);
      } catch {
        throw new ProtocolError("The Router's response is not JSON", { details: { source: "client" } });
      }
      return { body: parsed, trailers: trailersOf(response.headers) };
    } catch (error) {
      throw control.error(error);
    } finally {
      control.done();
    }
  }

  /** Sends the request; resolves with a 2xx response or throws the call's error. */
  private async open(
    service: string, method: string, body: string, contentType: string, options: CallOptions, control: CallControl,
  ): Promise<Response> {
    const headers: Record<string, string> = { ...(this.options.headers ?? {}), "Content-Type": contentType };
    if (this.options.token !== undefined) headers.Authorization = "Bearer " + this.options.token;
    if (options.timeoutMs !== undefined) headers["MeshCall-Timeout-Ms"] = String(Math.max(1, Math.round(options.timeoutMs)));
    if (options.idleTimeoutMs !== undefined) headers["MeshCall-Idle-Timeout-Ms"] = String(Math.max(1, Math.round(options.idleTimeoutMs)));
    if (options.idempotencyKey !== undefined) headers["Idempotency-Key"] = options.idempotencyKey;
    if (options.instance !== undefined) headers["MeshCall-Instance"] = options.instance;
    for (const [key, value] of Object.entries(validateMetadata(options.metadata) ?? {})) headers["MeshCall-Meta-" + key] = value;
    const response = await this.fetcher(
      this.base + "/v1/call/" + encodeURIComponent(service) + "/" + encodeURIComponent(method),
      { method: "POST", headers, body, signal: control.controller.signal },
    );
    if (response.ok) return response;
    let parsed: unknown = null;
    try {
      parsed = await response.json();
    } catch {
      // Not a MeshCall error envelope (a proxy's page, an empty body).
    }
    throw errorOf(parsed, response.status);
  }
}

/** A server stream read from NDJSON envelopes: {"item"} …, then {"end"} or {"error"}. */
class HttpServerStream<Item> implements RpcStream<Item> {
  public trailers: Readonly<Metadata> = {};
  private readonly control: CallControl;
  private reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  private readonly opened: Promise<void>;
  private readonly decoder = new TextDecoder();
  private buffer = "";
  private finished = false;

  public constructor(
    open: (control: CallControl) => Promise<Response>,
    private readonly options: CallOptions,
    budgetMs: number | undefined,
  ) {
    this.control = new CallControl(options, budgetMs);
    this.opened = open(this.control).then((response) => {
      if (response.body === null) throw new ProtocolError("The stream has no body", { details: { source: "client" } });
      this.reader = response.body.getReader();
    });
    this.opened.catch(() => undefined); // reported by next()
  }

  public [Symbol.asyncIterator](): AsyncIterableIterator<Item> {
    return this;
  }

  public async next(): Promise<IteratorResult<Item>> {
    if (this.finished) return { done: true, value: undefined };
    try {
      await this.opened;
      for (;;) {
        const newline = this.buffer.indexOf("\n");
        if (newline >= 0) {
          const line = this.buffer.slice(0, newline).trim();
          this.buffer = this.buffer.slice(newline + 1);
          if (line === "") continue;
          const result = this.envelope(line);
          if (result !== undefined) return result;
          continue;
        }
        const chunk = await this.reader!.read();
        if (chunk.done) {
          throw new UnavailableError("The stream ended without an end or error line", { retryable: true });
        }
        this.buffer += this.decoder.decode(chunk.value, { stream: true });
      }
    } catch (error) {
      this.finish();
      throw this.control.error(error);
    }
  }

  private envelope(line: string): IteratorResult<Item> | undefined {
    let parsed: { item?: unknown; end?: { trailers?: Metadata }; error?: unknown; trailers?: Metadata };
    try {
      parsed = JSON.parse(line) as typeof parsed;
    } catch {
      throw new ProtocolError("A stream line is not a JSON envelope", { details: { source: "client" } });
    }
    if ("item" in parsed) {
      checkReceived(this.options.itemSchema, parsed.item, "output_item");
      return { done: false, value: parsed.item as Item };
    }
    if (parsed.end !== undefined) {
      this.trailers = parsed.end.trailers ?? {};
      this.finish();
      return { done: true, value: undefined };
    }
    if (parsed.error !== undefined) {
      this.trailers = parsed.trailers ?? {};
      throw errorOf(parsed, 200);
    }
    return undefined; // unknown envelope kinds are ignored, like unknown fields
  }

  private finish(): void {
    this.finished = true;
    this.control.done();
  }

  public async cancel(_reason = "client_closed"): Promise<void> {
    if (this.finished) return;
    this.finish();
    // Closing the request is the cancellation: the Router sends call.cancel.
    this.control.abort(new CallCancelledError("Call was cancelled"));
    await this.reader?.cancel().catch(() => undefined);
  }

  public async return(): Promise<IteratorResult<Item>> {
    await this.cancel("iterator_closed");
    return { done: true, value: undefined };
  }

  public async [Symbol.asyncDispose](): Promise<void> {
    await this.cancel("iterator_closed");
  }
}


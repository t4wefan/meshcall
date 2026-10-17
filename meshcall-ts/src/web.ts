/**
 * `meshcall/web`: a MeshCall client over the Router's HTTP v1 API
 * (docs/protocol.md → HTTP calls) using `fetch`. No Node imports: it runs in
 * browsers, Deno, Bun, edge runtimes and Node 18+. Browsers cannot set
 * `Authorization` on a WebSocket, which is why the web client speaks HTTP.
 *
 *   import { Router, Mesh } from "meshcall/web";
 *   const router = new Router("https://router.example");
 *   const mesh = new Mesh(router, { token: "mk_…" });
 *   const chat = new ChatClient(mesh); // any generated client without duplex methods
 *
 * Unary, server-stream, client-stream and subscription calls behave like
 * WebSocket `Mesh`'s: the same options, errors, trailers, local schema
 * validation, unary retry policy and subscription resume. A client stream's
 * items are collected and sent as one NDJSON body. Duplex is WebSocket-only.
 * Calling from another origin needs `--http-cors-origin` on the Router.
 */
import type { CallOptions, DuplexCallOptions, MeshCallDuplexCall, SubscriptionOptions } from "./client.js";
import { CallCancelledError, DeadlineExceededError, MeshCallError, ProtocolError, UnavailableError } from "./errors.js";
import type { RpcLogger } from "./log.js";
import {
  clientDeadlineGraceMs, DEFAULT_RETRY, retryDelayMs, validateMetadata, type ReconnectPolicy, type RetryPolicy,
} from "./policy.js";
import type { Metadata } from "./protocol.js";
import { mergeCallOptions, type MeshCallRpc, type RpcStream } from "./rpc.js";
// Types only: importing router-auth's values would pull contract.ts (and Node's
// crypto/fs/path) into browser bundles.
import type { IssuedToken, RegistryService, RouterIdentity, RouterScope } from "./router-auth.js";
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

export const DEFAULT_ROUTER_URL = "ws://127.0.0.1:8765";

export type RouterEndpoint = string | { readonly unixPath: string } | { readonly pipe: string };

export interface RouterOptions {
  readonly url?: RouterEndpoint | ReadonlyArray<RouterEndpoint>;
  /** Unix socket path (not on Windows: use `pipe`). */
  readonly unixPath?: string;
  /** Named pipe `\\.\pipe\NAME` (Windows only). */
  readonly pipe?: string;
}

const VALID_ROUTER_OPTION_KEYS = new Set(["url", "unixPath", "pipe"]);

function defaultRouterEndpoint(): string {
  const envUrl = (
    (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.MESHCALL_ROUTER_URL ?? ""
  ).trim();
  return envUrl !== "" ? envUrl : DEFAULT_ROUTER_URL;
}

function isHttpEndpoint(endpoint: RouterEndpoint): boolean {
  return typeof endpoint === "string" && (endpoint.startsWith("http://") || endpoint.startsWith("https://"));
}

function validateEndpointScheme(endpoint: RouterEndpoint): void {
  if (typeof endpoint !== "string") return;
  if (
    endpoint.startsWith("ws://") ||
    endpoint.startsWith("wss://") ||
    endpoint.startsWith("http://") ||
    endpoint.startsWith("https://") ||
    endpoint.startsWith("unix://") ||
    endpoint.startsWith("pipe://")
  ) {
    return;
  }
  throw new TypeError(
    `Router URL must use ws://, wss://, http://, https://, unix:// or pipe://, got ${JSON.stringify(endpoint)}`,
  );
}

export function formatRouterEndpointUrl(endpoint: RouterEndpoint): string {
  if (typeof endpoint === "string") return endpoint;
  if ("unixPath" in endpoint) {
    return endpoint.unixPath.startsWith("unix://") ? endpoint.unixPath : `unix://${endpoint.unixPath}`;
  }
  const bare = endpoint.pipe.startsWith("pipe://")
    ? endpoint.pipe.slice("pipe://".length)
    : endpoint.pipe.replace(/^\\\\\.\\pipe\\/u, "");
  return `pipe://${bare}`;
}

function resolveRouterEndpoints(options: RouterOptions): {
  endpoints: ReadonlyArray<RouterEndpoint>;
  isHttp: boolean;
} {
  for (const key of Object.keys(options)) {
    if (!VALID_ROUTER_OPTION_KEYS.has(key)) {
      throw new TypeError(
        `Router only describes an endpoint (url, unixPath, or pipe); unknown option ${JSON.stringify(key)} — pass connection and auth options to new Mesh(router, options)`,
      );
    }
  }
  const count = [options.url, options.unixPath, options.pipe].filter((v) => v !== undefined).length;
  if (count > 1) {
    throw new TypeError("Choose one of url, unixPath (not on Windows) or pipe (Windows)");
  }
  let endpoints: ReadonlyArray<RouterEndpoint>;
  if (options.unixPath !== undefined) {
    if (options.unixPath.trim() === "") throw new RangeError("unixPath must not be empty");
    endpoints = [{ unixPath: options.unixPath }];
  } else if (options.pipe !== undefined) {
    if (options.pipe.trim() === "") throw new RangeError("pipe must not be empty");
    endpoints = [{ pipe: options.pipe }];
  } else if (options.url !== undefined) {
    const list = Array.isArray(options.url)
      ? [...(options.url as ReadonlyArray<RouterEndpoint>)]
      : [options.url as RouterEndpoint];
    if (list.length === 0) throw new RangeError("At least one Router endpoint is required");
    endpoints = list;
  } else {
    endpoints = [defaultRouterEndpoint()];
  }

  for (const ep of endpoints) {
    validateEndpointScheme(ep);
  }
  const httpCount = endpoints.filter(isHttpEndpoint).length;
  if (httpCount > 0 && httpCount !== endpoints.length) {
    throw new TypeError("Cannot mix HTTP(S) and WebSocket/Unix/pipe endpoints in the same Router");
  }
  return { endpoints, isHttp: httpCount > 0 };
}

/**
 * Pure endpoint descriptor for a remote or external `meshcall-router`.
 * Performs zero I/O; pass it to `new Mesh(router)` or `await Mesh.connect(router)`.
 */
export class Router {
  public readonly options: RouterOptions;
  public readonly endpoints: ReadonlyArray<RouterEndpoint>;
  public readonly isHttp: boolean;

  public constructor(
    options: RouterOptions | RouterEndpoint | ReadonlyArray<RouterEndpoint> = {},
  ) {
    if (typeof options === "string") {
      this.options = { url: options };
    } else if (Array.isArray(options)) {
      this.options = { url: options };
    } else if (typeof options === "object" && options !== null) {
      if ("unixPath" in options && Object.keys(options).length === 1 && typeof options.unixPath === "string") {
        this.options = { unixPath: options.unixPath };
      } else if ("pipe" in options && Object.keys(options).length === 1 && typeof options.pipe === "string") {
        this.options = { pipe: options.pipe };
      } else {
        this.options = options as RouterOptions;
      }
    } else {
      throw new TypeError("Router expects a URL string, array of endpoints, or RouterOptions object");
    }
    const resolved = resolveRouterEndpoints(this.options);
    this.endpoints = resolved.endpoints;
    this.isHttp = resolved.isHttp;
  }

  public get url(): string {
    return formatRouterEndpointUrl(this.endpoints[0]!);
  }

  public get urls(): ReadonlyArray<string> {
    return this.endpoints.map(formatRouterEndpointUrl);
  }
}

export interface HttpTransportOptions {
  /** Authorization header value (e.g. `Bearer mk_...` or `Basic ...`), or built from `token`. */
  readonly authorization?: string;
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

export interface MeshWebOptions {
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

const REGISTRY_SERVICE = "meshcall.router.v1.Registry";
const ROUTER_AUTH_SERVICE = "meshcall.router.v1.AuthService";

/** The Router normalises a service name without a version segment to `.v0`. */
function normalizeScopeService(service: string): string {
  if (service === "*" || service === "" || /\.v\d/u.test(service)) return service;
  return `${service}.v0`;
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

/** @internal HTTP v1 transport used by `Mesh` when connecting over HTTP(S). */
export class HttpTransport implements MeshCallRpc {
  private readonly base: string;
  private readonly fetcher: typeof fetch;
  private defaultCallOptions: CallOptions = {};

  public constructor(url: string, private readonly options: HttpTransportOptions = {}) {
    this.base = url.replace(/^ws(s?):/u, "http$1:").replace(/\/+$/u, "");
    this.fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  public withOptions(options: CallOptions): this {
    const clone = Object.create(HttpTransport.prototype) as this;
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

  public duplex<Request, InputItem, OutputItem>(
    _service: string, _method: string, _request: Request, _options?: DuplexCallOptions,
  ): MeshCallDuplexCall<InputItem, OutputItem> {
    throw new MeshCallError("method_not_found", "duplex methods are WebSocket-only", {
      details: { source: "client", reason: "duplex_over_http" },
    });
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
    if (this.options.authorization !== undefined) {
      headers.Authorization = this.options.authorization;
    } else if (this.options.token !== undefined) {
      headers.Authorization = "Bearer " + this.options.token;
    }
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

/** Sub-client for the Router's built-in `meshcall.router.v1.Registry` service (`mesh.registry`). */
export class RegistryClient {
  public constructor(private readonly rpc: MeshCallRpc) {}

  public async list(options?: CallOptions): Promise<RegistryService[]> {
    const result = await this.rpc.unary<Record<string, never>, { services: RegistryService[] }>(
      REGISTRY_SERVICE, "list", {}, { idempotent: true, ...options },
    );
    return result.services ?? [];
  }

  public describe(service: string, options?: CallOptions): Promise<RegistryService> {
    return this.rpc.unary(
      REGISTRY_SERVICE, "describe", { service: normalizeScopeService(service) }, { idempotent: true, ...options },
    );
  }
}

/** Sub-client for the Router's built-in `meshcall.router.v1.AuthService` (`mesh.auth`). */
export class AuthClient {
  public constructor(private readonly rpc: MeshCallRpc) {}

  public whoami(options?: CallOptions): Promise<RouterIdentity> {
    return this.rpc.unary(ROUTER_AUTH_SERVICE, "whoami", {}, { idempotent: true, ...options });
  }

  public issueToken(
    scopes: ReadonlyArray<RouterScope>, ttlSeconds = 900, options?: CallOptions,
  ): Promise<IssuedToken> {
    if (scopes.length === 0) throw new RangeError("issueToken needs at least one scope");
    if (!Number.isSafeInteger(ttlSeconds) || ttlSeconds <= 0) throw new RangeError("ttlSeconds must be a positive integer");
    return this.rpc.unary(ROUTER_AUTH_SERVICE, "issue_token", {
      scopes: scopes.map((scope) => ({ ...scope, service: normalizeScopeService(scope.service) })),
      ttl_seconds: ttlSeconds,
    }, options);
  }

  public async revokeToken(tokenId: string, options?: CallOptions): Promise<boolean> {
    if (tokenId === "") throw new RangeError("tokenId must not be empty");
    const result = await this.rpc.unary<{ token_id: string }, { revoked: boolean }>(
      ROUTER_AUTH_SERVICE, "revoke_token", { token_id: tokenId }, { idempotent: true, ...options },
    );
    return result.revoked;
  }
}

/**
 * Browser-safe `Mesh` participant over the Router's HTTP v1 API.
 * Pass a `Router` instance (`new Mesh(new Router("https://router.example"), { token })`)
 * and share `mesh` across generated clients (`new ChatClient(mesh)`), `mesh.registry`, and `mesh.auth`.
 */
export class Mesh implements MeshCallRpc {
  public readonly router: Router;
  public readonly registry: RegistryClient;
  public readonly auth: AuthClient;
  private readonly transport: HttpTransport;

  public constructor(router: Router, options: MeshWebOptions = {}) {
    if (!(router instanceof Router)) {
      throw new TypeError(
        "Mesh requires a Router instance; pass new Router(url) instead of a raw URL or options object",
      );
    }
    this.router = router;
    this.transport = new HttpTransport(router.url, options);
    this.registry = new RegistryClient(this);
    this.auth = new AuthClient(this);
  }

  public static async connect(router: Router, options: MeshWebOptions = {}): Promise<Mesh> {
    const mesh = new Mesh(router, options);
    await mesh.connect();
    return mesh;
  }

  public withOptions(options: CallOptions): this {
    const clone = Object.create(Mesh.prototype) as this;
    const transport = this.transport.withOptions(options);
    Object.assign(clone, {
      router: this.router,
      transport,
      registry: new RegistryClient(clone),
      auth: new AuthClient(clone),
    });
    return clone;
  }

  public async connect(): Promise<this> {
    return this;
  }

  public async close(): Promise<void> {
    // fetch keeps no persistent connection.
  }

  public async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  public unary<Request, Response>(service: string, method: string, request: Request, options?: CallOptions): Promise<Response> {
    return this.transport.unary(service, method, request, options);
  }

  public serverStream<Request, Item>(service: string, method: string, request: Request, options?: CallOptions): RpcStream<Item> {
    return this.transport.serverStream(service, method, request, options);
  }

  public clientStream<Request, Item, Response>(
    service: string, method: string, request: Request, items: AsyncIterable<Item>, options?: CallOptions,
  ): Promise<Response> {
    return this.transport.clientStream(service, method, request, items, options);
  }

  public subscribe<Request extends object, Item>(
    service: string, method: string, request: Request, options?: SubscriptionOptions,
  ): MeshCallSubscription<Item> {
    return this.transport.subscribe(service, method, request, options);
  }

  public duplex<Request, InputItem, OutputItem>(
    service: string, method: string, request: Request, options?: DuplexCallOptions,
  ): MeshCallDuplexCall<InputItem, OutputItem> {
    return this.transport.duplex(service, method, request, options);
  }
}

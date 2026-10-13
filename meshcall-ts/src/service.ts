import type { RpcLogger } from "./logging.js";
import type { BalancePolicy, Metadata } from "./protocol.js";

export interface RpcContext {
  readonly signal: AbortSignal;
  /**
   * Absolute `performance.now()` value by which the call must finish, or
   * `null` when the caller set no timeout.
   */
  readonly deadlineMs: number | null;
  /** Milliseconds left in the call's budget, or `null` when unlimited. */
  remainingMs(): number | null;
  readonly callId: string;
  readonly service: string;
  readonly method: string;
  /** Request metadata the caller attached to `call.open`. */
  readonly metadata: Readonly<Metadata>;
  /** Idempotency key the caller attached, if any. */
  readonly idempotencyKey: string | null;
  /** This server's instance id; what a targeted caller addressed. */
  readonly instanceId: string;
  /** Attach a trailer to the terminal `call.result` or `call.error` frame. */
  setTrailer(key: string, value: string): void;
  readonly trailers: Readonly<Metadata>;
  readonly logger: RpcLogger;
}

/** Kept as an alias for existing unary handlers. */
export type UnaryContext = RpcContext;

export interface UnaryMethod<Request = unknown, Response = unknown> {
  readonly stream: "unary";
  readonly balance?: BalancePolicy;
  readonly handler: (request: Request, context: RpcContext) => Response | Promise<Response>;
}

export interface ServerStreamMethod<Request = unknown, Item = unknown> {
  readonly stream: "server_stream";
  readonly balance?: BalancePolicy;
  readonly handler: (
    request: Request,
    context: RpcContext,
  ) => AsyncIterable<Item> | Promise<AsyncIterable<Item>>;
}

export interface ClientStreamMethod<Request = unknown, Item = unknown, Response = unknown> {
  readonly stream: "client_stream";
  readonly balance?: BalancePolicy;
  readonly handler: (
    request: Request,
    items: AsyncIterable<Item>,
    context: RpcContext,
  ) => Response | Promise<Response>;
}

// Handler payload types are erased only after their contracts have been compiled.
export type ServiceMethod =
  | UnaryMethod<any, any>
  | ServerStreamMethod<any, any>
  | ClientStreamMethod<any, any, any>;

export interface ServiceDefinition {
  readonly name: string;
  readonly balance?: BalancePolicy;
  /**
   * Every call must name the instance it is for (`instance` in the call
   * options): instances hold distinct state such as a session, a loaded
   * model or an actor.
   */
  readonly targeted?: boolean;
  readonly methods: Readonly<Record<string, ServiceMethod>>;
}

/**
 * Runs the rest of the server chain: later interceptors, then the handler.
 * Resolves after the whole call, including every streamed item, with the
 * handler's final result (`null` for server streams). Handler and runtime
 * errors reject through it.
 */
export type ServerCallNext = () => Promise<unknown>;

/**
 * `(ctx, request, next) => result`. Runs in the order given to
 * `MeshCallServer({ interceptors })`; the first one is outermost. `request` is
 * the validated request. Throwing a `MeshCallError` short-circuits the call;
 * a returned value is validated like a handler's.
 */
export type ServerInterceptor = (
  context: RpcContext,
  request: unknown,
  next: ServerCallNext,
) => Promise<unknown>;

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

/**
 * What a duplex handler receives: iterate it for input items and `send`
 * output items while doing so. Returning from the handler closes the output
 * and ends the call; `closeSend` closes the output earlier while input is
 * still being read.
 */
export interface DuplexChannel<Input, Output> extends AsyncIterable<Input> {
  send(item: Output): Promise<void>;
  closeSend(): Promise<void>;
}

/**
 * Input and output stream at the same time: the shape of last resort (see
 * `@method.duplex`). There is no result value.
 */
export interface DuplexMethod<Request = unknown, Input = unknown, Output = unknown> {
  readonly stream: "duplex";
  readonly balance?: BalancePolicy;
  /** Why a server stream plus unary calls, or a client stream, will not do. */
  readonly reason: string;
  readonly handler: (
    request: Request,
    channel: DuplexChannel<Input, Output>,
    context: RpcContext,
  ) => void | Promise<void>;
}

// Handler payload types are erased only after their contracts have been compiled.
export type ServiceMethod =
  | UnaryMethod<any, any>
  | ServerStreamMethod<any, any>
  | ClientStreamMethod<any, any, any>
  | DuplexMethod<any, any, any>;

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
 * handler's final result (`null` for server streams and duplex calls). Handler and runtime
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

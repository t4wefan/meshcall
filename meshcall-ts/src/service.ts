import type { RpcLogger } from "./logging.js";
import type { BalancePolicy } from "./protocol.js";

export interface RpcContext {
  readonly signal: AbortSignal;
  readonly deadlineUnixMs: number | null;
  readonly callId: string;
  readonly service: string;
  readonly method: string;
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
  readonly methods: Readonly<Record<string, ServiceMethod>>;
}

/**
 * What generated clients need from a client runtime. `MeshCallClient`
 * (WebSocket) and `MeshCallHttpClient` (fetch over HTTP v1, from
 * `meshcall/web`) both satisfy it; duplex calls need `MeshCallClient`.
 */
import type { CallOptions, SubscriptionOptions } from "./client.js";
import type { Metadata } from "./protocol.js";
import type { MeshCallSubscription } from "./subscription.js";

/** A stream of items from the server, cancellable, with trailers once it ends. */
export interface RpcStream<Item> extends AsyncIterableIterator<Item> {
  /** Server trailers; populated once the stream has terminated. */
  readonly trailers: Readonly<Metadata>;
  cancel(reason?: string): Promise<void>;
}

export interface MeshCallRpc {
  unary<Request, Response>(service: string, method: string, request: Request, options?: CallOptions): Promise<Response>;
  serverStream<Request, Item>(service: string, method: string, request: Request, options?: CallOptions): RpcStream<Item>;
  clientStream<Request, Item, Response>(
    service: string, method: string, request: Request, items: AsyncIterable<Item>, options?: CallOptions,
  ): Promise<Response>;
  subscribe<Request extends object, Item>(
    service: string, method: string, request: Request, options?: SubscriptionOptions,
  ): MeshCallSubscription<Item>;
}

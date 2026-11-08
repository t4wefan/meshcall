/**
 * What generated clients need from a client runtime. `Mesh` (from `meshcall`
 * or `meshcall/web`) and `MeshCallClient` satisfy it; duplex calls need a
 * WebSocket connection (`ws://`, `wss://`, a Unix socket or a named pipe).
 */
import type { CallOptions, DuplexCallOptions } from "./client.js";
import type { Metadata } from "./protocol.js";

declare global {
  interface SymbolConstructor {
    readonly asyncDispose: unique symbol;
  }
}

(Symbol as { asyncDispose?: symbol }).asyncDispose ??= Symbol.for("Symbol.asyncDispose");

export function mergeCallOptions<T extends CallOptions>(base: CallOptions, extra?: T): T {
  const mergedMetadata =
    base.metadata !== undefined && extra?.metadata !== undefined
      ? { ...base.metadata, ...extra.metadata }
      : (extra?.metadata ?? base.metadata);
  const out: CallOptions = {
    ...base,
    ...extra,
    ...(mergedMetadata === undefined ? {} : { metadata: mergedMetadata }),
  };
  return out as T;
}

/** A stream of items from the server, cancellable, with trailers once it ends. */
export interface RpcStream<Item> extends AsyncIterableIterator<Item> {
  /** Server trailers; populated once the stream has terminated. */
  readonly trailers: Readonly<Metadata>;
  cancel(reason?: string): Promise<void>;
  [Symbol.asyncDispose]?(): Promise<void>;
}

/**
 * A duplex call: `send` input items while iterating output items. The output
 * ends when the call ends successfully; a failure is thrown from the
 * iteration. Read output concurrently with sending: a caller that only sends
 * stalls once the server's output credit runs out.
 */
export interface RpcDuplex<Input, Output> extends RpcStream<Output> {
  send(item: Input): Promise<void>;
  /** Half-close the input; the server's iteration of it then ends. */
  closeSend(): Promise<void>;
}

export interface MeshCallRpc {
  unary<Request, Response>(service: string, method: string, request: Request, options?: CallOptions): Promise<Response>;
  serverStream<Request, Item>(service: string, method: string, request: Request, options?: CallOptions): RpcStream<Item>;
  clientStream<Request, Item, Response>(
    service: string, method: string, request: Request, items: AsyncIterable<Item>, options?: CallOptions,
  ): Promise<Response>;
  duplex?<Request, InputItem, OutputItem>(
    service: string, method: string, request: Request, options?: DuplexCallOptions,
  ): RpcDuplex<InputItem, OutputItem>;
  withOptions(options: CallOptions): this;
  close?(): Promise<void>;
  [Symbol.asyncDispose]?(): Promise<void>;
}

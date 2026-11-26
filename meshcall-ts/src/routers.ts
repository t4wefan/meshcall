// A Mesh over several independent Routers (docs/protocol.md → Several
// Routers): one connection per Router, opened when first needed; each call
// goes to the Router where its instance was last reached, or to the Routers
// in turn, moving on only when a Router did not route the call.

import type { CallOptions, DuplexCallOptions } from "./client.js";
import { CredentialSourceError } from "./credentials.js";
import { DeadlineExceededError, MeshCallError, UnavailableError } from "./errors.js";
import { reconnectDelays, retryUnary, type ReconnectPolicy, type RetryPolicy } from "./policy.js";
import type { Metadata } from "./protocol.js";
import { mergeCallOptions, type MeshCallRpc, type RpcDuplex, type RpcStream } from "./rpc.js";

/** One Router's connection: a client of that Router alone. */
export interface RouterConnection extends MeshCallRpc {
  connect?(): Promise<unknown>;
  close?(): Promise<void>;
}

/** The Router's own services, which only one Router can answer. */
export const ROUTER_SERVICE_PREFIX = "meshcall.router.";

/**
 * What a Router answers for a call it did not route (docs/protocol.md →
 * Several Routers), the answer worth reporting first when none routed it: a
 * draining Router may take the call again soon, and one that serves the
 * service but not the instance says more than one that lacks the service.
 */
const NOT_ROUTED = ["router_draining", "instance_unknown", "no_instances"];

function rank(error: unknown): number {
  return NOT_ROUTED.indexOf(String((error as MeshCallError).details?.["reason"]));
}

/**
 * The Router answered without routing the call, so it reached no handler and
 * another Router may take it: the service or the named instance is not
 * registered there, or that Router is draining.
 */
export function routerDidNotRoute(error: unknown): boolean {
  return error instanceof MeshCallError && error.details?.["source"] === "router"
    && NOT_ROUTED.includes(String(error.details?.["reason"]));
}

interface RouterSetState {
  readonly connections: Array<RouterConnection | undefined>;
  /** Instance id → index of the Router a call to it last succeeded on. */
  readonly located: Map<string, number>;
  /** Index of the Router that served the last call; tried first. */
  cursor: number;
  everConnected: boolean;
  closed: boolean;
}

export interface RouterSetOptions {
  /** Backoff once no Router could be reached. */
  readonly reconnect?: ReconnectPolicy;
  /** Retries of unary calls, each going round the Routers again. */
  readonly retry?: RetryPolicy | false;
}

/**
 * A `MeshCallRpc` over several Routers that know nothing of each other. Each
 * `open(router)` connection must try only that Router, must not wait between
 * connection attempts and must not retry calls: the set moves on to the next
 * Router instead, backs off (`reconnect`) only once none could be reached,
 * and retries unary calls (`retry`) across all of them.
 */
export class RouterSet implements MeshCallRpc {
  private readonly state: RouterSetState;
  private defaults: CallOptions = {};

  public constructor(
    /** The Routers as listed (`wss://…`, `unix:///…`): how sources and results name them. */
    public readonly routers: ReadonlyArray<string>,
    private readonly open: (router: string) => RouterConnection,
    private readonly options: RouterSetOptions = {},
  ) {
    if (routers.length < 2) throw new RangeError("A RouterSet needs at least two Routers");
    if (new Set(routers).size !== routers.length) throw new RangeError("A Router is listed twice");
    this.state = { connections: routers.map(() => undefined), located: new Map(), cursor: 0, everConnected: false, closed: false };
  }

  /** The connection to one of the Routers, opened when first needed. */
  public at(router: string): RouterConnection {
    const index = this.routers.indexOf(router);
    if (index < 0) throw new RangeError(`${JSON.stringify(router)} is not one of this Mesh's Routers: ${this.routers.join(", ")}`);
    return this.connection(index);
  }

  public withOptions(options: CallOptions): this {
    const clone = Object.create(RouterSet.prototype) as this;
    Object.assign(clone, {
      routers: this.routers, open: this.open, options: this.options, state: this.state,
      defaults: mergeCallOptions(this.defaults, options),
    });
    return clone;
  }

  /** Connect to one Router, the first that answers. */
  public async connect(options: CallOptions = {}): Promise<void> {
    await this.route(undefined, options, async () => undefined);
  }

  public async close(): Promise<void> {
    this.state.closed = true;
    const open = this.state.connections.filter((c): c is RouterConnection => c !== undefined);
    this.state.connections.fill(undefined);
    this.state.located.clear();
    await Promise.all(open.map((c) => c.close?.()));
  }

  public async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  public unary<Request, Response>(service: string, method: string, request: Request, options?: CallOptions): Promise<Response> {
    const merged = this.prepare(service, options);
    return retryUnary(this.options.retry, merged, (timeoutMs) => this.route(
      merged.instance, timeoutMs === undefined ? merged : { ...merged, timeoutMs },
      (connection, attempt) => connection.unary<Request, Response>(service, method, request, attempt),
    ));
  }

  public serverStream<Request, Item>(service: string, method: string, request: Request, options?: CallOptions): RpcStream<Item> {
    const merged = this.prepare(service, options);
    return new RoutedStream<Item>(() => this.route(merged.instance, merged, async (connection, attempt) => {
      const stream = connection.serverStream<Request, Item>(service, method, request, attempt);
      return { stream, first: Promise.resolve(await firstResult(stream)) };
    }));
  }

  public async clientStream<Request, Item, Response>(
    service: string, method: string, request: Request, items: AsyncIterable<Item>, options?: CallOptions,
  ): Promise<Response> {
    const merged = this.prepare(service, options);
    const input = new ReplayableInput(items);
    try {
      return await this.route(merged.instance, merged, (connection, attempt) =>
        connection.clientStream<Request, Item, Response>(service, method, request, input.attempt(), attempt));
    } finally {
      await input.close();
    }
  }

  public duplex<Request, InputItem, OutputItem>(
    service: string, method: string, request: Request, options?: DuplexCallOptions,
  ): RpcDuplex<InputItem, OutputItem> {
    const merged = this.prepare(service, options) as DuplexCallOptions;
    return new RoutedDuplex<InputItem, OutputItem>((probe) =>
      this.route(merged.instance, merged, (connection, attempt) => {
        if (connection.duplex === undefined) throw new Error("Duplex is not supported by this transport");
        return probe(connection.duplex<Request, InputItem, OutputItem>(service, method, request, attempt as DuplexCallOptions));
      }));
  }

  private prepare(service: string, options: CallOptions | undefined): CallOptions {
    if (service.startsWith(ROUTER_SERVICE_PREFIX)) {
      throw new TypeError(
        `${service} belongs to one Router and this Mesh has several: pick it with mesh.at(router) (${this.routers.join(", ")})`,
      );
    }
    return mergeCallOptions(this.defaults, options);
  }

  private connection(index: number): RouterConnection {
    if (this.state.closed) throw new UnavailableError("Mesh is closed", { retryable: false, details: { source: "client" } });
    let connection = this.state.connections[index];
    if (connection === undefined) {
      connection = this.open(this.routers[index]!);
      this.state.connections[index] = connection;
    }
    return connection;
  }

  /** The Routers to try: where the instance was last reached, then from the last good one on. */
  private order(instance: string | undefined): number[] {
    const count = this.routers.length;
    const order = Array.from({ length: count }, (_, i) => (this.state.cursor + i) % count);
    const located = instance === undefined ? undefined : this.state.located.get(instance);
    return located === undefined ? order : [located, ...order.filter((i) => i !== located)];
  }

  /**
   * Run `attempt` on each Router in turn until one routes the call. A Router
   * that cannot be reached, or answers that it did not route the call, is
   * skipped; any other outcome is the call's. When no Router routed it and
   * one could not be reached, wait per the reconnect policy and go round
   * again within the call's timeout.
   */
  public async route<T>(
    instance: string | undefined,
    options: CallOptions,
    attempt: (connection: RouterConnection, options: CallOptions, router: string) => Promise<T>,
  ): Promise<T> {
    options = mergeCallOptions(this.defaults, options);
    const startedAt = performance.now();
    const remaining = (): number | undefined =>
      options.timeoutMs === undefined ? undefined : options.timeoutMs - (performance.now() - startedAt);
    const policy = this.options.reconnect ?? {};
    const delays = (policy.enabled ?? true) && this.state.everConnected ? reconnectDelays(policy) : undefined;
    for (;;) {
      let unreachable: unknown;
      let notRouted: unknown;
      for (const index of this.order(instance)) {
        const left = remaining();
        if (left !== undefined && left <= 0) throw new DeadlineExceededError("Call timed out", { details: { source: "client" } });
        const connection = this.connection(index);
        try {
          await within(connection.connect?.(), left);
          this.state.everConnected = true;
        } catch (error) {
          if (error instanceof CredentialSourceError || error instanceof DeadlineExceededError || !(error instanceof MeshCallError)) {
            throw error;
          }
          unreachable ??= error;
          continue;
        }
        try {
          const result = await attempt(connection, left === undefined ? options : { ...options, timeoutMs: left }, this.routers[index]!);
          this.state.cursor = index;
          if (instance !== undefined) this.state.located.set(instance, index);
          return result;
        } catch (error) {
          // Lost, draining or gone from there: find it again next time.
          if (instance !== undefined && error instanceof UnavailableError && this.state.located.get(instance) === index) {
            this.state.located.delete(instance);
          }
          if (!routerDidNotRoute(error)) throw error;
          if (notRouted === undefined || rank(error) < rank(notRouted)) notRouted = error;
        }
      }
      // Every Router answered that it did not route the call: that is the answer.
      if (unreachable === undefined) throw notRouted;
      const next = delays?.next();
      const left = remaining();
      if (next === undefined || next.done === true || this.state.closed || (left !== undefined && next.value >= left)) {
        throw unreachable;
      }
      await new Promise((resolve) => setTimeout(resolve, next.value));
    }
  }
}

async function within(promise: Promise<unknown> | undefined, budgetMs: number | undefined): Promise<void> {
  if (promise === undefined) return;
  if (budgetMs === undefined) {
    await promise;
    return;
  }
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new DeadlineExceededError("Call timed out", { details: { source: "client" } })), budgetMs);
    })]);
  } finally {
    clearTimeout(timer);
  }
}

/** The stream's first result; the stream is cancelled if it fails. */
async function firstResult<Item>(stream: RpcStream<Item>): Promise<IteratorResult<Item>> {
  try {
    return await stream.next();
  } catch (error) {
    await stream.cancel().catch(() => undefined);
    throw error;
  }
}

type Opened<Item> = { stream: RpcStream<Item>; first: Promise<IteratorResult<Item>> };

/**
 * A server stream opened on the first Router that routes it. Whether it was
 * routed is known from its first result, which is held for the caller.
 */
class RoutedStream<Item> implements RpcStream<Item> {
  private stream: RpcStream<Item> | undefined;
  private first: Promise<IteratorResult<Item>> | undefined;
  private opening: Promise<void> | undefined;
  private cancelled: string | undefined;

  public constructor(private readonly start: () => Promise<Opened<Item>>) {}

  public get trailers(): Readonly<Metadata> {
    return this.stream?.trailers ?? {};
  }

  protected opened(): Promise<void> {
    this.opening ??= this.start().then(async ({ stream, first }) => {
      this.stream = stream;
      this.first = first;
      if (this.cancelled !== undefined) await stream.cancel(this.cancelled);
    });
    return this.opening;
  }

  public async next(): Promise<IteratorResult<Item>> {
    await this.opened();
    const first = this.first;
    if (first !== undefined) {
      this.first = undefined;
      return first;
    }
    return this.stream!.next();
  }

  public async return(): Promise<IteratorResult<Item>> {
    await this.cancel();
    return { done: true, value: undefined };
  }

  public async cancel(reason?: string): Promise<void> {
    if (this.stream === undefined) {
      this.cancelled = reason ?? "cancelled";
      return;
    }
    await this.stream.cancel(reason);
  }

  public [Symbol.asyncIterator](): this {
    return this;
  }

  public async [Symbol.asyncDispose](): Promise<void> {
    await this.cancel();
  }
}

/**
 * The caller's items for a client stream that may be opened on more than one
 * Router. A runtime pulls the next item only after sending the previous one,
 * and sends nothing before the server grants credit; a Router that did not
 * route the call granted none, so only the item last pulled can be unsent,
 * and it is offered again to the next Router.
 */
class ReplayableInput<Item> {
  private readonly source: AsyncIterator<Item>;
  private held: IteratorResult<Item> | undefined;
  private done = false;

  public constructor(items: AsyncIterable<Item>) {
    this.source = items[Symbol.asyncIterator]();
  }

  public attempt(): AsyncIterable<Item> {
    let replay = this.held;
    return {
      [Symbol.asyncIterator]: () => ({
        next: async (): Promise<IteratorResult<Item>> => {
          if (replay !== undefined) {
            const value = replay;
            replay = undefined;
            return value;
          }
          if (this.done) return { done: true, value: undefined };
          const next = await this.source.next();
          this.held = next;
          if (next.done === true) this.done = true;
          return next;
        },
        // The source outlives one attempt; it is closed once the call ends.
        return: async (): Promise<IteratorResult<Item>> => ({ done: true, value: undefined }),
      }),
    };
  }

  public async close(): Promise<void> {
    if (!this.done) await this.source.return?.();
  }
}

interface PendingSend<Input> {
  readonly item: Input;
  readonly resolve: () => void;
  readonly reject: (error: unknown) => void;
  /** Settles with the caller's promise. */
  readonly settled: Promise<unknown>;
  /** Sending it on the current attempt failed with this. */
  failed?: unknown;
}

/**
 * A duplex call opened on the first Router that routes it. The call is
 * routed once a send completes (the server granted input credit) or it
 * produces a result; until then the caller's items are kept and, when a
 * Router did not route the call, sent again to the next one. A send resolves
 * once its item went out on the call that was routed.
 */
class RoutedDuplex<Input, Output> extends RoutedStream<Output> implements RpcDuplex<Input, Output> {
  private current: RpcDuplex<Input, Output> | undefined;
  private routed: RpcDuplex<Input, Output> | undefined;
  private failure: { error: unknown } | undefined;
  private readonly unsent: Array<PendingSend<Input>> = [];
  private sentOn: (() => void) | undefined;
  private halfClosing: Promise<void> | undefined;
  /** The call whose input was half-closed: once per call. */
  private closedOn: RpcDuplex<Input, Output> | undefined;

  public constructor(start: (probe: (call: RpcDuplex<Input, Output>) => Promise<Opened<Output>>) => Promise<Opened<Output>>) {
    super(() => start((call) => this.probe(call)).then((opened) => {
      this.routed = opened.stream as RpcDuplex<Input, Output>;
      for (const pending of [...this.unsent]) {
        if (pending.failed !== undefined) this.settle(pending, pending.failed);
      }
      return opened;
    }, (error: unknown) => {
      this.failure = { error };
      for (const pending of [...this.unsent]) this.settle(pending, error);
      throw error;
    }));
    // Opened at once, as a duplex call is; failures surface from next() and send().
    this.opened().catch(() => undefined);
  }

  /** Open the call on one Router: send what is kept, and wait until it is routed or fails. */
  private async probe(call: RpcDuplex<Input, Output>): Promise<Opened<Output>> {
    this.current = call;
    for (const pending of this.unsent) this.forward(call, pending);
    if (this.halfClosing !== undefined && this.unsent.length === 0) this.closeOn(call).catch(() => undefined);
    const first = call.next();
    first.catch(() => undefined);
    const sent = new Promise<void>((resolve) => { this.sentOn = resolve; });
    const outcome = await Promise.race([
      first.then(() => undefined, (error: unknown) => ({ error })),
      sent,
    ]);
    if (outcome !== undefined) {
      await call.cancel().catch(() => undefined);
      throw outcome.error;
    }
    return { stream: call, first };
  }

  private forward(call: RpcDuplex<Input, Output>, pending: PendingSend<Input>): void {
    pending.failed = undefined;
    call.send(pending.item).then(() => {
      if (this.current !== call) return;
      this.settle(pending);
      this.sentOn?.(); // credit was granted: the call is routed
    }, (error: unknown) => {
      if (this.current !== call) return;
      if (this.routed === call) this.settle(pending, error);
      else pending.failed = error; // decided once the attempt is
    });
  }

  private settle(pending: PendingSend<Input>, error?: unknown): void {
    const index = this.unsent.indexOf(pending);
    if (index < 0) return;
    this.unsent.splice(index, 1);
    if (error === undefined) pending.resolve();
    else pending.reject(error);
  }

  public send(item: Input): Promise<void> {
    if (this.halfClosing !== undefined) return Promise.reject(new MeshCallError("protocol_error", "Input stream is closed"));
    if (this.routed !== undefined) return this.routed.send(item);
    if (this.failure !== undefined) return Promise.reject(this.failure.error);
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<void>((res, rej) => { resolve = res; reject = rej; });
    const pending: PendingSend<Input> = { item, resolve, reject, settled: promise.catch(() => undefined) };
    this.unsent.push(pending);
    if (this.current !== undefined) this.forward(this.current, pending);
    return promise;
  }

  /** Half-close after the items sent before it: they settle once the call is routed. */
  public closeSend(): Promise<void> {
    this.halfClosing ??= (async () => {
      await Promise.all(this.unsent.map((pending) => pending.settled));
      // Not routed yet: the server may be waiting for the end of the input.
      if (this.routed === undefined && this.current !== undefined) this.closeOn(this.current).catch(() => undefined);
      await this.opened().catch(() => undefined);
      if (this.routed !== undefined) await this.closeOn(this.routed);
    })();
    return this.halfClosing;
  }

  private async closeOn(call: RpcDuplex<Input, Output>): Promise<void> {
    if (this.closedOn === call) return;
    this.closedOn = call;
    await call.closeSend();
  }
}

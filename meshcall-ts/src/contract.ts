import { randomUUID } from "node:crypto";
import { mkdir, rename, unlink, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

import { z } from "zod";

import {
  DependencyScope,
  validateInjectables,
  type Injectable,
} from "./di.js";
import { recordWarnings } from "./diagnostics.js";
import type { BalancePolicy } from "./protocol.js";
import type { DuplexChannel, RpcContext, ServiceDefinition, ServiceMethod } from "./service.js";

export type JsonSchema = Readonly<Record<string, unknown>>;

type Awaitable<T> = T | Promise<T>;
type Schema = z.ZodType;
export type InputMap = Readonly<Record<string, Schema>>;
export type InputDeclaration = InputMap | Schema;

/** Semantic hints shared by every method shape. */
export interface MethodSemantics {
  /**
   * Shown to humans and to LLM callers; keep it to one or two sentences.
   * Required: pass `""` to declare none on purpose.
   */
  readonly description: string;
  /** Safe to retry without an idempotency key. Implied by `readOnly`. */
  readonly idempotent?: boolean;
  readonly readOnly?: boolean;
  /** Irreversible side effects; tools must ask before calling. */
  readonly destructive?: boolean;
  /** Replacement to use instead; a short sentence when there is none. */
  readonly deprecated?: string;
  readonly balance?: BalancePolicy;
  /** Dependencies and metadata headers injected after business parameters. */
  readonly inject?: ReadonlyArray<Injectable<any>>;
}

export interface UnaryOptions<Request extends Schema, Response extends Schema> extends MethodSemantics {
  readonly request: Request;
  readonly response: Response;
}

export interface ServerStreamOptions<Request extends Schema, Item extends Schema> extends MethodSemantics {
  readonly request: Request;
  readonly outputItem: Item;
}

export interface ClientStreamOptions<Request extends Schema, Item extends Schema, Response extends Schema>
  extends MethodSemantics {
  readonly request: Request;
  readonly inputItem: Item;
  readonly response: Response;
}

/**
 * How a subscription resumes (docs/protocol.md → Subscriptions): `cursor` is
 * the item property holding each item's position, `resume` the optional
 * request property that continues after it. Both or neither.
 */
export type SubscriptionSpec<Request extends Schema, Item extends Schema> =
  | { readonly cursor: keyof z.output<Item> & string; readonly resume: keyof z.input<Request> & string }
  | { readonly cursor?: undefined; readonly resume?: undefined };

export type SubscriptionOptions<Request extends Schema, Item extends Schema> =
  ServerStreamOptions<Request, Item> & SubscriptionSpec<Request, Item>;

/** A duplex call is never replayed, so it has no idempotent / readOnly hint. */
export interface DuplexOptions<Request extends Schema, Input extends Schema, Output extends Schema>
  extends Omit<MethodSemantics, "idempotent" | "readOnly"> {
  /** Why a server stream plus unary calls, or a client stream, will not do. */
  readonly reason: string;
  readonly request: Request;
  readonly inputItem: Input;
  readonly outputItem: Output;
}

export interface QueryOptions<Response extends Schema = Schema> extends Omit<MethodSemantics, "readOnly" | "destructive"> {
  readonly input?: InputDeclaration;
  readonly request?: Schema;
  readonly output?: Response;
  readonly response?: Response;
}

export interface StreamOptions<Item extends Schema = Schema> extends Omit<MethodSemantics, "readOnly" | "destructive"> {
  readonly input?: InputDeclaration;
  readonly request?: Schema;
  readonly output?: Item;
  readonly outputItem?: Item;
}

export interface MutatesOptions extends MethodSemantics {
  readonly input?: InputDeclaration;
  readonly request?: Schema;
  readonly streamInput?: Schema;
  readonly inputItem?: Schema;
  readonly output?: Schema;
  readonly response?: Schema;
  readonly streamOutput?: Schema;
  readonly outputItem?: Schema;
  readonly reason?: string;
  readonly sideEffect?: string;
}

export interface ServiceOptions {
  /** What the service is for; required, `""` to declare none on purpose. */
  readonly description: string;
  /**
   * Every call must name the instance it is for (`instance` in the call
   * options): instances hold distinct state such as a session, a loaded
   * model or an actor.
   */
  readonly targeted?: boolean;
  readonly balance?: BalancePolicy;
}

export interface NamedServiceOptions extends ServiceOptions {
  readonly name: string;
}

type Stream = ServiceMethod["stream"];
export type BindingMode = "request_object" | "positional" | "single_schema";

/** One method as declared by a decorator. */
export interface MethodSpec extends Partial<Omit<MethodSemantics, "description">> {
  readonly name: string;
  readonly stream: Stream;
  readonly description: string;
  readonly request: Schema;
  readonly response?: Schema;
  readonly inputItem?: Schema;
  readonly outputItem?: Schema;
  readonly reason?: string;
  readonly subscription?: { readonly cursor?: string; readonly resume?: string };
  readonly bindingMode?: BindingMode;
  readonly parameterNames?: ReadonlyArray<string>;
}

/** A `@service` class's contract: everything but the handlers. */
export interface ServiceSpec {
  readonly name: string;
  readonly version: string;
  readonly description: string;
  readonly targeted: boolean;
  readonly balance?: BalancePolicy;
  readonly sourceModule: string;
  readonly sourceQualname: string;
  readonly methods: ReadonlyArray<MethodSpec>;
}

interface Pending {
  readonly fn: unknown;
  readonly spec: MethodSpec;
}

// Method decorators run before their class's decorator, so the class
// decorator collects what its methods recorded here.
let pending: Pending[] = [];
const specs = new WeakMap<object, ServiceSpec>();

function record(context: ClassMethodDecoratorContext<any>, fn: unknown, spec: Omit<MethodSpec, "name">): void {
  if (context.static || context.private || typeof context.name !== "string") {
    throw new Error("@method decorates public instance methods only");
  }
  if (spec.inject !== undefined && spec.inject.length > 0) {
    validateInjectables(spec.inject, context.name);
  }
  pending.push({ fn, spec: { ...spec, name: context.name } });
}

function isZodSchema(value: unknown): value is Schema {
  return value instanceof z.ZodType
    || (typeof value === "object" && value !== null && "safeParse" in value && typeof (value as { safeParse?: unknown }).safeParse === "function");
}

function normalizeInputDeclaration(
  input: InputDeclaration | undefined,
  request: Schema | undefined,
): { request: Schema; bindingMode: BindingMode; parameterNames?: ReadonlyArray<string> } {
  if (request !== undefined) {
    return { request, bindingMode: "request_object" };
  }
  if (input === undefined) {
    return { request: z.object({}), bindingMode: "positional", parameterNames: [] };
  }
  if (isZodSchema(input)) {
    return { request: input, bindingMode: "single_schema", parameterNames: ["request"] };
  }
  const names = Object.keys(input);
  return {
    request: z.object(input as Record<string, Schema>),
    bindingMode: "positional",
    parameterNames: names,
  };
}

function looksLikeDuplexHandler(fn: unknown, positionalParamCount: number): boolean {
  if (typeof fn !== "function") return false;
  const src = Function.prototype.toString.call(fn);
  if (src.includes(".send(") || src.includes(".closeSend(")) {
    return true;
  }
  const match = /^[^(]*\(([^)]*)\)/u.exec(src);
  if (match === null) return false;
  const rawParams = match[1]!
    .split(",")
    .map((p) => p.trim().split(/[=\s]/u)[0] ?? "")
    .filter((p) => p !== "");
  const streamParam = rawParams[positionalParamCount];
  return streamParam === "channel" || streamParam === "duplex";
}

/**
 * Shorthand decorator for read-only, idempotent unary RPC methods (`@query({...})`).
 */
export function query<Response extends Schema = Schema>(options: QueryOptions<Response>) {
  return <This>(
    fn: (this: This, ...args: any[]) => Awaitable<z.input<Response>>,
    context: ClassMethodDecoratorContext<This>,
  ): void => {
    const response = options.output ?? options.response;
    if (response === undefined) {
      throw new Error(`@query on ${String(context.name)} requires an output (or response) schema`);
    }
    const normalizedInput = normalizeInputDeclaration(options.input, options.request);
    record(context, fn, {
      description: options.description,
      stream: "unary",
      request: normalizedInput.request,
      response,
      bindingMode: normalizedInput.bindingMode,
      ...(normalizedInput.parameterNames !== undefined ? { parameterNames: normalizedInput.parameterNames } : {}),
      readOnly: true,
      idempotent: true,
      destructive: false,
      ...(options.deprecated !== undefined ? { deprecated: options.deprecated } : {}),
      ...(options.balance !== undefined ? { balance: options.balance } : {}),
      ...(options.inject !== undefined ? { inject: options.inject } : {}),
    });
  };
}

/**
 * Shorthand decorator for read-only, idempotent server-streaming RPC methods (`@stream({...})`).
 */
export function stream<Item extends Schema = Schema>(options: StreamOptions<Item>) {
  return <This>(
    fn: (this: This, ...args: any[]) => AsyncIterable<z.input<Item>> | Promise<AsyncIterable<z.input<Item>>>,
    context: ClassMethodDecoratorContext<This>,
  ): void => {
    const outputItem = options.output ?? options.outputItem;
    if (outputItem === undefined) {
      throw new Error(`@stream on ${String(context.name)} requires an output (or outputItem) schema`);
    }
    const normalizedInput = normalizeInputDeclaration(options.input, options.request);
    record(context, fn, {
      description: options.description,
      stream: "server_stream",
      request: normalizedInput.request,
      outputItem,
      bindingMode: normalizedInput.bindingMode,
      ...(normalizedInput.parameterNames !== undefined ? { parameterNames: normalizedInput.parameterNames } : {}),
      readOnly: true,
      idempotent: true,
      destructive: false,
      ...(options.deprecated !== undefined ? { deprecated: options.deprecated } : {}),
      ...(options.balance !== undefined ? { balance: options.balance } : {}),
      ...(options.inject !== undefined ? { inject: options.inject } : {}),
    });
  };
}

/**
 * Shorthand decorator for mutating RPC methods (unary, client stream, or duplex).
 */
export function mutates(options: MutatesOptions) {
  return <This>(
    fn: (this: This, ...args: any[]) => any,
    context: ClassMethodDecoratorContext<This>,
  ): void => {
    const normalizedInput = normalizeInputDeclaration(options.input, options.request);
    const inputItem = options.streamInput ?? options.inputItem;
    const idempotent = options.idempotent === true;
    const destructive = options.destructive ?? !idempotent;

    if (inputItem === undefined) {
      const response = options.output ?? options.response;
      if (response === undefined) {
        throw new Error(`@mutates on ${String(context.name)} requires an output (or response) schema`);
      }
      record(context, fn, {
        description: options.description,
        stream: "unary",
        request: normalizedInput.request,
        response,
        bindingMode: normalizedInput.bindingMode,
        ...(normalizedInput.parameterNames !== undefined ? { parameterNames: normalizedInput.parameterNames } : {}),
        readOnly: false,
        idempotent,
        destructive,
        ...(options.deprecated !== undefined ? { deprecated: options.deprecated } : {}),
        ...(options.balance !== undefined ? { balance: options.balance } : {}),
        ...(options.inject !== undefined ? { inject: options.inject } : {}),
      });
      return;
    }

    const paramCount = normalizedInput.parameterNames?.length ?? 1;
    const isExplicitDuplex =
      options.streamOutput !== undefined ||
      options.outputItem !== undefined ||
      options.reason !== undefined;
    const isExplicitClientStream = options.response !== undefined;
    const isDuplex = isExplicitDuplex || (!isExplicitClientStream && looksLikeDuplexHandler(fn, paramCount));

    if (isDuplex) {
      const outputItem = options.streamOutput ?? options.outputItem ?? options.output;
      if (outputItem === undefined) {
        throw new Error(`@mutates duplex on ${String(context.name)} requires an output (or streamOutput) schema`);
      }
      const reason = (options.reason ?? options.sideEffect ?? options.description).trim() || "Interactive bidirectional stream";
      record(context, fn, {
        description: options.description,
        stream: "duplex",
        request: normalizedInput.request,
        inputItem,
        outputItem,
        reason,
        bindingMode: normalizedInput.bindingMode,
        ...(normalizedInput.parameterNames !== undefined ? { parameterNames: normalizedInput.parameterNames } : {}),
        readOnly: false,
        idempotent: false,
        destructive: options.destructive ?? true,
        ...(options.deprecated !== undefined ? { deprecated: options.deprecated } : {}),
        ...(options.balance !== undefined ? { balance: options.balance } : {}),
        ...(options.inject !== undefined ? { inject: options.inject } : {}),
      });
      return;
    }

    const response = options.response ?? options.output;
    if (response === undefined) {
      throw new Error(`@mutates client stream on ${String(context.name)} requires an output (or response) schema`);
    }
    record(context, fn, {
      description: options.description,
      stream: "client_stream",
      request: normalizedInput.request,
      inputItem,
      response,
      bindingMode: normalizedInput.bindingMode,
      ...(normalizedInput.parameterNames !== undefined ? { parameterNames: normalizedInput.parameterNames } : {}),
      readOnly: false,
      idempotent,
      destructive,
      ...(options.deprecated !== undefined ? { deprecated: options.deprecated } : {}),
      ...(options.balance !== undefined ? { balance: options.balance } : {}),
      ...(options.inject !== undefined ? { inject: options.inject } : {}),
    });
  };
}

/**
 * A unary method: `(request, ctx) => response`. The handler receives the
 * parsed request (defaults applied, unknown fields dropped); what it returns
 * is validated against `response` before it is sent.
 */
export function method<Request extends Schema, Response extends Schema>(options: UnaryOptions<Request, Response>) {
  return <This>(
    fn: (this: This, request: z.output<Request>, context: RpcContext) => Awaitable<z.input<Response>>,
    context: ClassMethodDecoratorContext<This>,
  ): void => record(context, fn, { ...options, stream: "unary", bindingMode: "request_object" });
}

/** A server stream: an async generator yielding `outputItem`s. */
method.serverStream = <Request extends Schema, Item extends Schema>(options: ServerStreamOptions<Request, Item>) =>
  <This>(
    fn: (this: This, request: z.output<Request>, context: RpcContext) => AsyncIterable<z.input<Item>>,
    context: ClassMethodDecoratorContext<This>,
  ): void => record(context, fn, { ...options, stream: "server_stream", bindingMode: "request_object" });

/**
 * (Experimental) A subscription: a server stream meant to stay open, which clients reopen
 * after a retryable failure, passing the last item's `cursor` as `resume`.
 */
method.subscription = <Request extends Schema, Item extends Schema>(options: SubscriptionOptions<Request, Item>) =>
  <This>(
    fn: (this: This, request: z.output<Request>, context: RpcContext) => AsyncIterable<z.input<Item>>,
    context: ClassMethodDecoratorContext<This>,
  ): void => {
    const { cursor, resume, ...rest } = options;
    record(context, fn, {
      ...rest, stream: "server_stream", bindingMode: "request_object",
      subscription: cursor === undefined || resume === undefined ? {} : { cursor, resume },
    });
  };

/** A client stream: `(request, items, ctx) => response`. */
method.clientStream = <Request extends Schema, Item extends Schema, Response extends Schema>(
  options: ClientStreamOptions<Request, Item, Response>,
) =>
  <This>(
    fn: (this: This, request: z.output<Request>, items: AsyncIterable<z.output<Item>>, context: RpcContext)
      => Awaitable<z.input<Response>>,
    context: ClassMethodDecoratorContext<This>,
  ): void => record(context, fn, { ...options, stream: "client_stream", bindingMode: "request_object" });

/**
 * Input and output stream at the same time — the shape of last resort. A
 * duplex call is never retried or deduplicated, takes no idempotency key, is
 * not reachable over HTTP, pins one instance for its whole life and can
 * deadlock if a side only sends. Prefer a server stream plus unary calls, or
 * a client stream. `reason` says why this method needs it; it goes into the
 * contract. The handler iterates the channel, sends on it, and returns nothing.
 */
method.duplex = <Request extends Schema, Input extends Schema, Output extends Schema>(
  options: DuplexOptions<Request, Input, Output>,
) =>
  <This>(
    fn: (this: This, request: z.output<Request>, channel: DuplexChannel<z.output<Input>, z.input<Output>>,
      context: RpcContext) => Awaitable<void>,
    context: ClassMethodDecoratorContext<This>,
  ): void => {
    if (typeof options.reason !== "string" || options.reason.trim() === "") {
      throw new Error("@method.duplex needs reason: say why a server stream plus unary calls, or a client stream, will not do");
    }
    record(context, fn, { ...options, reason: options.reason.trim(), stream: "duplex", bindingMode: "request_object" });
  };

const METHOD_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/u;

/**
 * Declare a class as a MeshCall service. Its `@method`, `@query`, `@stream`,
 * or `@mutates`-decorated methods are the service's methods; pass instances
 * to `serve(new MyService())`, `run(new MyService())`, or `MeshCallServer({ services })`.
 */
export function service(name: string, options: ServiceOptions): (
  target: abstract new (...args: never[]) => unknown,
  context: ClassDecoratorContext,
) => void;
export function service(options: NamedServiceOptions): (
  target: abstract new (...args: never[]) => unknown,
  context: ClassDecoratorContext,
) => void;
export function service(
  nameOrOptions: string | NamedServiceOptions,
  maybeOptions?: ServiceOptions,
) {
  const rawName = typeof nameOrOptions === "string" ? nameOrOptions : nameOrOptions.name;
  const options: ServiceOptions = typeof nameOrOptions === "string"
    ? (maybeOptions ?? { description: "" })
    : nameOrOptions;
  return (target: abstract new (...args: never[]) => unknown, context: ClassDecoratorContext): void => {
    const prototype = target.prototype as Record<string, unknown>;
    const mine = pending.filter((entry) => prototype[entry.spec.name] === entry.fn);
    const strays = pending.filter((entry) => !mine.includes(entry));
    pending = [];
    if (strays.length > 0) {
      throw new Error("@method on " + strays.map((s) => s.spec.name).join(", ")
        + " outside a @service class: decorate that class with @service");
    }
    const normalized = normalizeServiceName(rawName);
    if (mine.length === 0) throw new Error(`Service ${rawName} does not declare any methods`);
    for (const { spec } of mine) {
      if (!METHOD_NAME.test(spec.name)) throw new Error(`Invalid cross-language method name: ${spec.name}`);
      if (spec.readOnly === true && spec.destructive === true) {
        throw new Error(`Method ${spec.name} cannot be both readOnly and destructive`);
      }
      if (spec.subscription?.resume !== undefined) checkResume(spec);
    }
    const spec: ServiceSpec = {
      name: normalized.name,
      version: normalized.version,
      description: options.description,
      targeted: options.targeted === true,
      ...(options.balance === undefined ? {} : { balance: options.balance }),
      sourceModule: "typescript",
      sourceQualname: String(context.name ?? target.name),
      methods: mine.map((entry) => entry.spec),
    };
    specs.set(target, recordWarnings(target, spec));
  };
}

function checkResume(spec: MethodSpec): void {
  const resume = spec.subscription?.resume as string;
  const shape = spec.request instanceof z.ZodObject ? spec.request.shape as Record<string, Schema> : {};
  const field = shape[resume];
  if (field === undefined) throw new Error(`subscription resume ${JSON.stringify(resume)} is not a request property`);
  if (!field.safeParse(undefined).success) {
    throw new Error(`subscription resume ${JSON.stringify(resume)} must be optional: the first call has no cursor yet`);
  }
}

/** Whether `value` is a `@service` class or an instance of one. */
export function isService(value: unknown): value is object {
  if ((typeof value !== "object" && typeof value !== "function") || value === null) return false;
  const target = typeof value === "function" ? value : (value as { constructor?: object }).constructor;
  return target !== undefined && specs.has(target);
}

/** The contract of a `@service` class or of one of its instances. */
export function serviceSpec(service: object): ServiceSpec {
  const target = typeof service === "function" ? service : service.constructor;
  const spec = specs.get(target);
  if (spec === undefined) {
    const name = (target as { name?: string }).name ?? typeof service;
    throw new TypeError(`${name} is not a @service class: decorate it with @service(name, { description })`);
  }
  return spec;
}

function extractRequestArgs(spec: MethodSpec, request: unknown): unknown[] {
  const mode = spec.bindingMode ?? "request_object";
  if (mode === "request_object" || mode === "single_schema") {
    return [request];
  }
  const names = spec.parameterNames ?? [];
  const obj = (request !== null && typeof request === "object") ? (request as Record<string, unknown>) : {};
  return names.map((name) => obj[name]);
}

function bindMethodHandler(instance: object, m: MethodSpec, fn: (...args: unknown[]) => unknown): ServiceMethod["handler"] {
  const mode = m.bindingMode ?? "request_object";
  const inject = m.inject ?? [];
  if (mode === "request_object" && inject.length === 0) {
    return ((...args: unknown[]) => fn.apply(instance, args)) as ServiceMethod["handler"];
  }

  if (m.stream === "unary") {
    return async (request: unknown, context: RpcContext) => {
      const reqArgs = extractRequestArgs(m, request);
      if (inject.length === 0) {
        return await fn.apply(instance, [...reqArgs, context]);
      }
      const scope = new DependencyScope(context);
      try {
        const injectedArgs = await scope.resolveAll(inject);
        return await fn.apply(instance, [...reqArgs, ...injectedArgs, context]);
      } finally {
        await scope.close();
      }
    };
  }

  if (m.stream === "server_stream") {
    return async function* (request: unknown, context: RpcContext) {
      const reqArgs = extractRequestArgs(m, request);
      const scope = new DependencyScope(context);
      try {
        const injectedArgs = await scope.resolveAll(inject);
        const rawIterable = await fn.apply(instance, [...reqArgs, ...injectedArgs, context]);
        const iterable = rawIterable as AsyncIterable<unknown>;
        for await (const item of iterable) {
          yield item;
        }
      } finally {
        await scope.close();
      }
    };
  }

  if (m.stream === "client_stream") {
    return async (request: unknown, items: AsyncIterable<unknown>, context: RpcContext) => {
      const reqArgs = extractRequestArgs(m, request);
      if (inject.length === 0) {
        return await fn.apply(instance, [...reqArgs, items, context]);
      }
      const scope = new DependencyScope(context);
      try {
        const injectedArgs = await scope.resolveAll(inject);
        return await fn.apply(instance, [...reqArgs, items, ...injectedArgs, context]);
      } finally {
        await scope.close();
      }
    };
  }

  // duplex
  return async (request: unknown, channel: DuplexChannel<unknown, unknown>, context: RpcContext) => {
    const reqArgs = extractRequestArgs(m, request);
    if (inject.length === 0) {
      await fn.apply(instance, [...reqArgs, channel, context]);
      return;
    }
    const scope = new DependencyScope(context);
    try {
      const injectedArgs = await scope.resolveAll(inject);
      await fn.apply(instance, [...reqArgs, channel, ...injectedArgs, context]);
    } finally {
      await scope.close();
    }
  };
}

/** The runtime definition of a service instance: its handlers bound to it. */
export function bindService(instance: object): { spec: ServiceSpec; definition: ServiceDefinition } {
  if (typeof instance === "function") {
    throw new TypeError(`Pass an instance of ${instance.name} (new ${instance.name}(…)), not the class`);
  }
  const spec = serviceSpec(instance);
  const methods: Record<string, ServiceMethod> = {};
  for (const m of spec.methods) {
    const fn = (instance as Record<string, (...args: unknown[]) => unknown>)[m.name] as (...args: unknown[]) => unknown;
    const handler = bindMethodHandler(instance, m, fn);
    methods[m.name] = { stream: m.stream, handler, ...(m.balance === undefined ? {} : { balance: m.balance }),
      ...(m.reason === undefined ? {} : { reason: m.reason }) } as ServiceMethod;
  }
  return {
    spec,
    definition: {
      name: spec.name, methods, ...(spec.targeted ? { targeted: true } : {}),
      ...(spec.balance === undefined ? {} : { balance: spec.balance }),
    },
  };
}

const NAME_SEGMENT = /^[A-Za-z][A-Za-z0-9_]*$/u;
const MAJOR_SEGMENT = /^v(0|[1-9][0-9]*)$/u;
const NUMERIC_SEGMENT = /^(0|[1-9][0-9]*)$/u;
const PATCH_SEGMENT = /^(0|[1-9][0-9]*)(-[0-9A-Za-z-]+)?$/u;

/**
 * Normalise a service name to its full form and extract its version.
 * `chat` → `chat.v0`; `chat.v1`, `demo.v1.Greeting`, `chat.v1.2.0-beta1` stay.
 */
export function normalizeServiceName(name: string): { name: string; version: string } {
  const segments = name.split(".");
  const normalized: string[] = [];
  let version: string | undefined;
  for (let index = 0; index < segments.length;) {
    const segment = segments[index] as string;
    if (MAJOR_SEGMENT.test(segment)) {
      if (version !== undefined) throw new Error("Service name " + JSON.stringify(name) + " has more than one version segment");
      const minor = segments[index + 1];
      const patch = segments[index + 2];
      if (minor !== undefined && patch !== undefined && NUMERIC_SEGMENT.test(minor) && PATCH_SEGMENT.test(patch)) {
        version = segment.slice(1) + "." + minor + "." + patch;
        normalized.push(segment, minor, patch);
        index += 3;
      } else {
        version = segment.slice(1);
        normalized.push(segment);
        index += 1;
      }
      continue;
    }
    if (!NAME_SEGMENT.test(segment)) {
      throw new Error("Invalid service name " + JSON.stringify(name) + ": segment " + JSON.stringify(segment)
        + " must start with a letter and contain only letters, digits and underscores (a version looks like 'v1' or 'v1.2.0')");
    }
    normalized.push(segment);
    index += 1;
  }
  if (version === undefined) {
    version = "0";
    normalized.push("v0");
  }
  if (!normalized.some((part) => NAME_SEGMENT.test(part) && !MAJOR_SEGMENT.test(part))) {
    throw new Error("Service name " + JSON.stringify(name) + " needs a package or service segment");
  }
  return { name: normalized.join("."), version };
}

/** The portable contract entry for one service, as `renderContract` emits it. */
export function contractEntry(service: object): Record<string, unknown> {
  const spec = serviceSpec(service);
  const serviceBalance = normalizeBalance(spec.balance);
  const prefix = spec.sourceQualname;
  return {
    name: spec.name,
    version: spec.version,
    source_module: spec.sourceModule,
    source_qualname: spec.sourceQualname,
    balance: serviceBalance,
    description: spec.description,
    targeted: spec.targeted,
    methods: spec.methods.map((m) => {
      const base = prefix + pascal(m.name);
      const request = typeRef(m.request, "input", base + "Request", spec.sourceModule);
      const parameters = m.parameterNames !== undefined
        ? [...m.parameterNames]
        : Object.keys((request.schema.properties as Record<string, unknown> | undefined) ?? {});
      return {
        name: m.name,
        stream: m.stream,
        request,
        parameters,
        response: m.response === undefined ? null : typeRef(m.response, "output", base + "Response", spec.sourceModule),
        input_item: m.inputItem === undefined ? null : typeRef(m.inputItem, "input", base + "InputItem", spec.sourceModule),
        output_item: m.outputItem === undefined ? null : typeRef(m.outputItem, "output", base + "OutputItem", spec.sourceModule),
        balance: normalizeBalance(m.balance ?? spec.balance),
        description: m.description,
        annotations: {
          idempotent: m.idempotent === true || m.readOnly === true,
          read_only: m.readOnly === true,
          destructive: m.destructive === true,
        },
        deprecated: m.deprecated ?? null,
        duplex_reason: m.stream === "duplex" ? m.reason : null,
        subscription: m.subscription === undefined
          ? null
          : { cursor: m.subscription.cursor ?? null, resume: m.subscription.resume ?? null },
      };
    }),
  };
}

export function renderContract(services: ReadonlyArray<object>): string {
  const document = { protocol: "meshcall/1", services: services.map(contractEntry) };
  return `${JSON.stringify(document, null, 2)}\n`;
}

/** Write the contract document `meshcall generate` reads. Takes classes or instances. */
export async function writeContract(services: ReadonlyArray<object>, output: string): Promise<string> {
  const target = resolve(output);
  await mkdir(dirname(target), { recursive: true });
  const temporary = `${target}.${process.pid}.${randomUUID()}.tmp`;
  try {
    await writeFile(temporary, renderContract(services), "utf8");
    await rename(temporary, target);
  } catch (error) {
    await unlink(temporary).catch(() => undefined);
    throw error;
  }
  return target;
}

/**
 * A schema as the contract carries it: requests and input items in their
 * input form (defaults optional), responses and output items in their output
 * form. A schema with `.meta({ id })` is named by its id; others by the
 * method. Objects do not forbid unknown properties: peers ignore fields they
 * do not know (docs/protocol.md → Compatibility rule).
 */
function typeRef(schema: Schema, io: "input" | "output", fallback: string, module: string) {
  let document: Record<string, unknown>;
  try {
    document = z.toJSONSchema(schema, { io, target: "draft-2020-12", unrepresentable: "throw" }) as Record<string, unknown>;
  } catch {
    document = z.toJSONSchema(schema, { io, target: "draft-2020-12", unrepresentable: "any" }) as Record<string, unknown>;
    if (Object.keys(document).filter((k) => k !== "$schema").length === 0) {
      document = { type: "string", format: "binary" };
    }
  }
  delete document.$schema;
  let qualname = fallback;
  const defs = document.$defs as Record<string, Record<string, unknown>> | undefined;
  const ref = document.$ref;
  let root = document;
  if (typeof ref === "string" && ref.startsWith("#/$defs/") && Object.keys(document).length <= 2 && defs !== undefined) {
    qualname = ref.slice("#/$defs/".length);
    const { [qualname]: own, ...rest } = defs;
    root = { ...own, ...(Object.keys(rest).length > 0 ? { $defs: rest } : {}) };
  }
  return { module, qualname, schema: openObjects(root) as JsonSchema };
}

function openObjects(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(openObjects);
  if (value === null || typeof value !== "object") return value;
  const out: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value)) {
    if (key === "additionalProperties" && item === false) continue;
    // z.int() states the safe-integer range on every integer; it is noise to
    // the other languages' generators.
    if ((key === "minimum" && item === Number.MIN_SAFE_INTEGER) || (key === "maximum" && item === Number.MAX_SAFE_INTEGER)) continue;
    out[key] = openObjects(item);
  }
  return out;
}

function pascal(name: string): string {
  return name.split(/[_\W]+/u).filter(Boolean).map((part) => part[0]!.toUpperCase() + part.slice(1)).join("");
}

function normalizeBalance(balance?: BalancePolicy): Required<BalancePolicy> {
  return {
    kind: balance?.kind ?? "round_robin",
    key: balance?.key ?? null,
  };
}

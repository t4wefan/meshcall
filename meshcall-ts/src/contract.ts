import { randomUUID } from "node:crypto";
import { mkdir, rename, unlink, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

import { recordWarnings } from "./diagnostics.js";
import type { BalancePolicy } from "./protocol.js";
import type {
  ClientStreamMethod, DuplexMethod, ServerStreamMethod, ServiceDefinition, UnaryMethod,
} from "./service.js";
export type { ServiceDefinition } from "./service.js";

export type JsonSchema = Readonly<Record<string, unknown>>;

export interface TypeContract<Value> {
  readonly module: string;
  readonly qualname: string;
  readonly schema: JsonSchema;
  readonly __valueType?: (value: Value) => Value;
}

/** Semantic hints shared by every method shape. */
export interface MethodSemantics {
  /** Shown to humans and to LLM callers; keep it to one or two sentences. */
  readonly description?: string;
  /** Safe to retry without an idempotency key. Implied by `readOnly`. */
  readonly idempotent?: boolean;
  readonly readOnly?: boolean;
  /** Irreversible side effects; tools must ask before calling. */
  readonly destructive?: boolean;
  /** Replacement to use instead; empty string when there is none. */
  readonly deprecated?: string;
}

export interface ContractUnaryMethod<Request, Response>
  extends UnaryMethod<Request, Response>, MethodSemantics {
  readonly request: TypeContract<Request>;
  readonly response: TypeContract<Response>;
}

/**
 * How a subscription resumes (docs/protocol.md → Subscriptions): `cursor` is
 * the item property holding each item's position, `resume` the optional
 * request property that continues after it. Both or neither.
 */
export interface SubscriptionSpec {
  readonly cursor?: string;
  readonly resume?: string;
}

export interface ContractServerStreamMethod<Request, Item>
  extends ServerStreamMethod<Request, Item>, MethodSemantics {
  readonly request: TypeContract<Request>;
  readonly outputItem: TypeContract<Item>;
  /** Set by `subscriptionMethod`. */
  readonly subscription?: SubscriptionSpec;
}

export interface ContractClientStreamMethod<Request, Item, Response>
  extends ClientStreamMethod<Request, Item, Response>, MethodSemantics {
  readonly request: TypeContract<Request>;
  readonly inputItem: TypeContract<Item>;
  readonly response: TypeContract<Response>;
}

/** A duplex call is never replayed, so it has no idempotent / readOnly hint. */
export interface ContractDuplexMethod<Request, Input, Output>
  extends DuplexMethod<Request, Input, Output>, Omit<MethodSemantics, "idempotent" | "readOnly"> {
  readonly request: TypeContract<Request>;
  readonly inputItem: TypeContract<Input>;
  readonly outputItem: TypeContract<Output>;
}

export type ContractMethod =
  | ContractUnaryMethod<any, any>
  | ContractServerStreamMethod<any, any>
  | ContractClientStreamMethod<any, any, any>
  | ContractDuplexMethod<any, any, any>;

export type ExportableServiceDefinition<
  Methods extends Readonly<Record<string, ContractMethod>> = Readonly<
    Record<string, ContractMethod>
  >,
> = Omit<ServiceDefinition, "methods"> & {
  readonly sourceModule: string;
  readonly sourceQualname: string;
  readonly balance?: BalancePolicy;
  readonly description?: string;
  readonly methods: Methods;
};

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

export function defineType<Value>(
  qualname: string,
  schema: JsonSchema,
  options: { readonly module?: string } = {},
): TypeContract<Value> {
  if (qualname.length === 0) {
    throw new Error("Type qualname cannot be empty");
  }
  return {
    module: options.module ?? "typescript",
    qualname,
    schema,
  };
}

export function unaryMethod<Request, Response>(
  definition: Omit<ContractUnaryMethod<Request, Response>, "stream"> & { readonly description: string },
): ContractUnaryMethod<Request, Response>;
/**
 * @deprecated W001: add `description` — it becomes the contract's description
 * and the text LLM callers see. Pass `description: ""` to declare none on purpose.
 */
export function unaryMethod<Request, Response>(
  definition: Omit<ContractUnaryMethod<Request, Response>, "stream">,
): ContractUnaryMethod<Request, Response>;
export function unaryMethod<Request, Response>(
  definition: Omit<ContractUnaryMethod<Request, Response>, "stream">,
): ContractUnaryMethod<Request, Response> {
  return { ...definition, stream: "unary" };
}

export function serverStreamMethod<Request, Item>(
  definition: Omit<ContractServerStreamMethod<Request, Item>, "stream"> & { readonly description: string },
): ContractServerStreamMethod<Request, Item>;
/**
 * @deprecated W001: add `description` — it becomes the contract's description
 * and the text LLM callers see. Pass `description: ""` to declare none on purpose.
 */
export function serverStreamMethod<Request, Item>(
  definition: Omit<ContractServerStreamMethod<Request, Item>, "stream">,
): ContractServerStreamMethod<Request, Item>;
export function serverStreamMethod<Request, Item>(
  definition: Omit<ContractServerStreamMethod<Request, Item>, "stream">,
): ContractServerStreamMethod<Request, Item> {
  return { ...definition, stream: "server_stream" };
}

/**
 * A subscription: a server stream meant to stay open, which clients reopen
 * after a retryable failure, passing the last item's `cursor` as `resume`.
 */
export function subscriptionMethod<Request, Item>(
  definition: Omit<ContractServerStreamMethod<Request, Item>, "stream" | "subscription"> & SubscriptionSpec
    & { readonly description: string },
): ContractServerStreamMethod<Request, Item>;
/**
 * @deprecated W001: add `description` — it becomes the contract's description
 * and the text LLM callers see. Pass `description: ""` to declare none on purpose.
 */
export function subscriptionMethod<Request, Item>(
  definition: Omit<ContractServerStreamMethod<Request, Item>, "stream" | "subscription"> & SubscriptionSpec,
): ContractServerStreamMethod<Request, Item>;
export function subscriptionMethod<Request, Item>(
  definition: Omit<ContractServerStreamMethod<Request, Item>, "stream" | "subscription"> & SubscriptionSpec,
): ContractServerStreamMethod<Request, Item> {
  const { cursor, resume, ...rest } = definition;
  if ((cursor === undefined) !== (resume === undefined)) {
    throw new Error("subscriptionMethod takes cursor and resume together (or neither)");
  }
  if (cursor !== undefined && resume !== undefined) {
    const items = (definition.outputItem.schema.properties ?? {}) as Record<string, unknown>;
    const request = (definition.request.schema.properties ?? {}) as Record<string, unknown>;
    const required = (definition.request.schema.required ?? []) as string[];
    if (!(cursor in items)) throw new Error(`subscription cursor ${JSON.stringify(cursor)} is not a property of ${definition.outputItem.qualname}`);
    if (!(resume in request)) throw new Error(`subscription resume ${JSON.stringify(resume)} is not a request property`);
    if (required.includes(resume)) {
      throw new Error(`subscription resume ${JSON.stringify(resume)} must be optional: the first call has no cursor yet`);
    }
  }
  const subscription: SubscriptionSpec = cursor !== undefined && resume !== undefined ? { cursor, resume } : {};
  return { ...rest, stream: "server_stream", subscription };
}

export function clientStreamMethod<Request, Item, Response>(
  definition: Omit<ContractClientStreamMethod<Request, Item, Response>, "stream"> & { readonly description: string },
): ContractClientStreamMethod<Request, Item, Response>;
/**
 * @deprecated W001: add `description` — it becomes the contract's description
 * and the text LLM callers see. Pass `description: ""` to declare none on purpose.
 */
export function clientStreamMethod<Request, Item, Response>(
  definition: Omit<ContractClientStreamMethod<Request, Item, Response>, "stream">,
): ContractClientStreamMethod<Request, Item, Response>;
export function clientStreamMethod<Request, Item, Response>(
  definition: Omit<ContractClientStreamMethod<Request, Item, Response>, "stream">,
): ContractClientStreamMethod<Request, Item, Response> {
  return { ...definition, stream: "client_stream" };
}

/**
 * Input and output stream at the same time — the shape of last resort. A
 * duplex call is never retried or deduplicated, takes no idempotency key, is
 * not reachable over HTTP, pins one instance for its whole life and can
 * deadlock if a side only sends. Prefer a server stream plus unary calls, or
 * a client stream. `reason` says why this method needs it; it goes into the
 * contract. The handler receives a `DuplexChannel` and returns nothing.
 */
export function duplexMethod<Request, Input, Output>(
  definition: Omit<ContractDuplexMethod<Request, Input, Output>, "stream"> & { readonly description: string },
): ContractDuplexMethod<Request, Input, Output>;
/**
 * @deprecated W001: add `description` — it becomes the contract's description
 * and the text LLM callers see. Pass `description: ""` to declare none on purpose.
 */
export function duplexMethod<Request, Input, Output>(
  definition: Omit<ContractDuplexMethod<Request, Input, Output>, "stream">,
): ContractDuplexMethod<Request, Input, Output>;
export function duplexMethod<Request, Input, Output>(
  definition: Omit<ContractDuplexMethod<Request, Input, Output>, "stream">,
): ContractDuplexMethod<Request, Input, Output> {
  if (typeof definition.reason !== "string" || definition.reason.trim() === "") {
    throw new Error("duplexMethod needs reason: say why a server stream plus unary calls, or a client stream, will not do");
  }
  return { ...definition, reason: definition.reason.trim(), stream: "duplex" };
}

export function defineService<
  const Methods extends Readonly<Record<string, ContractMethod>>,
>(
  definition: ExportableServiceDefinition<Methods>,
): ExportableServiceDefinition<Methods> {
  const { name } = normalizeServiceName(definition.name);
  if (Object.keys(definition.methods).length === 0) {
    throw new Error(`Service ${definition.name} does not declare any methods`);
  }
  for (const methodName of Object.keys(definition.methods)) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(methodName)) {
      throw new Error(`Invalid cross-language method name: ${methodName}`);
    }
    const method = definition.methods[methodName] as MethodSemantics;
    if (method.readOnly === true && method.destructive === true) {
      throw new Error(`Method ${methodName} cannot be both readOnly and destructive`);
    }
    if ((method as { stream?: string }).stream === "duplex" && (method.readOnly === true || method.idempotent === true)) {
      throw new Error(`Duplex method ${methodName} cannot be idempotent or readOnly: a duplex call is never replayed`);
    }
  }
  return recordWarnings(name === definition.name ? definition : { ...definition, name });
}

/** The portable contract entry for one service, as `renderContract` emits it. */
export function contractEntry(service: ExportableServiceDefinition): Record<string, unknown> {
  const serviceBalance = normalizeBalance(service.balance);
  const normalized = normalizeServiceName(service.name);
  return {
    name: normalized.name,
    version: normalized.version,
    source_module: service.sourceModule,
    source_qualname: service.sourceQualname,
    balance: serviceBalance,
    description: service.description ?? null,
    targeted: service.targeted === true,
    methods: Object.entries(service.methods).map(([name, method]) => {
      const semantics = method as MethodSemantics;
      return {
        name,
        stream: method.stream,
        request: renderTypeRef(method.request),
        // Parameters are the request object's properties, in schema order.
        parameters: Object.keys((method.request.schema.properties as Record<string, unknown> | undefined) ?? {}),
        response: method.stream === "unary" || method.stream === "client_stream" ? renderTypeRef(method.response) : null,
        input_item: method.stream === "client_stream" || method.stream === "duplex" ? renderTypeRef(method.inputItem) : null,
        output_item: method.stream === "server_stream" || method.stream === "duplex" ? renderTypeRef(method.outputItem) : null,
        balance: normalizeBalance(method.balance ?? serviceBalance),
        description: method.description ?? null,
        annotations: {
          idempotent: semantics.idempotent === true || semantics.readOnly === true,
          read_only: semantics.readOnly === true,
          destructive: method.destructive === true,
        },
        deprecated: method.deprecated ?? null,
        duplex_reason: method.stream === "duplex" ? method.reason : null,
        subscription: method.stream === "server_stream" && method.subscription !== undefined
          ? { cursor: method.subscription.cursor ?? null, resume: method.subscription.resume ?? null }
          : null,
      };
    }),
  };
}

/**
 * The contract entry for a service definition if it is exportable (has
 * `sourceModule`/`sourceQualname` and typed contracts on every method).
 */
export function tryContractEntry(service: ServiceDefinition): Record<string, unknown> | undefined {
  const candidate = service as Partial<ExportableServiceDefinition>;
  if (typeof candidate.sourceModule !== "string" || typeof candidate.sourceQualname !== "string") return undefined;
  for (const method of Object.values(service.methods)) {
    const typed = method as Partial<ContractUnaryMethod<unknown, unknown>>;
    if (typed.request === undefined) return undefined;
  }
  try {
    return contractEntry(service as ExportableServiceDefinition);
  } catch {
    return undefined;
  }
}

export function renderContract(
  services: ReadonlyArray<ExportableServiceDefinition>,
): string {
  const document = { protocol: "meshcall/1", services: services.map(contractEntry) };
  return `${JSON.stringify(document, null, 2)}\n`;
}

export async function writeContract(
  services: ReadonlyArray<ExportableServiceDefinition>,
  output: string,
): Promise<string> {
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

function renderTypeRef(type: TypeContract<unknown>): Record<string, unknown> {
  return {
    module: type.module,
    qualname: type.qualname,
    schema: type.schema,
  };
}

function normalizeBalance(balance?: BalancePolicy): Required<BalancePolicy> {
  return {
    kind: balance?.kind ?? "round_robin",
    key: balance?.key ?? null,
  };
}

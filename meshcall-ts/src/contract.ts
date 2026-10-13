import { randomUUID } from "node:crypto";
import { mkdir, rename, unlink, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

import { recordWarnings } from "./diagnostics.js";
import type { BalancePolicy } from "./protocol.js";
import type {
  ClientStreamMethod, ServerStreamMethod, ServiceDefinition, UnaryMethod,
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

export interface ContractServerStreamMethod<Request, Item>
  extends ServerStreamMethod<Request, Item>, MethodSemantics {
  readonly request: TypeContract<Request>;
  readonly outputItem: TypeContract<Item>;
}

export interface ContractClientStreamMethod<Request, Item, Response>
  extends ClientStreamMethod<Request, Item, Response>, MethodSemantics {
  readonly request: TypeContract<Request>;
  readonly inputItem: TypeContract<Item>;
  readonly response: TypeContract<Response>;
}

export type ContractMethod =
  | ContractUnaryMethod<any, any>
  | ContractServerStreamMethod<any, any>
  | ContractClientStreamMethod<any, any, any>;

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
    methods: Object.entries(service.methods).map(([name, method]) => ({
      name,
      stream: method.stream,
      request: renderTypeRef(method.request),
      // Parameters are the request object's properties, in schema order.
      parameters: Object.keys((method.request.schema.properties as Record<string, unknown> | undefined) ?? {}),
      response: method.stream === "server_stream" ? null : renderTypeRef(method.response),
      input_item: method.stream === "client_stream" ? renderTypeRef(method.inputItem) : null,
      output_item: method.stream === "server_stream" ? renderTypeRef(method.outputItem) : null,
      balance: normalizeBalance(method.balance ?? serviceBalance),
      description: method.description ?? null,
      annotations: {
        idempotent: method.idempotent === true || method.readOnly === true,
        read_only: method.readOnly === true,
        destructive: method.destructive === true,
      },
      deprecated: method.deprecated ?? null,
    })),
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

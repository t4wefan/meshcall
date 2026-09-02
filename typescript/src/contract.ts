import { randomUUID } from "node:crypto";
import { mkdir, rename, unlink, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

import type { BalancePolicy } from "./protocol.js";
import type { ServiceDefinition, UnaryMethod } from "./server.js";

export type JsonSchema = Readonly<Record<string, unknown>>;

export interface TypeContract<Value> {
  readonly module: string;
  readonly qualname: string;
  readonly schema: JsonSchema;
  readonly __valueType?: (value: Value) => Value;
}

export interface ContractUnaryMethod<Request, Response>
  extends UnaryMethod<Request, Response> {
  readonly request: TypeContract<Request>;
  readonly response: TypeContract<Response>;
}

type AnyContractUnaryMethod = ContractUnaryMethod<any, any>;

export type ExportableServiceDefinition<
  Methods extends Readonly<Record<string, AnyContractUnaryMethod>> = Readonly<
    Record<string, AnyContractUnaryMethod>
  >,
> = Omit<ServiceDefinition, "methods"> & {
  readonly sourceModule: string;
  readonly sourceQualname: string;
  readonly balance?: BalancePolicy;
  readonly methods: Methods;
};

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
  definition: Omit<ContractUnaryMethod<Request, Response>, "stream">,
): ContractUnaryMethod<Request, Response> {
  return { ...definition, stream: "unary" };
}

export function defineService<
  const Methods extends Readonly<Record<string, AnyContractUnaryMethod>>,
>(
  definition: ExportableServiceDefinition<Methods>,
): ExportableServiceDefinition<Methods> {
  if (!/^[A-Za-z][A-Za-z0-9_.]*$/.test(definition.name)) {
    throw new Error(`Invalid service name: ${JSON.stringify(definition.name)}`);
  }
  if (Object.keys(definition.methods).length === 0) {
    throw new Error(`Service ${definition.name} does not declare any methods`);
  }
  for (const methodName of Object.keys(definition.methods)) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(methodName)) {
      throw new Error(`Invalid cross-language method name: ${methodName}`);
    }
  }
  return definition;
}

export function renderContract(
  services: ReadonlyArray<ExportableServiceDefinition>,
): string {
  const document = {
    protocol: "meshcall/1",
    services: services.map((service) => {
      const serviceBalance = normalizeBalance(service.balance);
      return {
        name: service.name,
        source_module: service.sourceModule,
        source_qualname: service.sourceQualname,
        balance: serviceBalance,
        methods: Object.entries(service.methods).map(([name, method]) => ({
          name,
          stream: "unary",
          request: renderTypeRef(method.request),
          response: renderTypeRef(method.response),
          input_item: null,
          output_item: null,
          balance: normalizeBalance(method.balance ?? serviceBalance),
          request_style: "model",
          request_fields: [],
        })),
      };
    }),
  };
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

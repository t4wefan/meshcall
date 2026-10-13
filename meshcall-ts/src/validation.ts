import { Ajv2020, type ErrorObject, type ValidateFunction } from "ajv/dist/2020.js";
import addFormats from "ajv-formats";

import type { JsonSchema, TypeContract } from "./contract.js";
import { InternalError, InvalidArgumentError, ProtocolError, type Violation } from "./errors.js";
import type { ServiceMethod } from "./service.js";

export interface MethodValidators {
  request: ValidateFunction | undefined;
  response: ValidateFunction | undefined;
  input: ValidateFunction | undefined;
  output: ValidateFunction | undefined;
}

export function compileMethod(method: ServiceMethod): MethodValidators {
  const ajv = new Ajv2020({ strict: false, useDefaults: true });
  addFormats.default(ajv);
  const typed = method as ServiceMethod & {
    request?: TypeContract<unknown>;
    response?: TypeContract<unknown>;
    inputItem?: TypeContract<unknown>;
    outputItem?: TypeContract<unknown>;
  };
  const compile = (type: TypeContract<unknown> | undefined) =>
    type === undefined ? undefined : ajv.compile(type.schema);
  return {
    request: compile(typed.request),
    response: compile(typed.response),
    input: compile(typed.inputItem),
    output: compile(typed.outputItem),
  };
}

/** What was validated; decides who is at fault (docs/protocol.md → Validation failures). */
export type ValidationSubject = "request" | "input_item" | "response" | "output_item";

/**
 * The service produced a response or output item that breaks its own contract.
 * Carries the violations for the server-side log only; the wire sees `internal`.
 */
export class InvalidOutputError extends InternalError {
  public constructor(public readonly subject: "response" | "output_item", public readonly violations: Violation[]) {
    super("Internal service error", {
      details: { source: "server", reason: subject === "response" ? "invalid_response" : "invalid_output_item" },
    });
    this.name = "InvalidOutputError";
  }
}

export function validatePayload(validate: ValidateFunction | undefined, value: unknown, subject: ValidationSubject): unknown {
  if (validate === undefined || validate(value)) return value;
  const violations = (validate.errors ?? []).map(violationOf);
  if (subject === "response" || subject === "output_item") {
    throw new InvalidOutputError(subject, violations);
  }
  throw new InvalidArgumentError(subject === "request" ? "Invalid request" : "Invalid input stream item", {
    details: { source: "server", reason: subject, violations },
  });
}

const compiled = new WeakMap<JsonSchema, ValidateFunction>();

/** Compile a schema once per object; generated clients hold schemas as module constants. */
export function compileSchema(schema: JsonSchema): ValidateFunction {
  let validate = compiled.get(schema);
  if (validate === undefined) {
    const ajv = new Ajv2020({ strict: false });
    addFormats.default(ajv);
    validate = ajv.compile(schema);
    compiled.set(schema, validate);
  }
  return validate;
}

/**
 * Client-side check of what the server sent against the client's contract.
 * A mismatch is version skew between the two, reported as `protocol_error`
 * from the client (docs/protocol.md → Validation failures).
 */
export function checkReceived(schema: JsonSchema | undefined, value: unknown, subject: "response" | "output_item"): void {
  if (schema === undefined) return;
  const validate = compileSchema(schema);
  if (validate(value)) return;
  throw new ProtocolError(subject === "response" ? "Invalid response" : "Invalid output stream item", {
    details: {
      source: "client",
      reason: subject === "response" ? "invalid_response" : "invalid_output_item",
      violations: (validate.errors ?? []).map(violationOf),
    },
  });
}

/** Redact an Ajv error to the same `(path, code, message)` shape Python sends. */
function violationOf(error: ErrorObject): Violation {
  let path = error.instancePath.split("/").filter((part) => part !== "").join(".");
  let code = error.keyword;
  if (error.keyword === "required") {
    const missing = String((error.params as { missingProperty?: unknown }).missingProperty ?? "");
    path = path === "" ? missing : path + "." + missing;
    code = "missing";
  }
  return { path, code, message: error.message ?? code };
}

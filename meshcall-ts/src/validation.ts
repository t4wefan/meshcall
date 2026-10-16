import { Ajv2020, type ErrorObject, type ValidateFunction } from "ajv/dist/2020.js";
import addFormats from "ajv-formats";
import type { z } from "zod";

import type { JsonSchema, MethodSpec } from "./contract.js";
import { InternalError, InvalidArgumentError, ProtocolError, type Violation } from "./errors.js";

/** The zod schemas a server validates one method's payloads with. */
export interface MethodValidators {
  request: z.ZodType | undefined;
  response: z.ZodType | undefined;
  input: z.ZodType | undefined;
  output: z.ZodType | undefined;
}

export function methodValidators(method: MethodSpec): MethodValidators {
  return { request: method.request, response: method.response, input: method.inputItem, output: method.outputItem };
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

/**
 * Parse a payload: the parsed value (defaults applied, unknown fields dropped)
 * or the error for whoever is at fault.
 */
export function validatePayload(schema: z.ZodType | undefined, value: unknown, subject: ValidationSubject): unknown {
  if (schema === undefined) return value;
  const result = schema.safeParse(value);
  if (result.success) return result.data;
  const violations = result.error.issues.map((issue) => zodViolation(issue, value));
  if (subject === "response" || subject === "output_item") {
    throw new InvalidOutputError(subject, violations);
  }
  throw new InvalidArgumentError(subject === "request" ? "Invalid request" : "Invalid input stream item", {
    details: { source: "server", reason: subject, violations },
  });
}

/** A zod issue in the `(path, code, message)` shape every runtime sends. */
function zodViolation(issue: z.core.$ZodIssue, root: unknown): Violation {
  const path = issue.path.map(String).join(".");
  let code: string = issue.code;
  if (issue.code === "invalid_type") {
    let at: unknown = root;
    for (const key of issue.path) at = at !== null && typeof at === "object" ? (at as Record<PropertyKey, unknown>)[key] : undefined;
    code = at === undefined ? "missing" : "type";
  }
  return { path, code, message: issue.message };
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

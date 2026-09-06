import { Ajv2020, type ValidateFunction } from "ajv/dist/2020.js";
import addFormats from "ajv-formats";

import type { TypeContract } from "./contract.js";
import { MeshCallError } from "./errors.js";
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

export function validatePayload(
  validate: ValidateFunction | undefined,
  value: unknown,
  label: string,
): unknown {
  if (validate !== undefined && !validate(value)) {
    throw new MeshCallError("invalid_argument", "Invalid " + label, {
      details: {
        errors: validate.errors?.map(({ instancePath, keyword, message }) => ({
          path: instancePath, keyword, message,
        })) ?? [],
      },
    });
  }
  return value;
}

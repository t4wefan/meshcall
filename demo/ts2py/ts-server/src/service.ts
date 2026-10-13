import {
  clientStreamMethod,
  defineService,
  defineType,
  serverStreamMethod,
  unaryMethod,
} from "meshcall";

export interface GreetingRequest {
  name: string;
  repeat: number;
}

export interface GreetingResult {
  message: string;
  handled_by: string;
}

const greetingRequest = defineType<GreetingRequest>("GreetingRequest", {
  type: "object",
  properties: {
    name: { type: "string" },
    repeat: { type: "integer", minimum: 1, maximum: 5 },
  },
  required: ["name", "repeat"],
  additionalProperties: false,
}, { module: "ts2py_service" });

const greetingResult = defineType<GreetingResult>("GreetingResult", {
  type: "object",
  properties: {
    message: { type: "string" },
    handled_by: { type: "string" },
  },
  required: ["message", "handled_by"],
  additionalProperties: false,
}, { module: "ts2py_service" });

const countRequest = defineType<{ count: number }>("CountRequest", {
  type: "object",
  properties: { count: { type: "integer", minimum: 0, maximum: 1000 } },
  required: ["count"], additionalProperties: false,
}, { module: "ts2py_service" });

const numberItem = defineType<{ value: number }>("NumberItem", {
  type: "object", properties: { value: { type: "integer" } },
  required: ["value"], additionalProperties: false,
}, { module: "ts2py_service" });

const sumRequest = defineType<{ offset: number }>("SumRequest", {
  type: "object", properties: { offset: { type: "integer", default: 0 } },
  additionalProperties: false,
}, { module: "ts2py_service" });

const sumResult = defineType<{ total: number }>("SumResult", {
  type: "object", properties: { total: { type: "integer" } },
  required: ["total"], additionalProperties: false,
}, { module: "ts2py_service" });

export const service = defineService({
  name: "demo.v1.TypeScriptGreetingService",
  description: "A TypeScript service consumed by the generated Python client.",
  sourceModule: "ts2py_service",
  sourceQualname: "TypeScriptGreetingService",
  methods: {
    count: serverStreamMethod({
      description: "Stream the integers from 0 up to, but not including, count.",
      readOnly: true,
      request: countRequest, outputItem: numberItem,
      handler: async function* ({ count }, { signal }) {
        for (let value = 0; value < count; value += 1) {
          signal.throwIfAborted();
          yield { value };
        }
      },
    }),
    sum: clientStreamMethod({
      description: "Add every streamed value to offset and return the total.",
      idempotent: true,
      request: sumRequest, inputItem: numberItem, response: sumResult,
      handler: async ({ offset }, items, { logger }) => {
        let total = offset;
        for await (const item of items) total += item.value;
        logger.info("summed input stream");
        return { total };
      },
    }),
    greet: unaryMethod({
      description: "Greet name repeat times and say which runtime answered.",
      readOnly: true,
      request: greetingRequest,
      response: greetingResult,
      handler: async (payload) => ({
        message: Array.from({ length: payload.repeat }, () => `Hello, ${payload.name}!`).join(" "),
        handled_by: "typescript",
      }),
    }),
  },
});

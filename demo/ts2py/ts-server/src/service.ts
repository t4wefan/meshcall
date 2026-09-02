import {
  defineService,
  defineType,
  unaryMethod,
} from "@meshcall/runtime";

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

export const service = defineService({
  name: "demo.v1.TypeScriptGreetingService",
  sourceModule: "ts2py_service",
  sourceQualname: "TypeScriptGreetingService",
  methods: {
    greet: unaryMethod({
      request: greetingRequest,
      response: greetingResult,
      handler: async (payload) => ({
        message: Array.from({ length: payload.repeat }, () => `Hello, ${payload.name}!`).join(" "),
        handled_by: "typescript",
      }),
    }),
  },
});

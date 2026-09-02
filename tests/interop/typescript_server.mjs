import { pathToFileURL } from "node:url";

const [runtimeIndex, contractPath] = process.argv.slice(2);
if (runtimeIndex === undefined || contractPath === undefined) {
  throw new Error("Expected runtime index and contract output path");
}

const {
  MeshCallServer,
  defineService,
  defineType,
  unaryMethod,
  writeContract,
} = await import(pathToFileURL(runtimeIndex).href);

const request = defineType(
  "TypeScriptRequest",
  {
    type: "object",
    properties: {
      value: { type: "integer" },
      factor: { type: "integer" },
    },
    required: ["value", "factor"],
    additionalProperties: false,
  },
  { module: "typescript_interop_service" },
);

const result = defineType(
  "TypeScriptResult",
  {
    type: "object",
    properties: {
      product: { type: "integer" },
      handled_by: { type: "string" },
    },
    required: ["product", "handled_by"],
    additionalProperties: false,
  },
  { module: "typescript_interop_service" },
);

const service = defineService({
  name: "test.v1.TypeScriptInteropService",
  sourceModule: "typescript_interop_service",
  sourceQualname: "TypeScriptInteropService",
  methods: {
    multiply: unaryMethod({
      request,
      response: result,
      handler: async (payload) => ({
        product: payload.value * payload.factor,
        handled_by: "typescript",
      }),
    }),
  },
});

await writeContract([service], contractPath);
const server = new MeshCallServer({ services: [service] });
await server.start();
process.stdout.write(`${JSON.stringify({ port: server.boundPort })}\n`);

process.stdin.resume();
await new Promise((resolve) => process.stdin.once("data", resolve));
process.stdin.pause();
await server.close();

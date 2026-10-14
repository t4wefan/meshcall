import { pathToFileURL } from "node:url";

const [runtimeIndex, contractPath] = process.argv.slice(2);
if (runtimeIndex === undefined || contractPath === undefined) {
  throw new Error("Expected runtime index and contract output path");
}

const {
  MeshCallServer,
  clientStreamMethod,
  defineService,
  defineType,
  unaryMethod,
  serverStreamMethod,
  writeContract,
} = await import(pathToFileURL(runtimeIndex).href);
const options = JSON.parse(process.argv[4] ?? "{}");
const handledBy = options.router?.instanceId ?? "typescript";

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

const item = defineType("TypeScriptItem", {
  type: "object",
  properties: { value: { type: "integer" }, handled_by: { type: "string" } },
  required: ["value", "handled_by"],
  additionalProperties: false,
}, { module: "typescript_interop_service" });

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
        handled_by: handledBy,
      }),
    }),
    download: serverStreamMethod({
      request, outputItem: item,
      handler: async function* (payload) {
        for (let value = 0; value < payload.value; value += 1) {
          yield { value: value * payload.factor, handled_by: handledBy };
        }
      },
    }),
    upload: clientStreamMethod({
      request, inputItem: item, response: result,
      handler: async (payload, items) => {
        let product = payload.value;
        for await (const item of items) product += item.value * payload.factor;
        return { product, handled_by: handledBy };
      },
    }),
  },
});

await writeContract([service], contractPath);
const server = new MeshCallServer({ services: [service], accessLog: false, ...options });
await server.start();
process.stdout.write(JSON.stringify({
  port: options.router || options.unixPath ? null : server.boundPort,
}) + "\n");

process.stdin.resume();
await new Promise((resolve) => process.stdin.once("data", resolve));
process.stdin.pause();
await server.close();

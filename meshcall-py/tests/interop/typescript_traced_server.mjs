// A TypeScript service with OpenTelemetry tracing, registered with a Router;
// its `whereami` method reports the trace and parent span it runs in, so a
// Python caller can check that one trace spans both languages.
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

const [runtimeIndex, routerUrl] = process.argv.slice(2);
const load = (name) => import(pathToFileURL(createRequire(runtimeIndex).resolve(name)).href);
const { context, propagation, trace } = await load("@opentelemetry/api");
const { AsyncLocalStorageContextManager } = await load("@opentelemetry/context-async-hooks");
const { W3CTraceContextPropagator } = await load("@opentelemetry/core");
const { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } = await load("@opentelemetry/sdk-trace-base");
const { MeshCallServer, defineService, defineType, unaryMethod } = await import(pathToFileURL(runtimeIndex).href);
const { otelServerInterceptor } = await import(pathToFileURL(runtimeIndex.replace(/index\.js$/, "otel.js")).href);

context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
propagation.setGlobalPropagator(new W3CTraceContextPropagator());
const spans = new InMemorySpanExporter();
const tracerProvider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(spans)] });

const empty = defineType("Empty", { type: "object", properties: {} });
const where = defineType("Where", {
  type: "object", properties: { trace_id: { type: "string" }, parent_span_id: { type: "string" } },
  required: ["trace_id", "parent_span_id"],
});
const service = defineService({
  name: "test.v1.Traced", description: "", sourceModule: "traced", sourceQualname: "Traced",
  methods: {
    whereami: unaryMethod({
      description: "", readOnly: true, request: empty, response: where,
      handler: () => {
        const current = trace.getSpan(context.active());
        return {
          trace_id: current?.spanContext().traceId ?? "",
          parent_span_id: current?.parentSpanContext?.spanId ?? "",
        };
      },
    }),
  },
});

const server = new MeshCallServer({
  services: [service], accessLog: false, router: { endpoint: routerUrl },
  interceptors: [otelServerInterceptor({ tracerProvider })],
});
await server.start();
process.stdout.write("ready\n");
process.stdin.resume();
await new Promise((resolve) => process.stdin.once("data", resolve));
process.stdin.pause();
await server.close();

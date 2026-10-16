// A TypeScript service with OpenTelemetry tracing, registered with a Router;
// its `whereami` method reports the trace and parent span it runs in, so a
// Python caller can check that one trace spans both languages.
import { context, propagation, trace } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";

import { MeshCallServer, method, service, z } from "../../src/index.js";
import { otelServerInterceptor } from "../../src/otel.js";

const [routerUrl] = process.argv.slice(2);
if (routerUrl === undefined) throw new Error("Expected the Router URL");

context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
propagation.setGlobalPropagator(new W3CTraceContextPropagator());
const spans = new InMemorySpanExporter();
const tracerProvider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(spans)] });

const Where = z.object({ trace_id: z.string(), parent_span_id: z.string() }).meta({ id: "Where" });

@service("test.v1.Traced", { description: "" })
class Traced {
  @method({ description: "", readOnly: true, request: z.object({}), response: Where })
  async whereami() {
    const current = trace.getSpan(context.active());
    return {
      trace_id: current?.spanContext().traceId ?? "",
      // The SDK span carries its parent; the API type does not declare it.
      parent_span_id: (current as { parentSpanContext?: { spanId: string } } | undefined)?.parentSpanContext?.spanId ?? "",
    };
  }
}

const server = new MeshCallServer({
  services: [new Traced()], accessLog: false, router: { endpoint: routerUrl },
  interceptors: [otelServerInterceptor({ tracerProvider })],
});
await server.start();
process.stdout.write("ready\n");
process.stdin.resume();
await new Promise((resolve) => process.stdin.once("data", resolve));
process.stdin.pause();
await server.close();

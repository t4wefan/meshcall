import assert from "node:assert/strict";
import test from "node:test";

import { context, propagation, SpanKind, trace } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { AggregationTemporality, InMemoryMetricExporter, MeterProvider, PeriodicExportingMetricReader } from "@opentelemetry/sdk-metrics";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";

import { MeshCallClient, MeshCallError, MeshCallServer, method, service, z, type RpcContext } from "../src/index.js";
import { otelClientInterceptor, otelServerInterceptor } from "../src/otel.js";

const Text = z.object({ text: z.string() });
const Echo = z.object({ text: z.string(), traceparent: z.string(), traceId: z.string() });
type Text = z.infer<typeof Text>;
type Echo = z.infer<typeof Echo>;

@service("test.v1.Traced", { description: "" })
class Traced {
  @method({ description: "", readOnly: true, request: Text, response: Echo })
  echo({ text }: Text, ctx: RpcContext) {
    return {
      text, traceparent: ctx.metadata.traceparent ?? "",
      traceId: trace.getSpan(context.active())?.spanContext().traceId ?? "",
    };
  }

  @method({ description: "", readOnly: true, request: Text, response: Echo })
  fail(): Echo { throw new MeshCallError("app.nope", "no"); }
}

test("one trace spans client and server; metrics count calls by code", { timeout: 10000 }, async (t) => {
  // What an OpenTelemetry Node SDK registers: the active span follows awaits.
  context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
  propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  const spans = new InMemorySpanExporter();
  const tracerProvider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(spans)] });
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 60_000 });
  const meterProvider = new MeterProvider({ readers: [reader] });
  const options = { tracerProvider, meterProvider };

  const server = new MeshCallServer({ services: [new Traced()], accessLog: false, interceptors: [otelServerInterceptor(options)] });
  await server.start();
  const client = new MeshCallClient("ws://127.0.0.1:" + server.boundPort, undefined, {
    interceptors: [otelClientInterceptor(options)],
  });
  t.after(async () => { await client.close(); await server.close(); });

  const echo = await client.unary<Text, Echo>("test.v1.Traced", "echo", { text: "hi" });
  const finished = spans.getFinishedSpans();
  const clientSpan = finished.find((s) => s.kind === SpanKind.CLIENT)!;
  const serverSpan = finished.find((s) => s.kind === SpanKind.SERVER)!;
  const traceId = clientSpan.spanContext().traceId;
  assert.equal(echo.traceparent.split("-")[1], traceId);
  assert.equal(echo.traceId, traceId); // the handler runs inside the server span
  assert.equal(serverSpan.spanContext().traceId, traceId);
  assert.equal(serverSpan.parentSpanContext?.spanId, clientSpan.spanContext().spanId);
  assert.equal(serverSpan.name, "test.v1.Traced/echo");
  assert.equal(serverSpan.attributes["rpc.system"], "meshcall");

  await assert.rejects(client.unary("test.v1.Traced", "fail", { text: "" }));
  const failed = spans.getFinishedSpans().filter((s) => s.name.endsWith("/fail"));
  assert.deepEqual(new Set(failed.map((s) => s.attributes["rpc.meshcall.error_code"])), new Set(["app.nope"]));

  await reader.forceFlush();
  const calls = new Map<string, number>();
  for (const resource of exporter.getMetrics()) {
    for (const scope of resource.scopeMetrics) {
      for (const metric of scope.metrics) {
        if (metric.descriptor.name !== "meshcall.calls") continue;
        for (const point of metric.dataPoints) {
          const a = point.attributes;
          calls.set(`${String(a["rpc.method"])}/${String(a["meshcall.code"])}/${String(a["meshcall.side"])}`, point.value as number);
        }
      }
    }
  }
  assert.equal(calls.get("echo/ok/client"), 1);
  assert.equal(calls.get("echo/ok/server"), 1);
  assert.equal(calls.get("fail/app.nope/server"), 1);
});

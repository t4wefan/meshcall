/**
 * OpenTelemetry for MeshCall calls: spans, metrics and trace propagation.
 *
 *   import { otelClientInterceptor, otelServerInterceptor } from "meshcall/otel";
 *   new MeshCallClient(url, undefined, { interceptors: [otelClientInterceptor()] });
 *   new MeshCallServer({ services, interceptors: [otelServerInterceptor()] });
 *
 * Needs `@opentelemetry/api` (an optional peer dependency); with no SDK
 * registered everything is a no-op. Trace context travels as call metadata
 * (`traceparent` / `tracestate`, W3C), so a trace continues across languages
 * and through the Router. Spans and metrics match the Python `meshcall.otel`:
 * a span per call named `service/method` (`rpc.system = "meshcall"`,
 * `rpc.service`, `rpc.method`, `rpc.meshcall.error_code` on failure), and
 * `meshcall.calls` / `meshcall.call.duration` (seconds) by service, method,
 * code and side. A server span covers the whole call; a client span covers a
 * unary or client-stream call, and only opening a stream.
 */
import {
  context, metrics, propagation, SpanKind, SpanStatusCode, trace,
  type MeterProvider, type Span, type TracerProvider,
} from "@opentelemetry/api";

import type { ClientInterceptor } from "./client.js";
import { MeshCallError } from "./errors.js";
import type { ServerInterceptor } from "./service.js";

export interface OtelOptions {
  readonly tracerProvider?: TracerProvider;
  readonly meterProvider?: MeterProvider;
}

const SCOPE = "meshcall";

function instruments(options: OtelOptions) {
  const meter = (options.meterProvider ?? metrics.getMeterProvider()).getMeter(SCOPE);
  return {
    calls: meter.createCounter("meshcall.calls", { unit: "{call}", description: "MeshCall calls by result" }),
    duration: meter.createHistogram("meshcall.call.duration", { unit: "s", description: "MeshCall call duration" }),
  };
}

function errorCode(error: unknown): string {
  if (error instanceof MeshCallError) return error.code;
  return "internal";
}

function finish(
  span: Span, meters: ReturnType<typeof instruments>, attributes: Record<string, string>,
  started: number, error: unknown, side: "client" | "server",
): void {
  const code = error === undefined ? "ok" : errorCode(error);
  if (error !== undefined) {
    span.setAttribute("rpc.meshcall.error_code", code);
    span.setStatus({ code: SpanStatusCode.ERROR, message: error instanceof Error ? error.message : String(error) });
  }
  const labels = { ...attributes, "meshcall.code": code, "meshcall.side": side };
  meters.calls.add(1, labels);
  meters.duration.record((performance.now() - started) / 1000, labels);
  span.end();
}

/** Spans and metrics for outgoing calls; injects the trace context into the call metadata. */
export function otelClientInterceptor(options: OtelOptions = {}): ClientInterceptor {
  const tracer = (options.tracerProvider ?? trace.getTracerProvider()).getTracer(SCOPE);
  const meters = instruments(options);
  return async (info, next) => {
    const attributes = { "rpc.system": "meshcall", "rpc.service": info.service, "rpc.method": info.method };
    const span = tracer.startSpan(info.service + "/" + info.method, { kind: SpanKind.CLIENT, attributes });
    const active = trace.setSpan(context.active(), span);
    const carrier: Record<string, string> = { ...(info.options.metadata ?? {}) };
    propagation.inject(active, carrier);
    info.options = { ...info.options, metadata: carrier };
    const started = performance.now();
    try {
      const result = await context.with(active, () => next(info));
      finish(span, meters, attributes, started, undefined, "client");
      return result;
    } catch (error) {
      finish(span, meters, attributes, started, error, "client");
      throw error;
    }
  };
}

/** Spans and metrics for incoming calls, continuing the caller's trace. */
export function otelServerInterceptor(options: OtelOptions = {}): ServerInterceptor {
  const tracer = (options.tracerProvider ?? trace.getTracerProvider()).getTracer(SCOPE);
  const meters = instruments(options);
  return async (ctx, _request, next) => {
    const attributes = { "rpc.system": "meshcall", "rpc.service": ctx.service, "rpc.method": ctx.method };
    const parent = propagation.extract(context.active(), { ...ctx.metadata });
    const span = tracer.startSpan(ctx.service + "/" + ctx.method, { kind: SpanKind.SERVER, attributes }, parent);
    const started = performance.now();
    try {
      const result = await context.with(trace.setSpan(parent, span), next);
      finish(span, meters, attributes, started, undefined, "server");
      return result;
    } catch (error) {
      finish(span, meters, attributes, started, error, "server");
      throw error;
    }
  };
}

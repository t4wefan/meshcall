import assert from "node:assert/strict";
import test from "node:test";

import {
  defineService, defineType, MeshCallClient, MeshCallError, MeshCallServer, serverStreamMethod,
  unaryMethod, type ClientCallInfo, type ClientCallNext, type RpcContext, type ServerCallNext,
} from "../src/index.js";

interface Value { value: number }
interface Echo { value: number; seen: Record<string, string> }

const valueType = defineType<Value>("Value", {
  type: "object", properties: { value: { type: "integer" } }, required: ["value"],
});
const echoType = defineType<Echo>("Echo", {
  type: "object",
  properties: { value: { type: "integer" }, seen: { type: "object", additionalProperties: { type: "string" } } },
  required: ["value", "seen"],
});

const service = defineService({
  name: "test.v1.InterceptService",
  sourceModule: "interceptors.test", sourceQualname: "InterceptService",
  methods: {
    echo: unaryMethod({
      request: valueType, response: echoType,
      handler: (request, ctx) => {
        ctx.setTrailer("handler", "ran");
        return { value: request.value, seen: { ...ctx.metadata } };
      },
    }),
    count: serverStreamMethod({
      request: valueType, outputItem: valueType,
      handler: async function* ({ value }) {
        for (let index = 0; index < value; index += 1) yield { value: index };
      },
    }),
  },
});

test("server interceptors run outermost-first, short-circuit, and span whole streams", { timeout: 5000 }, async (t) => {
  const events: string[] = [];
  const outer = async (ctx: RpcContext, _request: unknown, next: ServerCallNext) => {
    events.push("outer:" + ctx.method + ":" + (ctx.metadata.who ?? "-"));
    ctx.setTrailer("outer", "yes");
    const result = await next();
    events.push("outer:done");
    return result;
  };
  const guard = async (ctx: RpcContext, _request: unknown, next: ServerCallNext) => {
    if (ctx.metadata.who === "intruder") throw new MeshCallError("permission_denied", "not you");
    events.push("guard:pass");
    return next();
  };
  const server = new MeshCallServer({ services: [service], accessLog: false, interceptors: [outer, guard] });
  await server.start();
  const client = new MeshCallClient("ws://127.0.0.1:" + server.boundPort);
  t.after(async () => { await client.close(); await server.close(); });

  const echoed = await client.unary<Value, Echo>(service.name, "echo", { value: 1 }, { metadata: { who: "friend" } });
  assert.deepEqual(echoed, { value: 1, seen: { who: "friend" } });
  assert.deepEqual(events, ["outer:echo:friend", "guard:pass", "outer:done"]);

  events.length = 0;
  await assert.rejects(
    client.unary(service.name, "echo", { value: 0 }, { metadata: { who: "intruder" } }),
    (error: unknown) => error instanceof MeshCallError && error.code === "permission_denied",
  );
  assert.deepEqual(events, ["outer:echo:intruder"]);

  events.length = 0;
  const stream = client.serverStream<Value, Value>(service.name, "count", { value: 3 });
  const seen: number[] = [];
  for await (const item of stream) seen.push(item.value);
  assert.deepEqual(seen, [0, 1, 2]);
  assert.deepEqual(events, ["outer:count:-", "guard:pass", "outer:done"]);
  assert.deepEqual(stream.trailers, { outer: "yes" });
});

test("client interceptors propagate metadata, see trailers, and can retry", { timeout: 5000 }, async (t) => {
  const seen: Array<[string, string, Record<string, string>]> = [];
  const tracing = async (info: ClientCallInfo, next: ClientCallNext) => {
    info.options = { ...info.options, metadata: { ...info.options.metadata, traceparent: "00-trace" } };
    const result = await next(info);
    seen.push([info.stream, info.method, { ...info.trailers }]);
    return result;
  };
  let attempts = 0;
  const retryOnce = async (info: ClientCallInfo, next: ClientCallNext) => {
    attempts += 1;
    try {
      return await next(info);
    } catch (error) {
      if (error instanceof MeshCallError && error.retryable && attempts === 1) {
        attempts += 1;
        return next(info);
      }
      throw error;
    }
  };
  const server = new MeshCallServer({ services: [service], accessLog: false });
  await server.start();
  const client = new MeshCallClient("ws://127.0.0.1:" + server.boundPort, undefined, {
    interceptors: [retryOnce, tracing],
  });
  t.after(async () => { await client.close(); await server.close(); });

  const echoed = await client.unary<Value, Echo>(service.name, "echo", { value: 2 });
  assert.deepEqual(echoed.seen, { traceparent: "00-trace" });
  assert.deepEqual(seen, [["unary", "echo", { handler: "ran" }]]);
  const stream = client.serverStream<Value, Value>(service.name, "count", { value: 2 });
  const items: number[] = [];
  for await (const item of stream) items.push(item.value);
  assert.deepEqual(items, [0, 1]);
  assert.deepEqual(seen[1]?.slice(0, 2), ["server_stream", "count"]);
  assert.equal(attempts, 2);
});

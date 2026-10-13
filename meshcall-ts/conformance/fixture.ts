/** Reference TypeScript fixture for the conformance suite (see meshcall-router/conformance/FIXTURE.md). */
import { setTimeout as delay } from "node:timers/promises";

import {
  clientStreamMethod, defineService, defineType, MeshCallError, MeshCallServer, serverStreamMethod, unaryMethod,
} from "../src/index.js";

const object = (properties: Record<string, unknown>, required: string[] = []) =>
  ({ type: "object", properties, required, additionalProperties: false });

const echoType = defineType<{ value: number; text: string }>("Echo", object({ value: { type: "integer" }, text: { type: "string" } }, ["value", "text"]));
const failType = defineType<{ code: string; message: string; retryable?: boolean }>("Fail", object({ code: { type: "string" }, message: { type: "string" }, retryable: { type: "boolean", default: false } }, ["code", "message"]));
const okType = defineType<{ ok: boolean }>("Ok", object({ ok: { type: "boolean" } }, ["ok"]));
const slowType = defineType<{ ms: number; tag?: string }>("Slow", object({ ms: { type: "integer" }, tag: { type: "string", default: "" } }, ["ms"]));
const tagType = defineType<{ tag: string }>("Tag", object({ tag: { type: "string" } }, ["tag"]));
const emptyType = defineType<Record<string, never>>("Empty", object({}));
const tagsType = defineType<{ tags: string[] }>("Tags", object({ tags: { type: "array", items: { type: "string" } } }, ["tags"]));
const countType = defineType<{ n: number; gap_ms?: number }>("Count", object({ n: { type: "integer" }, gap_ms: { type: "integer", default: 0 } }, ["n"]));
const indexType = defineType<{ index: number }>("Index", object({ index: { type: "integer" } }, ["index"]));
const sumType = defineType<{ offset?: number }>("Sum", object({ offset: { type: "integer", default: 0 } }));
const valueType = defineType<{ value: number }>("Value", object({ value: { type: "integer" } }, ["value"]));
const totalType = defineType<{ total: number }>("Total", object({ total: { type: "integer" } }, ["total"]));
const metadataType = defineType<{ metadata: Record<string, string> }>("Metadata", object({ metadata: { type: "object", additionalProperties: { type: "string" } } }, ["metadata"]));
const drainType = defineType<{ delay_ms?: number }>("Drain", object({ delay_ms: { type: "integer", default: 0 } }));

const cancelled: string[] = [];
const invoked: string[] = [];
let server: MeshCallServer;

const fixture = defineService({
  name: "conformance.v1.Fixture",
  description: "Deterministic behaviours the conformance suite drives through the Router.",
  sourceModule: "conformance", sourceQualname: "Fixture",
  methods: {
    echo: unaryMethod({ description: "Return the input unchanged.", readOnly: true, request: echoType, response: echoType, handler: (r) => r }),
    fail: unaryMethod({
      description: "Raise an application error with the given code.", idempotent: true, request: failType, response: okType,
      handler: ({ code, message, retryable }) => { throw new MeshCallError(code, message, { retryable: retryable ?? false, details: { from: "fixture" } }); },
    }),
    bad_response: unaryMethod({
      description: "Return a response that breaks this method's own contract.", readOnly: true, request: emptyType, response: okType,
      handler: () => ({ ok: "not-a-bool" } as unknown as { ok: boolean }),
    }),
    slow: unaryMethod({
      description: "Sleep ms milliseconds; record tag if cancelled meanwhile.", idempotent: true, request: slowType, response: tagType,
      handler: async ({ ms, tag }, { signal }) => {
        invoked.push(tag ?? "");
        try {
          await delay(ms, undefined, { signal });
        } catch (error) {
          if (signal.aborted) cancelled.push(tag ?? "");
          throw error;
        }
        return { tag: tag ?? "" };
      },
    }),
    invoked_tags: unaryMethod({ description: "Tags of every slow call that started, in order.", readOnly: true, request: emptyType, response: tagsType, handler: () => ({ tags: [...invoked] }) }),
    cancelled_tags: unaryMethod({ description: "Tags of slow calls that observed cancellation.", readOnly: true, request: emptyType, response: tagsType, handler: () => ({ tags: [...cancelled] }) }),
    count: serverStreamMethod({
      description: "Yield 0..n-1 with gap_ms between items.", readOnly: true, request: countType, outputItem: indexType,
      handler: async function* ({ n, gap_ms }, { signal }) {
        for (let index = 0; index < n; index += 1) {
          if (gap_ms) await delay(gap_ms, undefined, { signal });
          yield { index };
        }
      },
    }),
    sum: clientStreamMethod({
      description: "Add every streamed value to offset.", idempotent: true, request: sumType, inputItem: valueType, response: totalType,
      handler: async ({ offset }, items) => { let total = offset ?? 0; for await (const item of items) total += item.value; return { total }; },
    }),
    metadata_echo: unaryMethod({
      description: "Return the call metadata and set trailers describing it.", readOnly: true, request: emptyType, response: metadataType,
      handler: (_, ctx) => { ctx.setTrailer("echoed", "yes"); ctx.setTrailer("count", String(Object.keys(ctx.metadata).length)); return { metadata: { ...ctx.metadata } }; },
    }),
    drain_self: unaryMethod({
      description: "Drain and stop this instance after delay_ms.", destructive: true, request: drainType, response: okType,
      handler: ({ delay_ms }) => {
        void delay(delay_ms ?? 0).then(() => server.close({ drain: true, drainTimeoutMs: 30_000 })).then(() => process.exit(0));
        return { ok: true };
      },
    }),
  },
});

const ping = Number(process.env.MESHCALL_PING_INTERVAL_MS ?? "15000");
server = new MeshCallServer({
  services: [fixture],
  router: { endpoint: process.env.MESHCALL_ROUTER_URL as string, instanceId: process.env.MESHCALL_CONFORMANCE_INSTANCE ?? "fixture-1" },
  pingIntervalMs: ping, pingTimeoutMs: Math.max(1000, ping * 2),
  accessLog: false,
});
await server.start();
process.stderr.write("fixture registered\n");

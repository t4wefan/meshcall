import assert from "node:assert/strict";
import test from "node:test";

import {
  DeadlineExceededError, defineService, defineType, ErrorCode, InternalError, InvalidArgumentError, MeshCallClient,
  MeshCallError, MeshCallServer, ProtocolError, serverStreamMethod, unaryMethod, type LogLevel, type RpcLogger,
} from "../src/index.js";

const SECRET = "not-an-int-s3cr3t";

interface Value { value: number }
interface Result { total: number }

const valueType = defineType<Value>("Value", {
  type: "object", properties: { value: { type: "integer" } }, required: ["value"],
});
const resultType = defineType<Result>("Result", {
  type: "object", properties: { total: { type: "integer" } }, required: ["total"],
});
const emptyType = defineType<Record<string, never>>("Empty", { type: "object", properties: {} });

const service = defineService({
  name: "test.v1.ErrorService",
  description: "Handlers that are right, wrong, or fed wrong input.",
  sourceModule: "errors.test", sourceQualname: "ErrorService",
  methods: {
    echo: unaryMethod({ description: "Echo value.", readOnly: true, request: valueType, response: resultType, handler: ({ value }) => ({ total: value }) }),
    bad_response: unaryMethod({
      description: "Break the response contract on purpose.", readOnly: true, request: emptyType, response: resultType,
      handler: () => ({ total: "nope" } as unknown as Result),
    }),
    bad_items: serverStreamMethod({
      description: "Yield one good item, then break the output contract.", readOnly: true, request: emptyType, outputItem: resultType,
      handler: async function* () { yield { total: 1 }; yield { total: "nope" } as unknown as Result; },
    }),
  },
});

class RecordingLogger implements RpcLogger {
  public readonly lines: Array<[LogLevel, string]> = [];
  public bind(): RpcLogger { return this; }
  public log(level: LogLevel, message: string, ...args: unknown[]): void { this.lines.push([level, message + " " + args.join(" ")]); }
  public debug(m: string, ...a: unknown[]): void { this.log("debug", m, ...a); }
  public info(m: string, ...a: unknown[]): void { this.log("info", m, ...a); }
  public warning(m: string, ...a: unknown[]): void { this.log("warning", m, ...a); }
  public error(m: string, ...a: unknown[]): void { this.log("error", m, ...a); }
}

async function setup(t: test.TestContext): Promise<{ client: MeshCallClient; logger: RecordingLogger }> {
  const logger = new RecordingLogger();
  const server = new MeshCallServer({ services: [service], logger, logLevel: "info" });
  await server.start();
  const client = new MeshCallClient("ws://127.0.0.1:" + server.boundPort);
  t.after(async () => { await client.close(); await server.close(); });
  return { client, logger };
}

test("invalid request is typed, redacted, and logged as a warning", { timeout: 5000 }, async (t) => {
  const { client, logger } = await setup(t);
  const error = await client.unary(service.name, "echo", { value: SECRET }).then(() => undefined, (e: unknown) => e);
  assert.ok(error instanceof InvalidArgumentError);
  assert.equal(error.message, "Invalid request");
  assert.equal(error.source, "server");
  assert.equal(error.reason, "request");
  assert.deepEqual(error.violations.map((v) => [v.path, v.code]), [["value", "type"]]);
  assert.ok(!JSON.stringify(error.details).includes(SECRET));

  const missing = await client.unary(service.name, "echo", {}).then(() => undefined, (e: unknown) => e);
  assert.ok(missing instanceof InvalidArgumentError);
  assert.deepEqual(missing.violations.map((v) => [v.path, v.code]), [["value", "missing"]]);

  assert.deepEqual(await client.unary(service.name, "echo", { value: 4 }), { total: 4 });
  const levels = logger.lines.filter(([, m]) => m.includes("RPC call")).map(([level]) => level);
  assert.deepEqual(levels, ["warning", "warning", "info"]);
});

test("invalid response and output items are the server's fault", { timeout: 5000 }, async (t) => {
  const { client, logger } = await setup(t);
  const error = await client.unary(service.name, "bad_response", {}).then(() => undefined, (e: unknown) => e);
  assert.ok(error instanceof InternalError);
  assert.equal(error.message, "Internal service error");
  assert.equal(error.reason, "invalid_response");
  assert.ok(!JSON.stringify(error.details).includes("nope"));
  assert.ok(logger.lines.some(([level, m]) => level === "error" && m.includes("invalid response") && m.includes("total")));

  const seen: number[] = [];
  const streamError = await (async () => {
    for await (const item of client.serverStream<Record<string, never>, Result>(service.name, "bad_items", {})) seen.push(item.total);
  })().then(() => undefined, (e: unknown) => e);
  assert.deepEqual(seen, [1]);
  assert.ok(streamError instanceof InternalError);
  assert.equal(streamError.reason, "invalid_output_item");
  assert.deepEqual(logger.lines.filter(([, m]) => m.includes("RPC call")).map(([level]) => level), ["error", "error"]);
});

test("fromCode dispatches to subclasses and keeps custom codes plain", () => {
  const late = MeshCallError.fromCode("deadline_exceeded", "late", { details: { source: "router" } });
  assert.ok(late instanceof DeadlineExceededError);
  assert.equal(late.code, ErrorCode.DEADLINE_EXCEEDED);
  assert.equal(late.source, "router");
  assert.ok(MeshCallError.fromCode("protocol_error", "bad") instanceof ProtocolError);
  const custom = MeshCallError.fromCode("app.v1.quota", "no", { retryable: true });
  assert.equal(custom.constructor, MeshCallError);
  assert.ok(custom.retryable);
});

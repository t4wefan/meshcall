import assert from "node:assert/strict";
import test from "node:test";

import {
  DeadlineExceededError, ErrorCode, InternalError, InvalidArgumentError, MeshCallClient, MeshCallError,
  MeshCallServer, method, ProtocolError, service, z, type LogLevel, type RpcLogger,
} from "../src/index.js";

const SECRET = "not-an-int-s3cr3t";

const Value = z.object({ value: z.number().int() });
const Result = z.object({ total: z.number().int() });
const Empty = z.object({});
type Value = z.infer<typeof Value>;
type Result = z.infer<typeof Result>;

const NAME = "test.v1.ErrorService";

@service(NAME, { description: "Handlers that are right, wrong, or fed wrong input." })
class ErrorService {
  @method({ description: "Echo value.", readOnly: true, request: Value, response: Result })
  echo({ value }: Value) { return { total: value }; }

  @method({ description: "Break the response contract on purpose.", readOnly: true, request: Empty, response: Result })
  bad_response() { return { total: "nope" } as unknown as Result; }

  @method.serverStream({
    description: "Yield one good item, then break the output contract.", readOnly: true, request: Empty, outputItem: Result,
  })
  async *bad_items() { yield { total: 1 }; yield { total: "nope" } as unknown as Result; }
}

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
  const server = new MeshCallServer({ services: [new ErrorService()], logger, logLevel: "info" });
  await server.start();
  const client = new MeshCallClient("ws://127.0.0.1:" + server.boundPort);
  t.after(async () => { await client.close(); await server.close(); });
  return { client, logger };
}

test("invalid request is typed, redacted, and logged as a warning", { timeout: 5000 }, async (t) => {
  const { client, logger } = await setup(t);
  const error = await client.unary(NAME, "echo", { value: SECRET }).then(() => undefined, (e: unknown) => e);
  assert.ok(error instanceof InvalidArgumentError);
  assert.equal(error.message, "Invalid request");
  assert.equal(error.source, "server");
  assert.equal(error.reason, "request");
  assert.deepEqual(error.violations.map((v) => [v.path, v.code]), [["value", "type"]]);
  assert.ok(!JSON.stringify(error.details).includes(SECRET));

  const missing = await client.unary(NAME, "echo", {}).then(() => undefined, (e: unknown) => e);
  assert.ok(missing instanceof InvalidArgumentError);
  assert.deepEqual(missing.violations.map((v) => [v.path, v.code]), [["value", "missing"]]);

  assert.deepEqual(await client.unary(NAME, "echo", { value: 4 }), { total: 4 });
  const levels = logger.lines.filter(([, m]) => m.includes("RPC call")).map(([level]) => level);
  assert.deepEqual(levels, ["warning", "warning", "info"]);
});

test("invalid response and output items are the server's fault", { timeout: 5000 }, async (t) => {
  const { client, logger } = await setup(t);
  const error = await client.unary(NAME, "bad_response", {}).then(() => undefined, (e: unknown) => e);
  assert.ok(error instanceof InternalError);
  assert.equal(error.message, "Internal service error");
  assert.equal(error.reason, "invalid_response");
  assert.ok(!JSON.stringify(error.details).includes("nope"));
  assert.ok(logger.lines.some(([level, m]) => level === "error" && m.includes("invalid response") && m.includes("total")));

  const seen: number[] = [];
  const streamError = await (async () => {
    for await (const item of client.serverStream<Record<string, never>, Result>(NAME, "bad_items", {})) seen.push(item.total);
  })().then(() => undefined, (e: unknown) => e);
  assert.deepEqual(seen, [1]);
  assert.ok(streamError instanceof InternalError);
  assert.equal(streamError.reason, "invalid_output_item");
  assert.deepEqual(logger.lines.filter(([, m]) => m.includes("RPC call")).map(([level]) => level), ["error", "error"]);
});

test("clients validate responses and items against their own contract", { timeout: 5000 }, async (t) => {
  const { client } = await setup(t);
  // The client expects a string where the service (correctly, by its own contract) sends a number.
  const skewed = { type: "object", properties: { total: { type: "string" } }, required: ["total"] };
  const error = await client.unary(NAME, "echo", { value: 4 }, { responseSchema: skewed }).then(() => undefined, (e: unknown) => e);
  assert.ok(error instanceof ProtocolError);
  assert.equal(error.source, "client");
  assert.equal(error.reason, "invalid_response");
  assert.deepEqual(error.details?.violations, [{ path: "total", code: "type", message: "must be string" }]);
  // Matching schema passes; mismatching item schema fails the stream.
  assert.deepEqual(await client.unary(NAME, "echo", { value: 4 }, { responseSchema: z.toJSONSchema(Result) }), { total: 4 });
  const items = client.serverStream<Record<string, never>, Result>(NAME, "bad_items", {}, { itemSchema: skewed });
  const streamError = await items.next().then(() => undefined, (e: unknown) => e);
  assert.ok(streamError instanceof ProtocolError);
  assert.equal(streamError.reason, "invalid_output_item");
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

import assert from "node:assert/strict";
import { pbkdf2Sync } from "node:crypto";
import { mkdtemp, writeFile, rm, access } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  MeshCallClient, MeshCallError, MeshCallServer, RouterAuthClient, WebSocketRouter,
  findRouterBinary, method, service, z,
} from "../src/index.js";

// These tests spawn the Go Router; skip with a clear reason when no binary is
// available instead of failing every router test.
const routerMissing = (() => { try { findRouterBinary(); return false; } catch { return "Go Router binary not built"; } })();

const password = "test-only-password";
const salt = Buffer.from("meshcall-test-01");
const hash = "pbkdf2-sha256$600000$" + salt.toString("hex") + "$" +
  pbkdf2Sync(password, salt, 600000, 32, "sha256").toString("hex");
const auth = { username: "issuer", password };
const NAME = "test.v1.RouterService";
const Value = z.object({ value: z.number().int() });
type Value = z.infer<typeof Value>;

@service(NAME, { description: "Router fixtures." })
class RouterService {
  @method({ description: "Echo the request.", readOnly: true, request: Value, response: Value })
  echo(request: Value) { return request; }

  @method.serverStream({ description: "Yield 0 up to value.", readOnly: true, request: Value, outputItem: Value })
  async *download(request: Value) { for (let value = 0; value < request.value; value++) yield { value }; }

  @method.clientStream({ description: "Add the items to value.", idempotent: true, request: Value, inputItem: Value, response: Value })
  async upload(request: Value, items: AsyncIterable<Value>) {
    let value = request.value;
    for await (const item of items) value += item.value;
    return { value };
  }
}
const isError = (code: string) => (error: unknown) => error instanceof MeshCallError && error.code === code;

test("TypeScript owns a Go process, retries a failed bind and handles Unix cleanup", { timeout: 10000, skip: routerMissing }, async (t) => {
  const router = new WebSocketRouter({ outboxSize: 64 * 1024 * 1024 });
  t.after(() => router.close());
  await Promise.all([router.start(), router.start()]);
  assert.notEqual(router.pid, process.pid);
  const pid = router.pid;
  assert.ok(pid);
  await router.start();
  assert.equal(router.pid, pid);
  const other = new WebSocketRouter({ port: router.boundPort });
  t.after(() => other.close());
  await assert.rejects(other.start(), /listen/u);
  assert.equal(other.isRunning, false);
  await router.stop();
  await other.start();
  assert.ok(other.isRunning);
  await other.close();
  if (process.platform !== "win32") { // the local transport there is a pipe; see local.test.ts
    const socket = join(tmpdir(), "meshcall-ts-router-" + process.pid + ".sock");
    const unix = new WebSocketRouter({ unixPath: socket });
    t.after(() => unix.stop());
    await unix.start();
    await access(socket);
    assert.throws(() => unix.boundPort, /TCP port/u);
    await unix.stop();
    await assert.rejects(access(socket));
    // Windows has no SIGTERM: kill() terminates without a graceful exit code.
    await router.start();
    const exited = router.waitClosed();
    process.kill(router.pid!, "SIGTERM");
    assert.equal(await exited, 0);
    assert.equal(router.isRunning, false);
  }
});

test("typed AuthService issues scopes, routes all call shapes and revokes an active stream", { timeout: 10000, skip: routerMissing }, async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "meshcall-ts-auth-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const file = join(directory, "auth.json");
  await writeFile(file, JSON.stringify({ users: [{
    username: auth.username, password_hash: hash, roles: ["client", "server"],
    register: [NAME], call: [NAME + "/*", "meshcall.router.v1.AuthService/*"],
  }] }));
  const router = new WebSocketRouter({ authFile: file });
  t.after(() => router.stop());
  await router.start();
  const endpoint = "ws://127.0.0.1:" + router.boundPort;
  const server = new MeshCallServer({ services: [new RouterService()], router: { endpoint, auth }, accessLog: false });
  t.after(() => server.close());
  await server.start();
  const issuer = new MeshCallClient(endpoint, undefined, { auth });
  t.after(() => issuer.close());
  const management = new RouterAuthClient(issuer);
  assert.equal((await management.whoami()).username, auth.username);
  const issued = await management.issueToken({
    scopes: [{ service: NAME, methods: ["echo", "download", "upload"] }],
  });
  const client = new MeshCallClient(endpoint, undefined, { auth: { token: issued.token } });
  t.after(() => client.close());
  assert.deepEqual(await client.unary(NAME, "echo", { value: 9 }), { value: 9 });
  const downloaded: number[] = [];
  for await (const item of client.serverStream<{ value: number }, { value: number }>(
    NAME, "download", { value: 64 },
  )) downloaded.push(item.value);
  assert.deepEqual(downloaded, Array.from({ length: 64 }, (_, i) => i));
  assert.deepEqual(await client.clientStream(NAME, "upload", { value: 7 },
    (async function* () { for (const value of downloaded) yield { value }; })()), { value: 2023 });
  await assert.rejects(client.unary(NAME, "forbidden", {}), isError("permission_denied"));
  await assert.rejects(client.unary("other.v1.Service", "echo", {}), isError("permission_denied"));
  await assert.rejects(new RouterAuthClient(client).issueToken({
    scopes: [{ service: NAME, methods: ["*"] }],
  }), isError("permission_denied"));
  const stream = client.serverStream(NAME, "download", { value: 100000 });
  await stream.next();
  assert.equal(await management.revokeToken(issued.token_id), true);
  await assert.rejects(async () => { for await (const _ of stream) { /* drain buffered items */ } }, isError("unavailable"));
  assert.equal(await management.revokeToken(issued.token_id), false);
  const revoked = new MeshCallClient(endpoint, undefined, { auth: { token: issued.token } });
  t.after(() => revoked.close());
  await assert.rejects(revoked.connect(), /401/u);
  await assert.rejects(management.issueToken({ scopes: [{ service: "other.v1.Service", methods: ["*"] }] }),
    isError("permission_denied"));
});

test("launcher rejects missing binaries and malformed auth before becoming ready", { timeout: 5000 }, async () => {
  const router = new WebSocketRouter({ binaryPath: "/not/a/meshcall-router" });
  await assert.rejects(router.start(), /not executable/u);
  assert.equal(router.pid, undefined);
  await router.stop();
  const missing = new WebSocketRouter({ authFile: "/not/an/auth.json" });
  await assert.rejects(missing.start(), /read auth file/u);
  assert.equal(missing.isRunning, false);
  await missing.stop();
  assert.throws(() => new WebSocketRouter({ maxFrameSize: 64 * 1024 * 1024 + 1 }), /maxFrameSize/u);
  assert.throws(() => new WebSocketRouter({ maxFrameSize: 2048, outboxSize: 1024 }), /outboxSize/u);
  assert.throws(() => new WebSocketRouter({ outboxSize: 32 * 1024 * 1024 }), /outboxSize/u);
});

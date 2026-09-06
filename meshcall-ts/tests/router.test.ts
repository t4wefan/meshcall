import assert from "node:assert/strict";
import { pbkdf2Sync } from "node:crypto";
import { mkdtemp, writeFile, rm, access } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  MeshCallClient, MeshCallError, MeshCallServer, RouterAuthClient, WebSocketRouter,
  defineService, defineType, unaryMethod, serverStreamMethod, clientStreamMethod,
} from "../src/index.js";

const password = "test-only-password";
const salt = Buffer.from("meshcall-test-01");
const hash = "pbkdf2-sha256$600000$" + salt.toString("hex") + "$" +
  pbkdf2Sync(password, salt, 600000, 32, "sha256").toString("hex");
const auth = { username: "issuer", password };
const valueType = defineType<{ value: number }>("Value", {
  type: "object", properties: { value: { type: "integer" } }, required: ["value"], additionalProperties: false,
});
const service = defineService({
  name: "test.v1.RouterService", sourceModule: "router.test", sourceQualname: "RouterService",
  methods: {
    echo: unaryMethod({ request: valueType, response: valueType, handler: (request) => request }),
    download: serverStreamMethod({
      request: valueType, outputItem: valueType,
      handler: async function* (request) { for (let value = 0; value < request.value; value++) yield { value }; },
    }),
    upload: clientStreamMethod({
      request: valueType, inputItem: valueType, response: valueType,
      handler: async (request, items) => {
        let value = request.value;
        for await (const item of items) value += item.value;
        return { value };
      },
    }),
  },
});
const isError = (code: string) => (error: unknown) => error instanceof MeshCallError && error.code === code;

test("TypeScript owns a Go process, retries a failed bind and handles Unix cleanup", { timeout: 10000 }, async (t) => {
  const router = new WebSocketRouter();
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
  const socket = join(tmpdir(), "meshcall-ts-router-" + process.pid + ".sock");
  const unix = new WebSocketRouter({ unixPath: socket });
  t.after(() => unix.stop());
  await unix.start();
  await access(socket);
  assert.throws(() => unix.boundPort, /TCP port/u);
  await unix.stop();
  await assert.rejects(access(socket));
  await router.start();
  const exited = router.waitClosed();
  process.kill(router.pid!, "SIGTERM");
  assert.equal(await exited, 0);
  assert.equal(router.isRunning, false);
});

test("typed AuthService issues scopes, routes all call shapes and revokes an active stream", { timeout: 10000 }, async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "meshcall-ts-auth-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const file = join(directory, "auth.json");
  await writeFile(file, JSON.stringify({ users: [{
    username: auth.username, password_hash: hash, roles: ["client", "server"],
    register: [service.name], call: [service.name + "/*", "meshcall.router.v1.AuthService/*"],
  }] }));
  const router = new WebSocketRouter({ authFile: file });
  t.after(() => router.stop());
  await router.start();
  const endpoint = "ws://127.0.0.1:" + router.boundPort;
  const server = new MeshCallServer({ services: [service], router: { endpoint, auth }, accessLog: false });
  t.after(() => server.close());
  await server.start();
  const issuer = new MeshCallClient(endpoint, undefined, { auth });
  t.after(() => issuer.close());
  const management = new RouterAuthClient(issuer);
  assert.equal((await management.whoami()).username, auth.username);
  const issued = await management.issueToken({
    scopes: [{ service: service.name, methods: ["echo", "download", "upload"] }],
  });
  const client = new MeshCallClient(endpoint, undefined, { auth: { token: issued.token } });
  t.after(() => client.close());
  assert.deepEqual(await client.unary(service.name, "echo", { value: 9 }), { value: 9 });
  const downloaded: number[] = [];
  for await (const item of client.serverStream<{ value: number }, { value: number }>(
    service.name, "download", { value: 64 },
  )) downloaded.push(item.value);
  assert.deepEqual(downloaded, Array.from({ length: 64 }, (_, i) => i));
  assert.deepEqual(await client.clientStream(service.name, "upload", { value: 7 },
    (async function* () { for (const value of downloaded) yield { value }; })()), { value: 2023 });
  await assert.rejects(client.unary(service.name, "forbidden", {}), isError("permission_denied"));
  await assert.rejects(client.unary("other.v1.Service", "echo", {}), isError("permission_denied"));
  await assert.rejects(new RouterAuthClient(client).issueToken({
    scopes: [{ service: service.name, methods: ["*"] }],
  }), isError("permission_denied"));
  const stream = client.serverStream(service.name, "download", { value: 100000 });
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
});

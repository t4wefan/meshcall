import assert from "node:assert/strict";
import { pbkdf2Sync } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  findRouterBinary,
  method,
  Router,
  RouterCredentials,
  serve,
  service,
  z,
  type CallOptions,
  type MeshCallRpc,
} from "../src/index.js";

const routerMissing = (() => {
  try {
    findRouterBinary();
    return false;
  } catch {
    return "Go Router binary not built";
  }
})();

const AddReq = z.object({ a: z.number().int(), b: z.number().int() });
const AddRes = z.object({ sum: z.number().int(), caller: z.string() });
const EchoReq = z.object({ text: z.string() });
const EchoRes = z.object({ text: z.string() });

@service("math", { description: "Math service for Router DX tests." })
class MathService {
  @method({
    description: "Add two integers and return caller metadata.",
    readOnly: true,
    request: AddReq,
    response: AddRes,
  })
  add(req: z.infer<typeof AddReq>, ctx: { metadata: Record<string, unknown> }) {
    return {
      sum: req.a + req.b,
      caller: typeof ctx.metadata["x-caller"] === "string" ? ctx.metadata["x-caller"] : "",
    };
  }
}

@service("echo", { description: "Echo service for Router DX tests." })
class EchoService {
  @method({
    description: "Echo text.",
    readOnly: true,
    request: EchoReq,
    response: EchoRes,
  })
  repeat(req: z.infer<typeof EchoReq>) {
    return { text: req.text };
  }
}

class MathTestClient {
  public constructor(private readonly rpc: MeshCallRpc) {}

  public withOptions(options: CallOptions): MathTestClient {
    return new MathTestClient(this.rpc.withOptions(options));
  }

  public add(a: number, b: number, options?: CallOptions) {
    return this.rpc.unary<z.infer<typeof AddReq>, z.infer<typeof AddRes>>("math.v0", "add", { a, b }, options);
  }
}

class EchoTestClient {
  public constructor(private readonly rpc: MeshCallRpc) {}

  public repeat(text: string, options?: CallOptions) {
    return this.rpc.unary<z.infer<typeof EchoReq>, z.infer<typeof EchoRes>>("echo.v0", "repeat", { text }, options);
  }
}

test(
  "serve auto-spawns a local Router, shares HTTP+WS on the same port, and Router.connect multiplexes clients and built-in services",
  { timeout: 10000, skip: routerMissing },
  async () => {
    const serverRouter = new Router({
      url: "ws://127.0.0.1:0",
      httpCorsOrigins: ["https://app.example.com"],
      httpDefaultTimeoutMs: 5000,
    });
    await using running = await serve(new MathService(), new EchoService(), {
      router: serverRouter,
      instanceId: "dx-node-1",
      accessLog: false,
    });

    assert.match(running.routerUrl, /^ws:\/\/127\.0\.0\.1:\d+$/u);
    assert.equal(serverRouter.url, running.routerUrl);

    // Verify HTTP and WS share the exact same port (/healthz and /v1/call/* on the same port)
    const httpBase = running.routerUrl.replace(/^ws:/u, "http:");
    const healthResp = await fetch(`${httpBase}/healthz`);
    assert.equal(healthResp.status, 200);
    const httpCallResp = await fetch(`${httpBase}/v1/call/echo.v0/repeat`, {
      method: "POST",
      headers: { "content-type": "application/json", origin: "https://app.example.com" },
      body: JSON.stringify({ text: "over-same-port-http" }),
    });
    assert.equal(httpCallResp.status, 200);
    assert.equal(httpCallResp.headers.get("access-control-allow-origin"), "https://app.example.com");
    assert.deepEqual(await httpCallResp.json(), { text: "over-same-port-http" });

    // Connect a single Router client and share it across multiple service clients + built-in Registry calls
    await using clientRouter = await Router.connect(running.routerUrl);
    const services = await clientRouter.listServices();
    const names = services.map((s) => s.name).sort();
    assert.ok(names.includes("math.v0"));
    assert.ok(names.includes("echo.v0"));

    const mathInfo = await clientRouter.describeService("math");
    assert.equal(mathInfo.name, "math.v0");
    assert.equal(mathInfo.instances[0]?.id, "dx-node-1");

    const math = new MathTestClient(clientRouter);
    const echo = new EchoTestClient(clientRouter);

    const res1 = await math.add(10, 32);
    assert.deepEqual(res1, { sum: 42, caller: "" });

    const res2 = await math.withOptions({ metadata: { "x-caller": "alice" }, timeoutMs: 3000 }).add(7, 8);
    assert.deepEqual(res2, { sum: 15, caller: "alice" });

    const echoed = await echo.repeat("shared-connection");
    assert.deepEqual(echoed, { text: "shared-connection" });
  },
);

test(
  "Router built-in AuthService methods (whoami, issueToken, revokeToken) work with RouterCredentials",
  { timeout: 10000, skip: routerMissing },
  async (t) => {
    const dir = await mkdtemp(join(tmpdir(), "meshcall-ts-router-dx-"));
    t.after(() => rm(dir, { recursive: true, force: true }));
    const password = "dx-secret-password";
    const salt = Buffer.from("meshcall-dx-salt");
    const hash =
      "pbkdf2-sha256$600000$" +
      salt.toString("hex") +
      "$" +
      pbkdf2Sync(password, salt, 600000, 32, "sha256").toString("hex");
    const authFile = join(dir, "auth.json");
    await writeFile(
      authFile,
      JSON.stringify({
        users: [
          {
            username: "admin",
            password_hash: hash,
            roles: ["client", "server"],
            register: ["*"],
            call: ["*"],
          },
        ],
      }),
    );

    const routerCfg = new Router({
      url: "ws://127.0.0.1:0",
      authFile,
      auth: RouterCredentials.basic("admin", password),
    });
    await using running = await serve(new EchoService(), { router: routerCfg, accessLog: false });

    await using adminRouter = await Router.connect({
      url: running.routerUrl,
      auth: RouterCredentials.basic("admin", password),
    });
    const identity = await adminRouter.whoami();
    assert.equal(identity.username, "admin");

    const issued = await adminRouter.issueToken([{ service: "echo", methods: ["repeat"] }], 300);
    assert.ok(issued.token);
    assert.ok(issued.token_id);

    await using scopedRouter = await Router.connect({
      url: running.routerUrl,
      auth: RouterCredentials.bearer(issued.token),
    });
    const echo = new EchoTestClient(scopedRouter);
    assert.deepEqual(await echo.repeat("hello"), { text: "hello" });

    const revoked = await adminRouter.revokeToken(issued.token_id);
    assert.equal(revoked, true);
  },
);

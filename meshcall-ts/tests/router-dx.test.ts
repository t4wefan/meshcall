import assert from "node:assert/strict";
import { pbkdf2Sync } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  findRouterBinary,
  LocalRouter,
  Mesh,
  method,
  Router,
  RouterCredentials,
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

test("strict layering between Router/LocalRouter and Mesh", () => {
  assert.throws(
    () => new Mesh("ws://127.0.0.1:8765" as unknown as Router),
    (err: unknown) => err instanceof TypeError && /Router or LocalRouter instance/u.test(err.message),
  );
  const httpMesh = new Mesh(new Router("http://127.0.0.1:8765"));
  assert.throws(
    () => httpMesh.mount(new EchoService()),
    (err: unknown) => err instanceof TypeError,
  );
});

test(
  "LocalRouter starts a managed subprocess, shares HTTP+WS on the same port, and Mesh multiplexes clients and built-in services",
  { timeout: 10000, skip: routerMissing },
  async () => {
    await using router = await LocalRouter.start({
      port: 0,
      httpCorsOrigins: ["https://app.example.com"],
      httpDefaultTimeoutMs: 5000,
    });

    assert.match(router.url, /^ws:\/\/127\.0\.0\.1:\d+$/u);
    assert.equal(router.isRunning, true);
    assert.ok(router.boundPort && router.boundPort > 0);
    assert.ok(router.pid && router.pid > 0);

    const serverMesh = new Mesh(router);
    serverMesh.mount(new MathService(), new EchoService(), {
      instanceId: "dx-node-1",
      accessLog: false,
    });
    await using running = await serverMesh.startServing();
    assert.equal(running.routerUrl, router.url);

    // Verify HTTP and WS share the exact same port (/healthz and /v1/call/* on the same port)
    const httpBase = router.httpUrl!;
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

    // Connect a single Mesh client via Router(url) and share it across multiple service clients + built-in Registry calls
    await using clientMesh = await Mesh.connect(new Router(router.url));
    const services = await clientMesh.registry.list();
    const names = services.map((s) => s.name).sort();
    assert.ok(names.includes("math.v0"));
    assert.ok(names.includes("echo.v0"));

    const mathInfo = await clientMesh.registry.describe("math");
    assert.equal(mathInfo.name, "math.v0");
    assert.equal(mathInfo.instances[0]?.id, "dx-node-1");

    const math = new MathTestClient(clientMesh);
    const echo = new EchoTestClient(clientMesh);

    const res1 = await math.add(10, 32);
    assert.deepEqual(res1, { sum: 42, caller: "" });

    const res2 = await math.withOptions({ metadata: { "x-caller": "alice" }, timeoutMs: 3000 }).add(7, 8);
    assert.deepEqual(res2, { sum: 15, caller: "alice" });

    const echoed = await echo.repeat("shared-connection");
    assert.deepEqual(echoed, { text: "shared-connection" });
  },
);

test(
  "Mesh built-in AuthClient methods (whoami, issueToken, revokeToken) work with RouterCredentials and token",
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

    await using router = await LocalRouter.start({
      port: 0,
      authFile,
    });
    const serverMesh = new Mesh(router, {
      credentials: RouterCredentials.basic("admin", password),
    });
    serverMesh.mount(new EchoService(), { accessLog: false });
    await using _running = await serverMesh.startServing();

    await using adminMesh = await Mesh.connect(new Router(router.url), {
      credentials: RouterCredentials.basic("admin", password),
    });
    const identity = await adminMesh.auth.whoami();
    assert.equal(identity.username, "admin");

    const issued = await adminMesh.auth.issueToken([{ service: "echo", methods: ["repeat"] }], 300);
    assert.ok(issued.token);
    assert.ok(issued.token_id);

    await using scopedMesh = await Mesh.connect(new Router(router.url), {
      token: issued.token,
    });
    const echo = new EchoTestClient(scopedMesh);
    assert.deepEqual(await echo.repeat("hello"), { text: "hello" });

    const revoked = await adminMesh.auth.revokeToken(issued.token_id);
    assert.equal(revoked, true);
  },
);

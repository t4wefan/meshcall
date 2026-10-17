import assert from "node:assert/strict";
import test from "node:test";
import {
  MeshCallClient,
  MeshCallError,
  contractEntry,
  contractWarnings,
  dependency,
  meta,
  mutates,
  query,
  serve,
  service,
  stream,
  z,
} from "../src/index.js";

const WelcomeProfileSchema = z.object({
  id: z.string(),
  name: z.string(),
  locale: z.string().default("en"),
});
type WelcomeProfile = z.infer<typeof WelcomeProfileSchema>;

const WelcomeCardSchema = z.object({
  title: z.string(),
  greeting: z.string(),
  operator: z.string(),
});

const LogLineSchema = z.object({
  seq: z.number().int(),
  text: z.string(),
  traceId: z.string(),
});

const UploadSummarySchema = z.object({
  path: z.string(),
  totalBytes: z.number().int(),
  chunkCount: z.number().int(),
  operator: z.string(),
});

const TerminalEventSchema = z.object({
  shell: z.string(),
  output: z.string(),
  operator: z.string(),
});

test("supports @service({ name, description }), @query, @stream, @mutates, dependency(), and meta()", async () => {
  const lifecycleEvents: string[] = [];

  const TraceId = meta("x-trace-id", { default: "default-trace" });
  const CurrentUser = dependency(
    async function* (traceId: string) {
      lifecycleEvents.push(`open:${traceId}`);
      try {
        yield { id: "u-42", traceId };
      } finally {
        lifecycleEvents.push(`close:${traceId}`);
      }
    },
    { inject: [TraceId] },
  );

  const RequestAudit = dependency(
    (user: { id: string; traceId: string }) => {
      lifecycleEvents.push(`audit:${user.id}:${user.traceId}`);
      return { tag: `${user.id}@${user.traceId}` };
    },
    { inject: [CurrentUser] },
  );

  @service({
    name: "demo.Workspace",
    description: "Workspace service using new DX decorators and DI",
  })
  class Workspace {
    @query({
      description: "Render a welcome card from a single object parameter (1:1 mirroring)",
      input: { profile: WelcomeProfileSchema },
      output: WelcomeCardSchema,
      inject: [CurrentUser, RequestAudit],
    })
    async renderWelcome(
      profile: WelcomeProfile,
      user: { id: string; traceId: string },
      audit: { tag: string },
    ) {
      assert.equal(audit.tag, `${user.id}@${user.traceId}`);
      return {
        title: `Welcome ${profile.name} (${profile.id})`,
        greeting: profile.locale === "zh" ? "你好" : "Hello",
        operator: audit.tag,
      };
    }

    @stream({
      description: "Tail service logs (Server Stream)",
      input: {
        serviceName: z.string(),
        lines: z.number().int().default(2),
      },
      output: LogLineSchema,
      inject: [TraceId],
    })
    async *tailLogs(serviceName: string, lines: number, traceId: string) {
      for (let i = 0; i < lines; i++) {
        yield {
          seq: i + 1,
          text: `[${serviceName}] line ${i + 1}`,
          traceId,
        };
      }
    }

    @mutates({
      description: "Upload file chunks (Client Stream)",
      input: { path: z.string() },
      streamInput: z.string(),
      output: UploadSummarySchema,
      idempotent: true,
      inject: [CurrentUser],
    })
    async uploadFile(
      path: string,
      chunks: AsyncIterable<string>,
      user: { id: string; traceId: string },
    ) {
      let totalBytes = 0;
      let chunkCount = 0;
      for await (const chunk of chunks) {
        totalBytes += Buffer.byteLength(chunk, "utf8");
        chunkCount += 1;
      }
      return {
        path,
        totalBytes,
        chunkCount,
        operator: user.id,
      };
    }

    @mutates({
      description: "Interactive terminal session (Duplex Stream)",
      input: { shell: z.string().default("bash") },
      streamInput: z.string(),
      output: TerminalEventSchema,
      sideEffect: "Opens a remote shell session",
      inject: [CurrentUser],
    })
    async openTerminal(
      shell: string,
      channel: {
        [Symbol.asyncIterator](): AsyncIterator<string>;
        send(item: z.infer<typeof TerminalEventSchema>): Promise<void>;
      },
      user: { id: string; traceId: string },
    ) {
      for await (const cmd of channel) {
        await channel.send({
          shell,
          output: `exec:${cmd.trim()}`,
          operator: user.id,
        });
      }
    }
  }

  // Verify strict mode warnings are empty
  const warnings = contractWarnings(Workspace);
  assert.deepEqual(warnings, []);

  // Verify extracted contract excludes DI/meta parameters
  const contract = contractEntry(Workspace) as {
    name: string;
    methods: Array<{ name: string; stream: string; parameters: string[] }>;
  };
  assert.equal(contract.name, "demo.Workspace.v0");
  const renderMethod = contract.methods.find((m) => m.name === "renderWelcome")!;
  assert.equal(renderMethod.stream, "unary");
  assert.deepEqual(renderMethod.parameters, ["profile"]);

  const uploadMethod = contract.methods.find((m) => m.name === "uploadFile")!;
  assert.equal(uploadMethod.stream, "client_stream");
  assert.deepEqual(uploadMethod.parameters, ["path"]);

  const terminalMethod = contract.methods.find((m) => m.name === "openTerminal")!;
  assert.equal(terminalMethod.stream, "duplex");
  assert.deepEqual(terminalMethod.parameters, ["shell"]);

  const srv = await serve(new Workspace());
  const client = new MeshCallClient(srv.url);
  try {
    // 1. Unary call with custom metadata header + per-call DI cache & generator teardown
    const card = await client
      .withOptions({ metadata: { "x-trace-id": "trace-99" } })
      .unary("demo.Workspace.v0", "renderWelcome", {
        profile: { id: "p1", name: "Alice", locale: "zh" },
      });
    assert.deepEqual(card, {
      title: "Welcome Alice (p1)",
      greeting: "你好",
      operator: "u-42@trace-99",
    });
    assert.deepEqual(lifecycleEvents, [
      "open:trace-99",
      "audit:u-42:trace-99",
      "close:trace-99",
    ]);

    // 2. Server stream call
    const logs: Array<z.infer<typeof LogLineSchema>> = [];
    for await (const line of client.serverStream("demo.Workspace.v0", "tailLogs", {
      serviceName: "api",
      lines: 2,
    })) {
      logs.push(line as z.infer<typeof LogLineSchema>);
    }
    assert.deepEqual(logs, [
      { seq: 1, text: "[api] line 1", traceId: "default-trace" },
      { seq: 2, text: "[api] line 2", traceId: "default-trace" },
    ]);

    // 3. Client stream call
    async function* uploadChunks() {
      yield "hello ";
      yield "world";
    }
    const summary = await client.clientStream(
      "demo.Workspace.v0",
      "uploadFile",
      { path: "/tmp/hello.bin" },
      uploadChunks(),
    );
    assert.deepEqual(summary, {
      path: "/tmp/hello.bin",
      totalBytes: 11,
      chunkCount: 2,
      operator: "u-42",
    });

    // 4. Duplex call
    const dup = client.duplex("demo.Workspace.v0", "openTerminal", {
      shell: "zsh",
    });
    await dup.send("whoami");
    await dup.send("pwd");
    await dup.closeSend();
    const events: Array<z.infer<typeof TerminalEventSchema>> = [];
    for await (const ev of dup) {
      events.push(ev as z.infer<typeof TerminalEventSchema>);
    }
    assert.deepEqual(events, [
      { shell: "zsh", output: "exec:whoami", operator: "u-42" },
      { shell: "zsh", output: "exec:pwd", operator: "u-42" },
    ]);
  } finally {
    await client.close();
    await srv.close();
  }
});

test("fails fast on missing required meta() before acquiring dependency resources", async () => {
  let dbOpened = false;
  const DbConn = dependency(async function* () {
    dbOpened = true;
    yield { connected: true };
  });
  const RequiredTenant = meta("x-tenant-id", { required: true });

  @service({ name: "demo.TenantSvc", description: "Tenant service" })
  class TenantSvc {
    @query({
      description: "Get tenant info",
      output: z.string(),
      inject: [DbConn, RequiredTenant],
    })
    async info(_db: { connected: boolean }, tenant: string) {
      return tenant;
    }
  }

  const srv = await serve(new TenantSvc());
  const client = new MeshCallClient(srv.url);
  try {
    await assert.rejects(
      () => client.unary("demo.TenantSvc.v0", "info", {}),
      (err: unknown) => err instanceof MeshCallError && err.code === "invalid_argument",
    );
    assert.equal(dbOpened, false);
  } finally {
    await client.close();
    await srv.close();
  }
});

test("detects circular dependencies at registration time", () => {
  const depA: ReturnType<typeof dependency<string>> = dependency(
    (b: string) => b,
    { inject: [] },
  );
  const depB: ReturnType<typeof dependency<string>> = dependency(
    (a: string) => a,
    { inject: [depA] },
  );
  (depA.inject as unknown[]).push(depB);

  assert.throws(() => {
    @service({ name: "demo.CycleSvc", description: "Cycle service" })
    class CycleSvc {
      @query({
        description: "Ping",
        output: z.string(),
        inject: [depA],
      })
      async ping(a: string) {
        return a;
      }
    }
    return CycleSvc;
  }, /Circular dependency/i);
});

# meshcall (TypeScript SDK)

**English** | [简体中文](https://github.com/t4wefan/meshcall/blob/main/meshcall-ts/README.zh-CN.md)

**Write cross-language streaming RPC the way you write ordinary TypeScript, Python and Go functions.**

MeshCall is an asynchronous, typed streaming RPC framework for TypeScript, Python and Go over WebSocket. A service is a `@service` class whose methods are declared with `@query` (pure read), `@mutates` (side effects) or `@stream` (server stream), with [zod](https://zod.dev) schemas for what goes in and out (`z` is re-exported, so there is one copy). Services register with the standalone Go Router; clients call them through it, and generated clients in every language take the same parameter list. Client streams and restricted duplex calls are covered below.

## Install

```bash
yarn add meshcall
# or: npm install meshcall
```

Installing `meshcall` automatically pulls the matching `meshcall-router-<os>-<cpu>` package containing the prebuilt Go Router (`meshcall-router`) and `meshcall` CLI binaries—no Go compiler or Python runtime is needed.

If coding agents work on your project, install the first-party agent skill once:

```bash
npx meshcall init                  # default: .agents/skills/meshcall/SKILL.md + AGENTS.md
npx meshcall init --agent claude   # or target: claude | cursor | copilot | windsurf | all
```

## Quick Start

A service is a class; its decorated methods are the service's methods, and zod schemas describe what goes in and out:

```typescript
import { LocalRouter, Mesh, mutates, query, service, stream, z } from "meshcall";

const Message = z.object({ text: z.string() }).meta({ id: "Message" });

@service("chat.v1.Chat", { description: "Chat sessions." })
class Chat {
  @query({
    description: "Return a greeting for name.",
    input: { name: z.string() },
    output: Message,
  })
  async greet(name: string) {
    return { text: `hello, ${name}` };
  }

  @stream({
    description: "Yield the most recent messages of a session.",
    input: { session_id: z.string(), limit: z.int().min(1).max(100).default(20) },
    output: Message,
  })
  async *history(session_id: string, limit: number) {
    for (let index = 0; index < limit; index += 1) {
      yield { text: `${session_id} #${index}` };
    }
  }

  @mutates({
    description: "Append a message and return the reply.",
    input: { session_id: z.string(), text: z.string() },
    output: Message,
  })
  async send(session_id: string, text: string) {
    return { text: `${session_id} > ${text}` };
  }
}

const mesh = new Mesh(new LocalRouter({ port: 8765 }));
mesh.mount(new Chat());
await using server = await mesh.startServing();
console.log(`serving on ${server.url}`);
```

`mesh.startServing()` starts in the background and resolves with a `RunningServer` (`.url`, `.close()`); `await mesh.serve()` is the same but blocks until `SIGINT` / `SIGTERM`, then drains. Passing `new LocalRouter({ port: 8765 })` automatically starts and stops a local `meshcall-router` subprocess with the `Mesh`; pass `new Router()` (reads `MESHCALL_ROUTER_URL`, default `ws://127.0.0.1:8765`) when connecting to an already-running Router.

Call the service through a generated client (generation is shown below):

```typescript
import { Mesh, Router } from "meshcall";
import { ChatClient } from "meshcall-chat-v1-chat-client"; // the generated package

await using mesh = await Mesh.connect(new Router()); // MESHCALL_ROUTER_URL or ws://127.0.0.1:8765
const chat = new ChatClient(mesh);                   // shares the Mesh connection

console.log((await chat.greet("Alice")).text);

for await (const message of chat.history("s1", 5)) {
  console.log(message.text);
}

// Per-call options never enter the business method signature.
await chat
  .withOptions({ timeoutMs: 5_000, metadata: { "x-caller-id": "cli" } })
  .send("s1", "hi");
```

What happens here:

- `@query` declares a pure read (`readOnly: true, idempotent: true`); `@mutates` declares a state-changing method (`idempotent: true` when repeating it is safe; otherwise it is `destructive` by default); `@stream` declares a server stream, an async generator that yields `output` items. A client stream is `@mutates` plus `streamInput`; duplex is the restricted last resort (`@mutates` with `streamInput`, `streamOutput` and `reason`, receiving a `DuplexChannel<In, Out>`).
- `input` is an inline zod map (`{ session_id: z.string(), text: z.string() }`), which binds positionally: the handler takes `(session_id, text)`, and the generated client takes the same parameters in the same order. A single schema binds one parameter, `request: schema` binds a request object, and omitting `input` means no business parameters.
- `output` (or `response` for unary calls) and `outputItem` validate what the handler returns; defaults are applied to the request. `.meta({ id: "Message" })` names a model in the portable contract (unnamed ones are named after the method). `description` is required by the types (`""` declares none on purpose); `@query` and `@stream` need an output schema, checked when the class is defined.
- The schemas become the portable contract that `meshcall generate` turns into typed clients for Python, TypeScript and Go.
- `mesh.mount(...)` and `mesh.serve()` / `mesh.startServing()` register the instance with the Router; the same `Mesh` can also call other services (`new OtherClient(mesh)`).
- `chat.withOptions({ timeoutMs, idleTimeoutMs, metadata, idempotencyKey, instance })` returns a view sharing the connection with defaults for every call; `idempotencyKey` lets the Router dedupe a retried mutation, and targeted services require `instance` on every call.

Decorators are TC39 standard decorators: TypeScript 5+ compiles them with no extra flags (do not set `experimentalDecorators`). Plain JavaScript needs a compiler that supports them.

## Dependency Injection and Metadata

Handlers can receive server-only values without adding anything to the contract. `meta(key)` reads a call metadata header; `dependency(provider, { inject })` computes a value from other dependencies. Both are listed in the decorator's `inject` array and arrive after the business parameters, before `RpcContext`:

```typescript
import { dependency, meta, query } from "meshcall";

const authorization = meta("authorization");
const currentUser = dependency(
  (authorization: string | undefined) => (authorization ?? "").replace(/^Bearer\s+/u, ""),
  { inject: [authorization] },
);

@service("chat.v1.Chat", { description: "Chat sessions." })
class Chat {
  @query({
    description: "Say who the caller is.",
    input: {},
    output: Message,
    inject: [currentUser],
  })
  async whoami(user: string) {
    return { text: `hello, ${user}` };
  }
}
```

Clients send the header as call metadata: `chat.withOptions({ metadata: { authorization: "Bearer …" } })`. Neither `user` nor `authorization` appears in the contract or in generated clients. A dependency resolves at most once per call; a provider that is a generator (`function*` / `async function*`) has its `finally` run when the call ends, so it can open a transaction or a connection safely. `meta(key, { required: true })`, `meta(key, { default: … })` and `meta(key, { schema })` enforce or parse the header, failing with `invalid_argument` before the handler runs.

## `Router`, `LocalRouter` and `Mesh`

`Router` is a pure endpoint descriptor for a remote Router; `LocalRouter` manages a local `meshcall-router` subprocess; `Mesh` is the peer runtime that mounts services, shares a single client connection across generated clients, and exposes the Router's built-in services via `mesh.registry` and `mesh.auth`:

```typescript
import { Mesh, Router, RouterCredentials } from "meshcall";

await using mesh = await Mesh.connect(new Router("ws://127.0.0.1:8765"), {
  credentials: RouterCredentials.basic("admin", process.env.MESHCALL_PASSWORD ?? ""),
});
const services = await mesh.registry.list();
const me = await mesh.auth.whoami();
const issued = await mesh.auth.issueToken([{ service: "chat.v1.Chat", methods: ["send"] }], 300);
await mesh.auth.revokeToken(issued.token_id);
```

`mesh.withOptions({ timeoutMs, idleTimeoutMs, metadata, idempotencyKey, instance })` returns a view that shares the connection and adds those options to every call. Several endpoints fail over without Router-side coordination: `new Router(["ws://router-a:8765", "ws://router-b:8765"])` registers a server with each and makes a client try them in order, starting from the last one that worked.

`LocalRouter` starts the bundled Router from Node: besides `host` / `port` it takes `unixPath` (Linux, macOS, FreeBSD) or `pipe` (Windows) for the local transport, `maxFrameSize` (default and maximum 64 MiB), `outboxSize` (bytes queued for a peer that is not reading; default the Router's 256 MiB, at least `maxFrameSize`), `maxConnections`, `binaryPath` and `authFile`.

`MeshCallClient` (`new MeshCallClient(url, peerId?, options?)`) and `MeshCallServer` (`start()` / `close()`) are the lower-level connection and server primitives that `Mesh` builds on.

## HTTP Transport & Browser (`meshcall/web`)

When `Router` is given an `http://` or `https://` URL, `Mesh` automatically selects the `fetch`-based HTTP client transport underneath. `meshcall/web` exports a browser-safe `Mesh` and `Router` with no Node imports that runs in Node, browsers, Deno, Bun and edge runtimes, and exposes `mesh.registry` and `mesh.auth`:

```ts
import { Mesh, Router } from "meshcall/web";
import { ChatClient } from "my-chat-client";

const mesh = new Mesh(new Router("https://router.example"), { token: "mk_…" });
const chat = new ChatClient(mesh);
console.log(await chat.withOptions({ timeoutMs: 5000 }).greet("Alice"));
console.log(await mesh.registry.list());
```

Duplex methods are WebSocket-only (`method_not_found`, `reason: "duplex_over_http"`), and `mesh.mount(...)` throws `TypeError` on an HTTP-backed `Mesh`; unary retries, streams and the built-ins work as over WebSocket.

## Explore, Call, and Generate Clients with the CLI

```bash
# Inspect registered services, instances, health, and method signatures
npx meshcall router inspect --router ws://127.0.0.1:8765

# Call a method from the terminal
npx meshcall call chat.v1.Chat.greet '{"name": "Alice"}'

# Generate a single-file client (default) or a package (--package) directly from source files
npx meshcall generate src/service.ts --language typescript --output src/generated_client.ts
npx meshcall generate src/service.ts --language python --output chat_client.py
npx meshcall generate src/service.ts --language go --output internal/chat

# Export the portable contract IR (*.meshcall.json) via an ephemeral inspect-only Router
npx meshcall export src/service.ts --output service.meshcall.json
```

`--language` defaults to `python`. `meshcall export` and `meshcall generate` accept one or more `.py`, `.ts` / `.js`, Go package (`./pkg`), or `.json` sources (`file:Service` selects a specific service; omitting `:Service` discovers all services in the source) and spin up an in-process inspect-only Router to collect their contracts (`writeContract([Chat], "service.meshcall.json")` is also available programmatically). Generated clients are typed against `MeshCallRpc`, and their methods mirror the handler's parameter list one to one.

## Subpath Exports

- `meshcall/web` exports browser-safe `Mesh` and `Router`, which call the Router's HTTP v1 API over `fetch` (browsers, Deno, Bun, edge runtimes). Generated clients accept `new ChatClient(mesh)`. Duplex calls are WebSocket-only; another origin needs `--http-cors-origin` on the Router.
- `meshcall/otel` exports `otelClientInterceptor()` and `otelServerInterceptor()` for the `interceptors` option of `Mesh` and `mesh.serve()`. They need the optional peer dependency `@opentelemetry/api` and carry W3C trace context as call metadata, matching the Python `meshcall.otel` spans and metrics, so a trace continues across languages and through the Router.

## Documentation

- [Monorepo overview & quickstart](https://github.com/t4wefan/meshcall#readme) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/README.zh-CN.md))
- [User Guide & Cookbook](https://github.com/t4wefan/meshcall/blob/main/docs/guide.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/guide.md))
- [Router authentication & management](https://github.com/t4wefan/meshcall/blob/main/docs/router.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/router.md))
- [Runtime architecture](https://github.com/t4wefan/meshcall/blob/main/docs/architecture.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/architecture.md))
- [Protocol specification](https://github.com/t4wefan/meshcall/blob/main/docs/protocol.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/protocol.md))

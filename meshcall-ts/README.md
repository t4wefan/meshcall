# meshcall (TypeScript SDK)

**English** | [简体中文](https://github.com/t4wefan/meshcall/blob/main/meshcall-ts/README.zh-CN.md)

**Write cross-language streaming RPC the way you write ordinary TypeScript, Python and Go functions.**

MeshCall is an asynchronous, typed streaming RPC framework for TypeScript, Python and Go over WebSocket. A service is a `@service` class whose methods are declared with `@query` (pure read), `@mutates` (side effects) or `@stream` (server stream), with [zod](https://zod.dev) schemas for what goes in and out (`z` is re-exported, so there is one copy). Services register with the standalone Go Router; clients call them through it, and generated clients in every language take the same parameter list. Client streams, restricted duplex calls and experimental subscriptions are covered below.

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
import { mutates, query, Router, serve, service, stream, z } from "meshcall";

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

const server = await serve(new Chat(), { router: new Router("ws://127.0.0.1:8765") });
console.log(`serving on ${server.url}`);
```

`serve` starts in the background and resolves with a `RunningServer` (`.url`, `.close()`); `run(...)` is the same but blocks until `SIGINT` / `SIGTERM`, then drains. Both default to Router mode: `await serve(new Chat())` uses `MESHCALL_ROUTER_URL` (default `ws://127.0.0.1:8765`) and, when that local address has no Router yet, spawns the bundled one.

Call the service through a generated client (generation is shown below):

```typescript
import { Router } from "meshcall";
import { ChatClient } from "meshcall-chat-v1-chat-client"; // the generated package

const router = await Router.connect();          // MESHCALL_ROUTER_URL or ws://127.0.0.1:8765
const chat = await ChatClient.connect(router);  // opens the shared connection
try {
  console.log((await chat.greet("Alice")).text);

  for await (const message of chat.history("s1", 5)) {
    console.log(message.text);
  }

  // Per-call options never enter the business method signature.
  await chat
    .withOptions({ timeoutMs: 5_000, metadata: { "x-caller-id": "cli" } })
    .send("s1", "hi");
} finally {
  await router.close();
}
```

What happens here:

- `@query` declares a pure read (`readOnly: true, idempotent: true`); `@mutates` declares a state-changing method (`idempotent: true` when repeating it is safe; otherwise it is `destructive` by default); `@stream` declares a server stream, an async generator that yields `output` items. A client stream is `@mutates` plus `streamInput`; duplex is the restricted last resort (`streamInput` + `streamOutput` + `reason`). The long forms `@method`, `@method.serverStream`, `@method.clientStream` and `@method.duplex` still exist.
- `input` is an inline zod map (`{ session_id: z.string(), text: z.string() }`), which binds positionally: the handler takes `(session_id, text)`, and the generated client takes the same parameters in the same order. A single schema binds one parameter, `request: schema` binds a request object, and omitting `input` means no business parameters.
- `output` (or `response` for unary calls) and `outputItem` validate what the handler returns; defaults are applied to the request. `.meta({ id: "Message" })` names a model in the portable contract (unnamed ones are named after the method). `description` is required by the types (`""` declares none on purpose); `@query` and `@stream` need an output schema, checked when the class is defined.
- The schemas become the portable contract that `meshcall generate` turns into typed clients for Python, TypeScript and Go.
- `serve` registers the instance with the Router; the same process can also call other services, because a `Router` is a client too.
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

## The Router

`Router` is the unified handle for both sides:

- **Configure a server**: pass `new Router({ url, auth, metricsListen, … })` to `serve` / `run`. With no argument it reads `MESHCALL_ROUTER_URL` (default `ws://127.0.0.1:8765`); `serve` / `run` spawn the bundled Router for a local endpoint when nothing listens there (`new Router({ spawnIfLocal: false })` disables that). Pass `router: null` to run without a Router at all (Direct mode, listening on `listen`, default `ws://127.0.0.1:0`).
- **Share one connection**: `const router = await Router.connect(endpointOrOptions?)`; give it to any number of generated clients, `new ChatClient(router)`, `new BillingClient(router)`. `await ChatClient.connect(router)` is the same, opening the connection first.
- **Call the built-ins**: `listServices()`, `describeService(name)`, `whoami()`, `issueToken(scopes, ttlSeconds)`, `revokeToken(tokenId)`.

```typescript
import { Router, RouterCredentials } from "meshcall";

const router = await Router.connect({
  url: "ws://127.0.0.1:8765",
  auth: RouterCredentials.basic("admin", process.env.MESHCALL_PASSWORD ?? ""),
});
const services = await router.listServices();
const me = await router.whoami();
const issued = await router.issueToken([{ service: "chat.v1.Chat", methods: ["send"] }], 300);
await router.revokeToken(issued.token_id);
await router.close();
```

`withOptions({ timeoutMs, idleTimeoutMs, metadata, idempotencyKey, instance })` returns a view that shares the connection and adds those options to every call. `Router.connect()` does not start a Router; only `serve` / `run` spawn the bundled binary. Several endpoints fail over without Router-side coordination: `new Router(["ws://router-a:8765", "ws://router-b:8765"])` registers a server with each and makes a client try them in order, starting from the last one that worked.

`WebSocketRouter` starts the bundled Router from Node: besides `host` / `port` it takes `unixPath` (Linux, macOS, FreeBSD) or `pipe` (Windows) for the local transport, `maxFrameSize` (default and maximum 64 MiB), `outboxSize` (bytes queued for a peer that is not reading; default the Router's 256 MiB, at least `maxFrameSize`), `maxConnections`, `binaryPath` and `authFile`.

`MeshCallClient` is the lower-level connection (`new MeshCallClient(url, peerId?, options?)`); `Router` wraps one. `MeshCallServer` likewise remains the explicit server (`start()` / `close()`) that `serve` builds on.

## HTTP Router

`HTTPRouter` (from `meshcall/web`) is a client-only Router over `fetch`; pass it to a generated client exactly like a WebSocket Router. It runs in Node, browsers, Deno, Bun and edge runtimes, and exposes the built-in Registry/AuthService calls.

```ts
import { HTTPRouter } from "meshcall/web";
import { ChatClient } from "my-chat-client";

const http = await HTTPRouter.connect("https://router.example", { token: "mk_…" });
const chat = await ChatClient.connect(http);
console.log(await chat.withOptions({ timeoutMs: 5000 }).greet("Alice"));
console.log(await http.listServices());
```

Duplex methods are WebSocket-only (`method_not_found`, `reason: "duplex_over_http"`); unary retries, streams, subscriptions and the built-ins work as over WebSocket.

## Explore, Call, and Generate Clients with the CLI

```bash
# Inspect registered services, instances, health, and method signatures
npx meshcall router inspect --router ws://127.0.0.1:8765

# Call a method from the terminal
npx meshcall call chat.v1.Chat.greet '{"name": "Alice"}'

# Generate a Python, TypeScript or Go client from a contract or a Python service
npx meshcall generate service.meshcall.json --language typescript --output generated/chat-ts
npx meshcall generate service.meshcall.json --language python --output generated/chat-py
npx meshcall generate service.meshcall.json --language go --output internal/chat
```

`--language` defaults to `python`. Export a TypeScript service's portable contract with `await writeContract([Chat], "service.meshcall.json")`. Generated clients are typed against `MeshCallRpc`, and their methods mirror the handler's parameter list one to one; `static connect(rpc)` opens the Router or client it is given.

## Subpath Exports

- `meshcall/web` exports `MeshCallHttpClient`, which calls the Router's HTTP v1 API over `fetch` (browsers, Deno, Bun, edge runtimes). Generated clients accept any `MeshCallRpc` — a `Router`, a `MeshCallClient` or a `MeshCallHttpClient`: `new ChatClient(new MeshCallHttpClient("https://router.example", { token }))`. Duplex calls stay WebSocket-only; another origin needs `--http-cors-origin` on the Router.
- `meshcall/otel` exports `otelClientInterceptor()` and `otelServerInterceptor()` for the `interceptors` option of `MeshCallClient` and `MeshCallServer`. They need the optional peer dependency `@opentelemetry/api` and carry W3C trace context as call metadata, matching the Python `meshcall.otel` spans and metrics, so a trace continues across languages and through the Router.

## Experimental

Subscriptions and `Topic` are experimental in v0.3. `@method.subscription({ description, request, outputItem, cursor, resume })` declares a server stream whose generated client resubscribes after a retryable failure, passing the last item's `cursor` as the `resume` request field; `Topic` fans published items out to every subscriber in one process. They are left out of the flow above because their semantics may still change; see [Subscriptions](https://github.com/t4wefan/meshcall/blob/main/docs/protocol.md#subscriptions).

## Documentation

- [Monorepo overview & quickstart](https://github.com/t4wefan/meshcall#readme) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/README.zh-CN.md))
- [User Guide & Cookbook](https://github.com/t4wefan/meshcall/blob/main/docs/guide.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/guide.md))
- [Router authentication & management](https://github.com/t4wefan/meshcall/blob/main/docs/router.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/router.md))
- [Runtime architecture](https://github.com/t4wefan/meshcall/blob/main/docs/architecture.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/architecture.md))
- [Protocol specification](https://github.com/t4wefan/meshcall/blob/main/docs/protocol.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/protocol.md))

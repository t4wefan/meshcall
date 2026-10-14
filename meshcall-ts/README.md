# meshcall (TypeScript SDK)

**English** | [简体中文](https://github.com/t4wefan/meshcall/blob/main/meshcall-ts/README.zh-CN.md)

Asynchronous, typed streaming RPC for TypeScript and Python over WebSocket.
Both the client and service runtimes support unary, server-streaming, and
client-streaming calls over Direct WebSocket or the shared Go Router (TCP and
Unix sockets).

## Install

```bash
yarn add meshcall
# or: npm install meshcall
```

Installing `meshcall` automatically pulls the matching
`meshcall-router-<os>-<cpu>` optional dependency containing the prebuilt Go
Router (`meshcall-router`) and `meshcall` CLI binaries—no Go compiler or Python
runtime is needed. On platforms without a prebuilt package,
`npx meshcall router install` downloads and verifies the matching release into
the per-user cache.

If coding agents work on your project, install the first-party agent skill once:

```bash
npx meshcall init                  # default: .agents/skills/meshcall/SKILL.md + AGENTS.md
npx meshcall init --agent claude   # or target: claude | cursor | copilot | windsurf | all
```

## Define and run a service

Service names carry one version segment and are normalised (`chat` → `chat.v0`;
`chat.v1`, `demo.v1.Greeting`, `chat.v1.2.0` are valid). Every method builder
accepts `description`, `idempotent`, `readOnly`, `destructive` and
`deprecated`; the service accepts `description`. These reach the contract and
the generated clients. Set `MESHCALL_STRICT=1` during development and CI so
advisory contract warnings fail fast at `defineService`.

Define methods with `unaryMethod`, `serverStreamMethod`, or `clientStreamMethod`.
Each builder takes a `request` type contract; unary and client-streaming methods
declare `response`, server-streaming methods declare `outputItem`, and
client-streaming methods declare `inputItem`. JSON Schema draft 2020-12 contracts
validate requests, responses, and stream items through Ajv. Defaults are applied,
but values are not coerced between JSON types.

```typescript
import {
  defineService,
  defineType,
  MeshCallServer,
  serverStreamMethod,
} from "meshcall";

const request = defineType<{ count: number }>("CountRequest", {
  type: "object",
  properties: { count: { type: "integer", minimum: 0 } },
  required: ["count"],
});
const item = defineType<{ value: number }>("CountItem", {
  type: "object",
  properties: { value: { type: "integer" } },
  required: ["value"],
});

const counter = defineService({
  name: "example.v1.Counter",
  description: "Stream sequential integer counts.",
  sourceModule: "counter",
  sourceQualname: "Counter",
  methods: {
    count: serverStreamMethod({
      description: "Yield integers from 0 up to count - 1.",
      readOnly: true,
      request,
      outputItem: item,
      handler: async function* ({ count }, { signal, logger }) {
        logger.info("counting to %d", count);
        for (let value = 0; value < count; value += 1) {
          signal.throwIfAborted();
          yield { value };
        }
      },
    }),
  },
});

const server = new MeshCallServer({
  services: [counter],
  router: { endpoint: "ws://127.0.0.1:8765", instanceId: "counter-ts-1" },
});
await server.start();
// Await server.close({ drain: true }) when the application shuts down.
```

For Direct mode (without a Router), pass `port: 8765` (or
`unixPath: "/tmp/meshcall.sock"`) instead of `router`. In Router mode, services
register before `start()` resolves; a disconnect terminates active calls and
re-registers with exponential backoff (`reconnect` option, a `ReconnectPolicy`)
while `isRunning` stays true.

## Explore, call, and generate clients with the CLI

```bash
# Inspect registered services, instances, health, and method signatures
npx meshcall router inspect --router ws://127.0.0.1:8765

# Call a method from the terminal
npx meshcall call example.v1.Counter.count '{"count": 5}'

# Generate a TypeScript or Python client from a portable contract file
npx meshcall generate service.meshcall.json --language typescript --output generated/client-ts
npx meshcall generate service.meshcall.json --language python --output generated/client-py
```

Export a TypeScript service's portable contract with
`await writeContract([counter], "service.meshcall.json")`.

## Context, options, keepalive, and interceptors

Handlers receive an `RpcContext` containing `signal`, `deadlineMs` and
`remainingMs()`, `callId`, `service`, `method`, `instanceId`, the caller's
`metadata`, `setTrailer(key, value)` for response trailers, and a bound `logger`.
Client-streaming handlers take `(request, items, context)`; the other handlers
take `(request, context)`. Observe `context.signal` during asynchronous work to
release resources on cancellation.

Callers pass `CallOptions`: `timeoutMs` (the whole call, including connecting),
`idleTimeoutMs` (longest gap between stream items), `metadata`, `idempotencyKey`,
`instance`, and `signal`. The client fails a call locally at `timeoutMs` plus a
grace (`deadlineGraceMs`, default `max(1s, 10%)`) so the server's or Router's
own `deadline_exceeded` normally arrives first; a locally raised one carries
`details.source === "client"`. Clients ping every `pingIntervalMs` (15 s) and
treat a missing `pong` after `pingTimeoutMs` (10 s) as a lost connection. Direct
servers close clients silent for `idleTimeoutMs` (45 s); Router-mode servers
ping the Router instead. Exiting a TypeScript `for await` loop cancels its
server stream automatically.

Interceptors wrap every call on either side. A server interceptor is
`(ctx, request, next) => result` passed as `interceptors` to `MeshCallServer`;
a client interceptor is `(info, next) => result` passed in `MeshCallClient`
options, where `info.options` can be replaced to add metadata or change
timeouts and `next(info)` may be called again to retry.

## Start the Go Router and authenticate

`new WebSocketRouter({ port: 8765, authFile: "/path/router-auth.json" })`
starts the bundled Go executable when `start()` is awaited; `stop()` or
`close()` reaps it. Set `binaryPath` or `MESHCALL_ROUTER_BINARY` to override
the bundled binary.

Client credentials use the third constructor option:
`new MeshCallClient(uri, undefined, { auth: { username, password } })`, or
`{ auth: { token } }`. Service credentials go in `router.auth`.

`RouterAuthClient` wraps a `MeshCallClient` and provides `whoami()`,
`issueToken({ ttl_seconds, scopes })`, and `revokeToken(tokenId)`.
Each scope has `service`, optional `methods` (`["*"]` for every method on that
service), and optional `register`. The Router prevents delegation beyond the
issuing account.

## Documentation

- [Monorepo overview & quickstart](https://github.com/t4wefan/meshcall#readme) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/README.zh-CN.md))
- [User Guide & Cookbook](https://github.com/t4wefan/meshcall/blob/main/docs/guide.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/guide.md))
- [Protocol specification](https://github.com/t4wefan/meshcall/blob/main/docs/protocol.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/protocol.md))
- [Runtime architecture](https://github.com/t4wefan/meshcall/blob/main/docs/architecture.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/architecture.md))
- [Router authentication & management](https://github.com/t4wefan/meshcall/blob/main/docs/router.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/router.md))

# meshcall (TypeScript SDK)

**English** | [简体中文](https://github.com/t4wefan/meshcall/blob/main/meshcall-ts/README.zh-CN.md)

**Write cross-language streaming RPC the way you write ordinary TypeScript, Python and Go functions.**

MeshCall is an asynchronous, typed streaming RPC framework for TypeScript, Python and Go over WebSocket, supporting unary, server-streaming, client-streaming and subscription calls (and, as a last resort, restricted duplex calls) routed through the standalone Go Router.

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

Start with a minimal service: a class whose decorated methods are the service's methods, with [zod](https://zod.dev) schemas for what goes in and out (`z` is re-exported, so there is one copy):

```typescript
import { MeshCallServer, method, service, z, type RpcContext } from "meshcall";

const CountRequest = z.object({ count: z.int().min(0) });
const CountItem = z.object({ value: z.int() }).meta({ id: "CountItem" });

@service("example.v1.Counter", { description: "Stream sequential integer counts." })
class Counter {
  @method.serverStream({
    description: "Yield integers from 0 up to count - 1.",
    readOnly: true,
    request: CountRequest,
    outputItem: CountItem,
  })
  async *count({ count }: z.infer<typeof CountRequest>, { signal }: RpcContext) {
    for (let value = 0; value < count; value += 1) {
      signal.throwIfAborted();
      yield { value };
    }
  }
}

const server = new MeshCallServer({
  services: [new Counter()],
  router: { endpoint: "ws://127.0.0.1:8765", instanceId: "counter-ts-1" },
});
await server.start();
```

What happens here:

- `@service(name, { description })` declares the class a MeshCall service; its instances are what `MeshCallServer` serves, so handlers can use `this`
- `@method` (unary), `@method.serverStream`, `@method.clientStream`, `@method.subscription` (with `cursor` / `resume`) and `@method.duplex` (with a `reason`; the restricted shape of last resort) declare the methods. The decorator checks the method against its schemas at compile time: the handler receives the parsed request (defaults applied, unknown fields dropped) and must return what `response` / `outputItem` accepts
- The schemas become the portable contract other languages generate clients from; `.meta({ id: "CountItem" })` names a model there (unnamed ones are named after the method). `description` is required by the types (`""` declares none on purpose)
- `MeshCallServer` registers the service with the Go Router, allowing the same node to both expose services and act as a client calling other services in the mesh

Decorators are TC39 standard decorators: TypeScript 5+ compiles them with no extra flags (do not set `experimentalDecorators`). Plain JavaScript needs a compiler that supports them.

`WebSocketRouter` starts the bundled Router from Node: besides `host` / `port` it takes `unixPath` (Linux, macOS, FreeBSD) or `pipe` (Windows) for the local transport, `maxFrameSize` (default and maximum 64 MiB), `outboxSize` (bytes queued for a peer that is not reading; default the Router's 256 MiB, at least `maxFrameSize`), `maxConnections`, `binaryPath` and `authFile`.

## Explore, Call, and Generate Clients with the CLI

```bash
# Inspect registered services, instances, health, and method signatures
npx meshcall router inspect --router ws://127.0.0.1:8765

# Call a method from the terminal
npx meshcall call example.v1.Counter.count '{"count": 5}'

# Generate a TypeScript, Python or Go client from a contract or Python service
npx meshcall generate service.meshcall.json --language typescript --output generated/client-ts
npx meshcall generate service.meshcall.json --language python --output generated/client-py
npx meshcall generate service.meshcall.json --language go --output internal/client
```

Export a TypeScript service's portable contract with `await writeContract([Counter], "service.meshcall.json")`.

## Subpath Exports

- `meshcall/web` exports `MeshCallHttpClient`, which calls the Router's HTTP v1 API over `fetch` (browsers, Deno, Bun, edge runtimes). Generated clients take the `MeshCallRpc` interface, so pass either a `MeshCallHttpClient` or a `MeshCallClient`: `new ChatClient(new MeshCallHttpClient("https://router.example", { token }))`. Duplex calls stay WebSocket-only; another origin needs `--http-cors-origin` on the Router.
- `meshcall/otel` exports `otelClientInterceptor()` and `otelServerInterceptor()` for the `interceptors` option of `MeshCallClient` and `MeshCallServer`. They need the optional peer dependency `@opentelemetry/api` and carry W3C trace context as call metadata, matching the Python `meshcall.otel` spans and metrics, so a trace continues across languages and through the Router.

## Documentation

- [Monorepo overview & quickstart](https://github.com/t4wefan/meshcall#readme) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/README.zh-CN.md))
- [User Guide & Cookbook](https://github.com/t4wefan/meshcall/blob/main/docs/guide.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/guide.md))
- [Router authentication & management](https://github.com/t4wefan/meshcall/blob/main/docs/router.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/router.md))
- [Runtime architecture](https://github.com/t4wefan/meshcall/blob/main/docs/architecture.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/architecture.md))
- [Protocol specification](https://github.com/t4wefan/meshcall/blob/main/docs/protocol.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/protocol.md))

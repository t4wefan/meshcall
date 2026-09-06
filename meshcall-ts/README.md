# meshcall-ts

The TypeScript runtime package for the MeshCall monorepo. Its package name is
`@meshcall/runtime`.

```bash
go -C ../meshcall-router build -o bin/meshcall-router ./cmd/meshcall-router
yarn install --frozen-lockfile
yarn run check
yarn test
```

Both the client and service runtimes support unary, server-streaming, and
client-streaming calls through the shared Go Router, the recommended topology.
Direct WebSocket listeners remain supported for compatibility and transport tests.
TCP and Unix sockets use the same runtime. Duplex remains experimental and is
not exposed by the TypeScript service API.

Define methods with `unaryMethod`, `serverStreamMethod`, or `clientStreamMethod`.
Each builder takes a `request` type contract; unary and client-streaming methods
declare `response`, server-streaming methods declare `outputItem`, and
client-streaming methods declare `inputItem`. JSON Schema draft 2020-12 contracts
validate requests, responses, and stream items through Ajv. Defaults are applied,
but values are not coerced between JSON types. Legacy method definitions without
type contracts remain supported without payload validation.

Handlers receive an `RpcContext` containing `signal`, `deadlineUnixMs`, `callId`,
`service`, `method`, and a bound `logger`. Client-streaming handlers take
`(request, items, context)`; the other handlers take `(request, context)`.
Observe `context.signal` during asynchronous work to release resources on
cancellation. Handlers still run on Node's event loop.

## Streaming service

```typescript
import {
  defineService, defineType, MeshCallServer, serverStreamMethod,
} from "@meshcall/runtime";

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
  sourceModule: "counter",
  sourceQualname: "Counter",
  methods: {
    count: serverStreamMethod({
      request, outputItem: item,
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
  router: {
    endpoint: process.env.MESHCALL_ROUTER_URL!,
    instanceId: "counter-ts-1",
    auth: { username: "worker", password: process.env.ROUTER_WORKER_PASSWORD! },
  },
});
await server.start();
// Await server.close() when the application shuts down.
```

Set `MESHCALL_ROUTER_URL` to an existing Router endpoint and configure the worker
account to register `example.v1.Counter`. Use `wss://` through a TLS reverse proxy
for network connections. Services register before `start()` resolves. A
disconnect terminates active calls and sets `isRunning` to false; reconnect
explicitly with `start()`.
The runtime does not replay calls.

For local IPC, replace `router.endpoint` with `{ unixPath: "/tmp/router.sock" }`
and keep the same authentication options. Clients accept either a Router URL or
`{ unixPath: "/tmp/router.sock" }`.

`accessLog` defaults to true. `logLevel`, `colorize`, and a compatible `logger`
configure terminal access entries; the default logger writes to stderr.
`maxFrameSize` defaults to 1 MiB and `handshakeTimeoutMs` to 10 seconds.
The client accepts these two limits in its optional third constructor argument.
Stream cancellation and failed calls clean up timers and credit waiters.
Exiting a TypeScript `for await` loop also cancels its server stream.

See [Router architecture](../docs/router.md) for balancing and deployment boundaries.

## Start the Go Router and authenticate

`new WebSocketRouter({ port: 8765, authFile: "/path/router-auth.json" })`
starts the standalone Go executable when `start()` is awaited; `stop()` or
`close()` reaps it. Set `binaryPath` or `MESHCALL_ROUTER_BINARY` for a prebuilt
binary. Neither a Go compiler nor a Python runtime is needed to run it. The npm
package does not bundle that binary. In deployments, connect to a Router owned
by Docker or another process manager. Use the launcher when the application
explicitly owns the Go child process, and close service/client connections
before stopping it. See [startup review](../docs/router-launcher.md).

Client credentials use the third constructor option:
`new MeshCallClient(uri, undefined, { auth: { username, password } })`, or
`{ auth: { token } }`. Service credentials go in `router.auth`.

`RouterAuthClient` wraps a `MeshCallClient` and provides `whoami()`,
`issueToken({ ttl_seconds, scopes })`, and `revokeToken(tokenId)`.
Each scope has `service`, optional `methods` (`["*"]` for every method on that
service), and optional `register`. The Router prevents delegation beyond the
issuing account. See [configuration and token examples](../docs/router.md).

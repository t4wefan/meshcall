# meshcall-ts

The TypeScript runtime package for the MeshCall monorepo. Its package name is
`@meshcall/runtime`.

```bash
yarn install --frozen-lockfile
yarn run check
yarn test
```

Both the client and service runtimes support unary, server-streaming, and
client-streaming calls over Direct WebSocket or the shared Python Router.
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
const server = new MeshCallServer({ services: [counter], port: 8765 });
await server.start();
// Await server.close() when the application shuts down.
```

For Router mode, replace `port` with
`router: { endpoint: "ws://127.0.0.1:8765", instanceId: "counter-ts-1" }`.
Services register before `start()` resolves. A disconnect terminates active
calls and sets `isRunning` to false; reconnect explicitly with `start()`.
The runtime does not replay calls.

For local IPC use `unixPath: "/tmp/meshcall.sock"` on a Direct server, or
`router: { endpoint: { unixPath: "/tmp/router.sock" } }` for Router mode.
Clients accept either a URL or `{ unixPath: "/tmp/meshcall.sock" }`.

`accessLog` defaults to true. `logLevel`, `colorize`, and a compatible `logger`
configure terminal access entries; the default logger writes to stderr.
`maxFrameSize` defaults to 1 MiB and `handshakeTimeoutMs` to 10 seconds.
The client accepts these two limits in its optional third constructor argument.
Stream cancellation and failed calls clean up timers and credit waiters.
Exiting a TypeScript `for await` loop also cancels its server stream.

See [Router architecture](../docs/router.md) for balancing and deployment boundaries.

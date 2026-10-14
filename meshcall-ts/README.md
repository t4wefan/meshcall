# meshcall (TypeScript SDK)

**English** | [简体中文](https://github.com/t4wefan/meshcall/blob/main/meshcall-ts/README.zh-CN.md)

**Write cross-language streaming RPC the way you write ordinary TypeScript and Python functions.**

MeshCall is an asynchronous, typed streaming RPC framework for TypeScript and Python over WebSocket, supporting unary, server-streaming, and client-streaming calls over Direct WebSocket or the standalone Go Router.

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

Start with a minimal service definition:

```typescript
import {
  defineService,
  defineType,
  MeshCallServer,
  serverStreamMethod,
} from "meshcall";

const CountRequest = defineType<{ count: number }>("CountRequest", {
  type: "object",
  properties: { count: { type: "integer", minimum: 0 } },
  required: ["count"],
});

const CountItem = defineType<{ value: number }>("CountItem", {
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
      request: CountRequest,
      outputItem: CountItem,
      handler: async function* ({ count }, { signal }) {
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
```

What happens here:

- `defineType` binds a TypeScript type to a JSON Schema for runtime validation
- `defineService` declares the service contract (`unaryMethod`, `serverStreamMethod`, or `clientStreamMethod`)
- `MeshCallServer` registers the service with the Go Router (or listens directly when given `port: 8765` instead of `router`)

## Explore, Call, and Generate Clients with the CLI

```bash
# Inspect registered services, instances, health, and method signatures
npx meshcall router inspect --router ws://127.0.0.1:8765

# Call a method from the terminal
npx meshcall call example.v1.Counter.count '{"count": 5}'

# Generate a TypeScript or Python client from a contract or Python service
npx meshcall generate service.meshcall.json --language typescript --output generated/client-ts
npx meshcall generate service.meshcall.json --language python --output generated/client-py
```

Export a TypeScript service's portable contract with `await writeContract([counter], "service.meshcall.json")`.

## Documentation

- [Monorepo overview & quickstart](https://github.com/t4wefan/meshcall#readme) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/README.zh-CN.md))
- [User Guide & Cookbook](https://github.com/t4wefan/meshcall/blob/main/docs/guide.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/guide.md))
- [Router authentication & management](https://github.com/t4wefan/meshcall/blob/main/docs/router.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/router.md))
- [Runtime architecture](https://github.com/t4wefan/meshcall/blob/main/docs/architecture.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/architecture.md))
- [Protocol specification](https://github.com/t4wefan/meshcall/blob/main/docs/protocol.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/protocol.md))

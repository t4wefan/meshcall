# meshcall (TypeScript SDK)

[English](./README.md) | **简体中文**

**用写普通 TypeScript / Python / Go 函数的方式，编写跨语言流式 RPC。**

MeshCall 是面向 TypeScript、Python 与 Go 的异步强类型流式 RPC 框架，基于 WebSocket。服务就是一个 `@service` 类，方法用 `@query`（纯读取）、`@mutates`（有副作用）或 `@stream`（服务端流）声明，并用 [zod](https://zod.dev) schema 描述输入输出（`meshcall` 重新导出了 `z`，保证只有一份 zod）。服务注册到独立的 Go Router，客户端通过 Router 调用；各语言生成的客户端接收完全一致、顺序相同的参数列表。客户端流与受限的双向流见下文。

## 安装

```bash
yarn add meshcall
# 或：npm install meshcall
```

安装 `meshcall` 会自动拉取匹配当前平台的 `meshcall-router-<os>-<cpu>` 二进制包，内含预编译的 Go Router（`meshcall-router`）与 `meshcall` CLI——无需安装 Go 编译器或 Python 运行时。

如果项目中使用 AI 编程助手，只需安装一次官方 Agent 技能：

```bash
npx meshcall init                  # 默认生成通用标准：.agents/skills/meshcall/SKILL.md + AGENTS.md
npx meshcall init --agent claude   # 或定向生成：claude | cursor | copilot | windsurf | all
```

## 快速开始 (Quick Start)

服务就是一个类；被装饰的方法即服务的方法，zod schema 描述输入与输出：

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

`mesh.startServing()` 在后台启动并返回 `RunningServer`（`.url`、`.close()`）；`await mesh.serve()` 行为相同，但会阻塞直到收到 `SIGINT` / `SIGTERM`，然后先排空在途调用再退出。传入 `new LocalRouter({ port: 8765 })` 会随 `Mesh` 自动启停本地 `meshcall-router` 子进程；连接已运行的外部 Router 时则传入 `new Router()`（读取 `MESHCALL_ROUTER_URL`，默认为 `ws://127.0.0.1:8765`）。

通过生成的客户端调用服务（客户端的生成方式见下文）：

```typescript
import { Mesh, Router } from "meshcall";
import { ChatClient } from "meshcall-chat-v1-chat-client"; // 生成的客户端包

await using mesh = await Mesh.connect(new Router()); // 读取 MESHCALL_ROUTER_URL 或默认 ws://127.0.0.1:8765
const chat = new ChatClient(mesh);                   // 共享同一条 Mesh 连接

console.log((await chat.greet("Alice")).text);

for await (const message of chat.history("s1", 5)) {
  console.log(message.text);
}

// 单次调用选项不进入业务方法签名。
await chat
  .withOptions({ timeoutMs: 5_000, metadata: { "x-caller-id": "cli" } })
  .send("s1", "hi");
```

这段代码做了什么：

- `@query` 声明纯读取（`readOnly: true, idempotent: true`）；`@mutates` 声明会改变状态的方法（重复执行安全时写 `idempotent: true`，否则默认是 `destructive`）；`@stream` 声明服务端流，即产出 `output` 条目的异步生成器。客户端流用 `@mutates` 加 `streamInput` 声明；duplex 是受限的最后手段（`@mutates` 同时指定 `streamInput`、`streamOutput` 与 `reason`，处理函数接收 `DuplexChannel<In, Out>`）。
- `input` 可以是内联 zod map（`{ session_id: z.string(), text: z.string() }`），按位置绑定：处理函数接收 `(session_id, text)`，生成的客户端也接收完全相同的参数、顺序一致。单个 schema 绑定一个参数，`request: schema` 绑定请求对象，省略 `input` 表示没有业务参数。
- `output`（一元调用也可写 `response`）与 `outputItem` 校验处理函数的返回值；请求会先应用默认值。`.meta({ id: "Message" })` 给模型命名（不命名的按方法名命名）。`description` 在类型上是必填的（写 `""` 表示有意不写）；`@query` 与 `@stream` 必须有输出 schema，在定义类时检查。
- schema 会变成可移植契约，`meshcall generate` 据此生成 Python、TypeScript 与 Go 的类型化客户端。
- `mesh.mount(...)` 与 `mesh.serve()` / `mesh.startServing()` 把实例注册到 Router；同一个 `Mesh` 实例也能调用其他服务（`new OtherClient(mesh)`）。
- `chat.withOptions({ timeoutMs, idleTimeoutMs, metadata, idempotencyKey, instance })` 返回一个共享连接的新视图，为每次调用提供默认选项；`idempotencyKey` 让 Router 对重试的变更调用去重，targeted 服务的每次调用都必须给 `instance`。

装饰器是 TC39 标准装饰器：TypeScript 5 及以上无需额外配置即可编译（不要开启 `experimentalDecorators`）。纯 JavaScript 需要支持标准装饰器的编译器。

## 依赖注入与元数据

处理函数可以拿到仅存在于服务端的值，而不把它们加进契约。`meta(key)` 读取调用元数据头；`dependency(provider, { inject })` 从其他依赖计算出一个值。两者都列在装饰器的 `inject` 数组里，按顺序注入在业务参数之后、`RpcContext` 之前：

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

客户端通过调用元数据传入该头：`chat.withOptions({ metadata: { authorization: "Bearer …" } })`。`user` 与 `authorization` 都不会出现在契约或生成的客户端里。每个依赖在一次调用内最多解析一次；如果 provider 是生成器（`function*` / `async function*`），调用结束时会执行它的 `finally`，因此可以安全地开启事务或连接。`meta(key, { required: true })`、`meta(key, { default: … })` 与 `meta(key, { schema })` 用于强制要求或解析该头；不满足时在处理函数运行前就以 `invalid_argument` 失败。

## `Router`、`LocalRouter` 与 `Mesh`

`Router` 是远端 Router 的纯端点描述符；`LocalRouter` 管理本地 `meshcall-router` 子进程；`Mesh` 是对端运行时——挂载服务实例、在多个生成的客户端之间共享同一条连接，并通过 `mesh.registry` 与 `mesh.auth` 暴露 Router 的内置服务：

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

`mesh.withOptions({ timeoutMs, idleTimeoutMs, metadata, idempotencyKey, instance })` 返回共享连接的新视图，为每次调用附加这些选项。多个端点无需 Router 侧协调即可故障转移：`new Router(["ws://router-a:8765", "ws://router-b:8765"])` 让服务实例向每个 Router 注册，客户端按顺序尝试，并从上次可用的端点开始。

`LocalRouter` 在 Node 中启动内置的 Router：除 `host` / `port` 外，还接受本地传输 `unixPath`（Linux、macOS、FreeBSD）或 `pipe`（Windows），以及 `maxFrameSize`（默认且最大 64 MiB）、`outboxSize`（为不读取数据的对端排队的字节数，默认使用 Router 的 256 MiB，不得小于 `maxFrameSize`）、`maxConnections`、`binaryPath` 与 `authFile`。

`MeshCallClient`（`new MeshCallClient(url, peerId?, options?)`）与 `MeshCallServer`（`start()` / `close()`）是 `Mesh` 底层的连接与服务端原语。

## HTTP 传输与浏览器端 (`meshcall/web`)

当 `Router` 传入 `http://` 或 `https://` 地址时，`Mesh` 底层会自动选择基于 `fetch` 的 HTTP 客户端传输。`meshcall/web` 导出不含任何 Node 依赖、浏览器安全的 `Mesh` 与 `Router`，可在 Node、浏览器、Deno、Bun 和边缘运行时中使用，并提供 `mesh.registry` 与 `mesh.auth`：

```ts
import { Mesh, Router } from "meshcall/web";
import { ChatClient } from "my-chat-client";

const mesh = new Mesh(new Router("https://router.example"), { token: "mk_…" });
const chat = new ChatClient(mesh);
console.log(await chat.withOptions({ timeoutMs: 5000 }).greet("Alice"));
console.log(await mesh.registry.list());
```

duplex 方法只能走 WebSocket（`method_not_found`，`reason: "duplex_over_http"`），在基于 HTTP 的 `Mesh` 上调用 `mesh.mount(...)` 会抛出 `TypeError`；一元重试、流与内置方法的行为都与 WebSocket 一致。

## 使用 CLI 探索、调用与生成客户端

```bash
# 查看已注册的服务、实例、健康状态与方法签名
npx meshcall router inspect --router ws://127.0.0.1:8765

# 在终端直接发起调用
npx meshcall call chat.v1.Chat.greet '{"name": "Alice"}'

# 从可移植契约文件或 Python 服务生成 Python、TypeScript 或 Go 客户端
npx meshcall generate service.meshcall.json --language typescript --output generated/chat-ts
npx meshcall generate service.meshcall.json --language python --output generated/chat-py
npx meshcall generate service.meshcall.json --language go --output internal/chat
```

`--language` 默认为 `python`。使用 `await writeContract([Chat], "service.meshcall.json")` 即可导出 TypeScript 服务的可移植契约文件。生成的客户端面向 `MeshCallRpc` 接口，方法与处理函数的参数列表一一对应。

## 子路径导出

- `meshcall/web` 导出浏览器安全的 `Mesh` 与 `Router`，通过 `fetch` 调用 Router 的 HTTP v1 API（适用于浏览器、Deno、Bun 与边缘运行时）。生成的客户端直接接收 `new ChatClient(mesh)`。duplex 调用只能走 WebSocket；跨域调用需要 Router 配置 `--http-cors-origin`。
- `meshcall/otel` 导出 `otelClientInterceptor()` 与 `otelServerInterceptor()`，用于 `Mesh` 与 `mesh.serve()` 的 `interceptors` 选项。它们依赖可选的 peer dependency `@opentelemetry/api`，以调用元数据传递 W3C trace context，与 Python `meshcall.otel` 的 span 和指标一致，因此链路可以跨语言、穿过 Router 延续。

## 相关文档

- [仓库总览与快速上手](../README.zh-CN.md) ([English](../README.md))
- [快速上手与实战指南 (Cookbook)](../docs/zh-CN/guide.md) ([English](../docs/guide.md))
- [Router 架构与鉴权](../docs/zh-CN/router.md) ([English](../docs/router.md))
- [运行时架构设计](../docs/zh-CN/architecture.md) ([English](../docs/architecture.md))
- [协议规范](../docs/zh-CN/protocol.md) ([English](../docs/protocol.md))

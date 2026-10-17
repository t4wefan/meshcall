# meshcall (TypeScript SDK)

[English](./README.md) | **简体中文**

**用写普通 TypeScript / Python / Go 函数的方式，编写跨语言流式 RPC。**

MeshCall 是面向 TypeScript、Python 与 Go 的异步强类型流式 RPC 框架，基于 WebSocket。服务就是一个 `@service` 类，方法用 `@query`（纯读取）、`@mutates`（有副作用）或 `@stream`（服务端流）声明，并用 [zod](https://zod.dev) schema 描述输入输出（`meshcall` 重新导出了 `z`，保证只有一份 zod）。服务注册到独立的 Go Router，客户端通过 Router 调用；各语言生成的客户端接收完全一致、顺序相同的参数列表。客户端流、受限的双向流与实验性的订阅见下文。

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

`serve` 在后台启动并返回 `RunningServer`（`.url`、`.close()`）；`run(...)` 行为相同，但会阻塞直到收到 `SIGINT` / `SIGTERM`，然后先排空在途调用再退出。两者默认使用 Router 模式：`await serve(new Chat())` 会读取 `MESHCALL_ROUTER_URL`（默认为 `ws://127.0.0.1:8765`）；当地址在本机且还没有 Router 监听时，会自动拉起内置 Router。

通过生成的客户端调用服务（客户端的生成方式见下文）：

```typescript
import { Router } from "meshcall";
import { ChatClient } from "meshcall-chat-v1-chat-client"; // 生成的客户端包

const router = await Router.connect();          // 读取 MESHCALL_ROUTER_URL，默认为 ws://127.0.0.1:8765
const chat = await ChatClient.connect(router);  // 建立并复用同一条连接
try {
  console.log((await chat.greet("Alice")).text);

  for await (const message of chat.history("s1", 5)) {
    console.log(message.text);
  }

  // 单次调用选项不进入业务方法签名。
  await chat
    .withOptions({ timeoutMs: 5_000, metadata: { "x-caller-id": "cli" } })
    .send("s1", "hi");
} finally {
  await router.close();
}
```

这段代码做了什么：

- `@query` 声明纯读取（`readOnly: true, idempotent: true`）；`@mutates` 声明会改变状态的方法（重复执行安全时写 `idempotent: true`，否则默认是 `destructive`）；`@stream` 声明服务端流，即产出 `output` 条目的异步生成器。客户端流用 `@mutates` 加 `streamInput` 声明；duplex 是受限的最后手段（`streamInput` + `streamOutput` + `reason`）。长写法 `@method`、`@method.serverStream`、`@method.clientStream` 与 `@method.duplex` 仍然可用。
- `input` 可以是内联 zod map（`{ session_id: z.string(), text: z.string() }`），按位置绑定：处理函数接收 `(session_id, text)`，生成的客户端也接收完全相同的参数、顺序一致。单个 schema 绑定一个参数，`request: schema` 绑定请求对象，省略 `input` 表示没有业务参数。
- `output`（一元调用也可写 `response`）与 `outputItem` 校验处理函数的返回值；请求会先应用默认值。`.meta({ id: "Message" })` 给模型命名（不命名的按方法名命名）。`description` 在类型上是必填的（写 `""` 表示有意不写）；`@query` 与 `@stream` 必须有输出 schema，在定义类时检查。
- schema 会变成可移植契约，`meshcall generate` 据此生成 Python、TypeScript 与 Go 的类型化客户端。
- `serve` 把实例注册到 Router；同一个进程也能调用其他服务——`Router` 本身也是客户端。
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

## Router

`Router` 是服务端与客户端两侧统一的操作入口：

- **配置服务端**：把 `new Router({ url, auth, metricsListen, … })` 传给 `serve` / `run`。不传参数时读取 `MESHCALL_ROUTER_URL`（默认为 `ws://127.0.0.1:8765`）；当地址是本地端点且无人监听时，`serve` / `run` 会自动拉起内置 Router（`new Router({ spawnIfLocal: false })` 可关闭）。传 `router: null` 则完全不使用 Router（Direct 模式，监听 `listen`，默认为 `ws://127.0.0.1:0`）。
- **共享一条连接**：`const router = await Router.connect(endpointOrOptions?)`，然后交给任意多个生成的客户端：`new ChatClient(router)`、`new BillingClient(router)`。`await ChatClient.connect(router)` 等价，但会先建立连接。
- **调用内置服务**：`listServices()`、`describeService(name)`、`whoami()`、`issueToken(scopes, ttlSeconds)`、`revokeToken(tokenId)`。

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

`withOptions({ timeoutMs, idleTimeoutMs, metadata, idempotencyKey, instance })` 返回共享连接的新视图，为每次调用附加这些选项。`Router.connect()` 不会启动 Router；只有 `serve` / `run` 会拉起内置二进制。多个端点无需 Router 侧协调即可故障转移：`new Router(["ws://router-a:8765", "ws://router-b:8765"])` 让服务实例向每个 Router 注册，客户端按顺序尝试，并从上次可用的端点开始。

`WebSocketRouter` 在 Node 中启动内置的 Router：除 `host` / `port` 外，还接受本地传输 `unixPath`（Linux、macOS、FreeBSD）或 `pipe`（Windows），以及 `maxFrameSize`（默认且最大 64 MiB）、`outboxSize`（为不读取数据的对端排队的字节数，默认使用 Router 的 256 MiB，不得小于 `maxFrameSize`）、`maxConnections`、`binaryPath` 与 `authFile`。

`MeshCallClient` 是更底层的连接（`new MeshCallClient(url, peerId?, options?)`），`Router` 在它之上封装。`MeshCallServer` 同样是显式的服务器（`start()` / `close()`），`serve` 构建于其上。

## HTTP Router

`HTTPRouter`（来自 `meshcall/web`）是使用 `fetch` 的客户端 Router；像 WebSocket Router 一样传给生成的客户端即可。它可在 Node、浏览器、Deno、Bun 和边缘运行时中使用，并提供内置的 Registry/AuthService 调用。

```ts
import { HTTPRouter } from "meshcall/web";
import { ChatClient } from "my-chat-client";

const http = await HTTPRouter.connect("https://router.example", { token: "mk_…" });
const chat = await ChatClient.connect(http);
console.log(await chat.withOptions({ timeoutMs: 5000 }).greet("Alice"));
console.log(await http.listServices());
```

duplex 方法只能走 WebSocket（`method_not_found`，`reason: "duplex_over_http"`）；一元重试、流、订阅与内置方法的行为都与 WebSocket 一致。

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

`--language` 默认为 `python`。使用 `await writeContract([Chat], "service.meshcall.json")` 即可导出 TypeScript 服务的可移植契约文件。生成的客户端面向 `MeshCallRpc` 接口，方法与处理函数的参数列表一一对应；`static connect(rpc)` 会先打开传给它的 Router 或客户端。

## 子路径导出

- `meshcall/web` 导出 `MeshCallHttpClient`，它通过 `fetch` 调用 Router 的 HTTP v1 API（适用于浏览器、Deno、Bun 与边缘运行时）。生成的客户端接受任意 `MeshCallRpc`——`Router`、`MeshCallClient` 或 `MeshCallHttpClient`：`new ChatClient(new MeshCallHttpClient("https://router.example", { token }))`。duplex 调用仍只能走 WebSocket；跨域调用需要 Router 配置 `--http-cors-origin`。
- `meshcall/otel` 导出 `otelClientInterceptor()` 与 `otelServerInterceptor()`，用于 `MeshCallClient` 与 `MeshCallServer` 的 `interceptors` 选项。它们依赖可选的 peer dependency `@opentelemetry/api`，以调用元数据传递 W3C trace context，与 Python `meshcall.otel` 的 span 和指标一致，因此链路可以跨语言、穿过 Router 延续。

## 实验特性

订阅与 `Topic` 在 v0.3 中是**实验特性**。`@method.subscription({ description, request, outputItem, cursor, resume })` 声明一个服务端流，生成的客户端在可重试失败后会自动重订阅，并把最后一条的 `cursor` 作为请求中的 `resume` 字段传回；`Topic` 把发布的条目分发给同一进程内的所有订阅者。由于语义仍可能调整，它们没有放进上面的主流程；详见[订阅](../docs/zh-CN/protocol.md#订阅subscriptions)。

## 相关文档

- [仓库总览与快速上手](../README.zh-CN.md) ([English](../README.md))
- [快速上手与实战指南 (Cookbook)](../docs/zh-CN/guide.md) ([English](../docs/guide.md))
- [Router 架构与鉴权](../docs/zh-CN/router.md) ([English](../docs/router.md))
- [运行时架构设计](../docs/zh-CN/architecture.md) ([English](../docs/architecture.md))
- [协议规范](../docs/zh-CN/protocol.md) ([English](../docs/protocol.md))

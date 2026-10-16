# meshcall (TypeScript SDK)

[English](./README.md) | **简体中文**

**用写普通 TypeScript / Python / Go 函数的方式，编写跨语言流式 RPC。**

MeshCall 是面向 TypeScript、Python 与 Go 的异步强类型流式 RPC 框架，由独立的 Go Router 提供路由支撑，支持一元调用（Unary）、服务端流（Server Stream）、客户端流（Client Stream）与订阅（Subscription），以及作为最后手段、受限制的双向流（duplex）调用。

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

从最小的服务开始：一个类，被装饰的方法就是服务的方法，用 [zod](https://zod.dev) schema 描述输入和输出（`meshcall` 重新导出了 `z`，保证只有一份 zod）：

```typescript
import { MeshCallServer, method, service, z, type RpcContext } from "meshcall";

const CountRequest = z.object({ count: z.int().min(0) });
const CountItem = z.object({ value: z.int() }).meta({ id: "CountItem" });

@service("example.v1.Counter", { description: "流式输出递增整数。" })
class Counter {
  @method.serverStream({
    description: "依次产出从 0 到 count - 1 的整数。",
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

这段代码做了什么：

- `@service(name, { description })` 把类声明为 MeshCall 服务；`MeshCallServer` 服务的是它的实例，所以处理函数里可以用 `this`
- `@method`（一元）、`@method.serverStream`、`@method.clientStream`、`@method.subscription`（带 `cursor` / `resume`）和 `@method.duplex`（必须给 `reason`，受限的最后手段）声明方法。装饰器在编译期用 schema 检查方法签名：处理函数拿到的是解析后的请求（已填默认值、去掉未知字段），返回值必须符合 `response` / `outputItem`
- schema 会变成可移植契约，其他语言据此生成客户端；`.meta({ id: "CountItem" })` 给模型命名（不命名的按方法名命名）。`description` 在类型上是必填的（写 `""` 表示有意不写）
- `MeshCallServer` 将服务注册到 Go Router，使同一个节点既能对外提供服务，也能同时作为客户端调用网格中的其他服务

装饰器是 TC39 标准装饰器：TypeScript 5 及以上无需额外配置即可编译（不要开启 `experimentalDecorators`）。纯 JavaScript 需要支持标准装饰器的编译器。

`WebSocketRouter` 在 Node 中启动内置的 Router：除 `host` / `port` 外，还接受本地传输 `unixPath`（Linux、macOS、FreeBSD）或 `pipe`（Windows），以及 `maxFrameSize`（默认且最大 64 MiB）、`outboxSize`（为不读取数据的对端排队的字节数，默认使用 Router 的 256 MiB，不得小于 `maxFrameSize`）、`maxConnections`、`binaryPath` 与 `authFile`。

订阅与双向流的规则见协议规范的[订阅](../docs/zh-CN/protocol.md#订阅subscriptions)与[双向流调用](../docs/zh-CN/protocol.md#双向流调用duplex-calls)：`@method.subscription` 的客户端在可重试失败后自动从最后一个 cursor 恢复，`Topic` 把发布的条目分发给进程内所有订阅者；`@method.duplex` 必须给出 `reason`，不重试、不去重、不能通过 HTTP 调用。

## 使用 CLI 探索、调用与生成客户端

```bash
# 查看已注册的服务、实例、健康状态与方法签名
npx meshcall router inspect --router ws://127.0.0.1:8765

# 在终端直接发起调用
npx meshcall call example.v1.Counter.count '{"count": 5}'

# 从可移植契约文件生成 TypeScript、Python 或 Go 客户端
npx meshcall generate service.meshcall.json --language typescript --output generated/client-ts
npx meshcall generate service.meshcall.json --language python --output generated/client-py
npx meshcall generate service.meshcall.json --language go --output internal/client
```

使用 `await writeContract([counter], "service.meshcall.json")` 即可导出 TypeScript 服务的可移植契约文件。

## 子路径导出

- `meshcall/web` 导出 `MeshCallHttpClient`，它通过 `fetch` 调用 Router 的 HTTP v1 API（适用于浏览器、Deno、Bun 与边缘运行时）。生成的客户端接受 `MeshCallRpc` 接口，因此既可以传入 `MeshCallHttpClient`，也可以传入 `MeshCallClient`：`new ChatClient(new MeshCallHttpClient("https://router.example", { token }))`。duplex 调用仍只能走 WebSocket；跨域调用需要 Router 配置 `--http-cors-origin`。
- `meshcall/otel` 导出 `otelClientInterceptor()` 与 `otelServerInterceptor()`，用于 `MeshCallClient` 与 `MeshCallServer` 的 `interceptors` 选项。它们依赖可选的 peer dependency `@opentelemetry/api`，以调用元数据传递 W3C trace context，与 Python `meshcall.otel` 的 span 和指标一致，因此链路可以跨语言、穿过 Router 延续。

## 相关文档

- [仓库总览与快速上手](../README.zh-CN.md) ([English](../README.md))
- [快速上手与实战指南 (Cookbook)](../docs/zh-CN/guide.md) ([English](../docs/guide.md))
- [Router 架构与鉴权](../docs/zh-CN/router.md) ([English](../docs/router.md))
- [运行时架构设计](../docs/zh-CN/architecture.md) ([English](../docs/architecture.md))
- [协议规范](../docs/zh-CN/protocol.md) ([English](../docs/protocol.md))

# meshcall (TypeScript SDK)

[English](./README.md) | **简体中文**

基于 WebSocket 的 TypeScript 与 Python 异步强类型流式 RPC 框架。
客户端与服务端运行时均支持在直连 WebSocket 或共享 Go Router（TCP 与 Unix 套接字）上运行一元调用（Unary）、服务端流（Server Stream）与客户端流（Client Stream）。

## 安装

```bash
yarn add meshcall
# 或：npm install meshcall
```

安装 `meshcall` 会自动拉取当前平台匹配的 `meshcall-router-<os>-<cpu>` 可选依赖包，内含预编译的 Go Router（`meshcall-router`）与 `meshcall` CLI 二进制——无需 Go 编译器或 Python 运行时。在没有预编译包的平台上，运行 `npx meshcall router install` 会自动下载并校验对应版本的发行二进制至用户级缓存。

如果项目中使用 AI 编程助手，只需安装一次官方 Agent 技能：

```bash
npx meshcall init                  # 默认生成通用标准：.agents/skills/meshcall/SKILL.md + AGENTS.md
npx meshcall init --agent claude   # 或定向生成：claude | cursor | copilot | windsurf | all
```

## 定义并运行服务

服务名称包含一个版本段并会被规范化（`chat` → `chat.v0`；`chat.v1`、`demo.v1.Greeting`、`chat.v1.2.0` 均为合法名称）。每个方法构建器均接受 `description`、`idempotent`、`readOnly`、`destructive` 与 `deprecated`；服务定义接受 `description`。这些元数据会进入可移植契约与生成的客户端中。在开发与 CI 中设置 `MESHCALL_STRICT=1`，可让契约警告在 `defineService` 阶段快速失败并给出修复提示。

使用 `unaryMethod`、`serverStreamMethod` 或 `clientStreamMethod` 定义方法。每个构建器接受 `request` 类型契约；一元与客户端流方法声明 `response`，服务端流方法声明 `outputItem`，客户端流方法声明 `inputItem`。JSON Schema draft 2020-12 契约通过 Ajv 校验请求、响应与流元素，自动应用默认值，但不在 JSON 类型之间做隐式转换。

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
  description: "流式输出递增整数。",
  sourceModule: "counter",
  sourceQualname: "Counter",
  methods: {
    count: serverStreamMethod({
      description: "依次产出从 0 到 count - 1 的整数。",
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
// 应用关闭时调用 await server.close({ drain: true }) 实现优雅排空。
```

若使用直连模式（无需 Router），传入 `port: 8765`（或 `unixPath: "/tmp/meshcall.sock"`）代替 `router`。在 Router 模式下，服务会在 `start()` 完成前注册完毕；断线会终止进行中的调用，并在 `isRunning` 为 `true` 期间按指数退避（`reconnect` 选项，类型为 `ReconnectPolicy`）自动重连并重新注册。

## 使用 CLI 探索、调用与生成客户端

```bash
# 查看已注册的服务、实例、健康状态与方法签名
npx meshcall router inspect --router ws://127.0.0.1:8765

# 在终端直接发起调用
npx meshcall call example.v1.Counter.count '{"count": 5}'

# 从可移植契约文件生成 TypeScript 或 Python 客户端
npx meshcall generate service.meshcall.json --language typescript --output generated/client-ts
npx meshcall generate service.meshcall.json --language python --output generated/client-py
```

使用 `await writeContract([counter], "service.meshcall.json")` 导出 TypeScript 服务的可移植契约文件。

## 上下文、调用选项、保活与拦截器

处理函数接收 `RpcContext`，内含 `signal`、`deadlineMs` 与 `remainingMs()`、`callId`、`service`、`method`、`instanceId`、调用方传入的 `metadata`、用于设置响应尾部的 `setTrailer(key, value)` 以及绑定的 `logger`。客户端流处理函数签名为 `(request, items, context)`；其余处理函数签名为 `(request, context)`。在异步工作期间监听 `context.signal`，以便在调用取消时及时释放资源。

调用方传入 `CallOptions`：`timeoutMs`（含建连在内的整次调用超时）、`idleTimeoutMs`（流元素之间的最大空闲间隔）、`metadata`、`idempotencyKey`、`instance` 与 `signal`。客户端会在 `timeoutMs` 加上宽限期（`deadlineGraceMs`，默认 `max(1s, 10%)`）后在本地判定超时失败，从而让服务端或 Router 的 `deadline_exceeded` 优先到达；本地触发的超时携带 `details.source === "client"`。客户端每隔 `pingIntervalMs`（15 秒）发送一次 ping，超过 `pingTimeoutMs`（10 秒）未收到 `pong` 即视为连接断开。直连模式服务端会关闭静默超过 `idleTimeoutMs`（45 秒）的客户端；Router 模式服务端则会主动 ping Router。提前退出 TypeScript 的 `for await` 循环会自动取消对应的服务端流。

拦截器可包裹任意一侧的所有调用。服务端拦截器签名为 `(ctx, request, next) => result`，通过 `MeshCallServer` 的 `interceptors` 传入；客户端拦截器签名为 `(info, next) => result`，通过 `MeshCallClient` 选项传入，可替换 `info.options` 来注入元数据或调整超时，也可再次调用 `next(info)` 实现重试。

## 启动 Go Router 与鉴权

`new WebSocketRouter({ port: 8765, authFile: "/path/router-auth.json" })` 在 `await start()` 时启动内置的 Go 可执行文件；`stop()` 或 `close()` 会回收该子进程。可通过 `binaryPath` 或 `MESHCALL_ROUTER_BINARY` 覆盖内置二进制路径。

客户端凭据通过构造函数的第三个选项传入：`new MeshCallClient(uri, undefined, { auth: { username, password } })` 或 `{ auth: { token } }`。服务端凭据放在 `router.auth` 中。

`RouterAuthClient` 包装 `MeshCallClient`，提供 `whoami()`、`issueToken({ ttl_seconds, scopes })` 与 `revokeToken(tokenId)`。每个 scope 包含 `service`、可选的 `methods`（`["*"]` 代表该服务的所有方法）与可选的 `register`。Router 会阻止超出签发账户自身权限的越权委托。

## 相关文档

- [仓库总览与快速上手](../README.zh-CN.md)
- [使用指南与场景速查手册 (Cookbook)](../docs/zh-CN/guide.md)
- [协议规范](../docs/zh-CN/protocol.md)
- [运行时架构](../docs/zh-CN/architecture.md)
- [Router 鉴权与管理](../docs/zh-CN/router.md)

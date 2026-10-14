# meshcall (TypeScript SDK)

[English](./README.md) | **简体中文**

**用写普通 TypeScript / Python 异步函数的方式，编写跨语言流式 RPC。**

MeshCall 是面向 TypeScript 与 Python 的异步强类型流式 RPC 框架，由独立的 Go Router 提供路由支撑，支持一元调用（Unary）、服务端流（Server Stream）与客户端流（Client Stream）。

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

从最小的服务定义开始：

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
  description: "流式输出递增整数。",
  sourceModule: "counter",
  sourceQualname: "Counter",
  methods: {
    count: serverStreamMethod({
      description: "依次产出从 0 到 count - 1 的整数。",
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

这段代码做了什么：

- `defineType` 将 TypeScript 类型与用于运行时校验的 JSON Schema 绑定
- `defineService` 声明服务契约（支持 `unaryMethod`、`serverStreamMethod` 或 `clientStreamMethod`）
- `MeshCallServer` 将服务注册到 Go Router，使同一个节点既能对外提供服务，也能同时作为客户端调用网格中的其他服务

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

使用 `await writeContract([counter], "service.meshcall.json")` 即可导出 TypeScript 服务的可移植契约文件。

## 相关文档

- [仓库总览与快速上手](../README.zh-CN.md) ([English](../README.md))
- [快速上手与实战指南 (Cookbook)](../docs/zh-CN/guide.md) ([English](../docs/guide.md))
- [Router 架构与鉴权](../docs/zh-CN/router.md) ([English](../docs/router.md))
- [运行时架构设计](../docs/zh-CN/architecture.md) ([English](../docs/architecture.md))
- [协议规范](../docs/zh-CN/protocol.md) ([English](../docs/protocol.md))

<p align="center">
  <img src="docs/assets/wordmark.svg" alt="MeshCall — Typed Streaming RPC for Python, TypeScript & Go" width="300">
</p>

<p align="center">
  <a href="./README.md">English</a> | <strong>简体中文</strong>
</p>

# MeshCall

**用写普通 Python / TypeScript 异步函数的方式，编写跨语言流式 RPC。**

MeshCall 是一个面向 **Python** 与 **TypeScript** 的异步强类型流式 RPC 框架，由独立的 **Go Router** 提供高性能路由支撑。

- **用普通函数声明服务** —— 函数参数即请求 Schema，返回值类型即响应 Schema
- **原生流式编程** —— 服务端流只需 `yield`，客户端流只需 `async for`
- **告别手写 `.proto` 文件** —— 一条命令即可从 Python 服务生成强类型 TypeScript 客户端（反之亦然）
- **将基础设施复杂度留在运行时边界之外** —— 路由转发、负载均衡、背压流控、超时控制与断线重连由 Router 与 Driver 承担，不侵入业务代码
- **对人类与 AI 编程助手同样友好** —— 任何运行中的服务都自带契约描述，可直接从终端查看和调用

---

## 安装

```bash
# Python (3.11+)
uv add meshcall
# 或: pip install meshcall

# TypeScript / Node.js (20+)
yarn add meshcall
# 或: npm install meshcall
```

预编译的 `meshcall-router` 与 `meshcall` CLI 二进制已内置在各平台安装包中，无需安装 Go 工具链。

---

## 快速开始 (Quick Start)

从最小的服务定义开始：

```python
from pydantic import BaseModel
from meshcall import method, service


class Reply(BaseModel):
    message: str


@service("demo.v1.Greeter")
class Greeter:
    """问候服务。"""

    @method(read_only=True)
    async def greet(self, name: str) -> Reply:
        """向指定名字返回问候语。"""
        return Reply(message=f"hello, {name}")
```

这几行代码已经展示了 MeshCall 的基本形态：

- 用 `@service(...)` 声明一个服务
- 用 `@method(...)` 暴露一个方法
- 编写普通的 `async def` 函数——它的参数（`name: str`）与返回模型（`Reply`）**本身就是契约**
- 服务定义中不包含任何网络传输或连接管理代码

当你继续深入时，MeshCall 的所有能力都建立在这个极简模型之上：

- **`Server` + `Driver` 负责运行时边界**（无论是点对点直连，还是接入共享的 Go Router）
- **流式通信** 只是普通的异步生成器（`@method.server_stream`）或异步迭代器（`@method.client_stream`）
- **上下文与日志**（`ctx: RpcContext`、`logger: RpcLogger`）仅在处理函数声明需要时按需注入

下面是一个可直接运行的完整示例，展示如何启动 Router、注册服务，并发起普通调用与服务端流调用：

```python
import asyncio
from collections.abc import AsyncIterator
from pydantic import BaseModel
from meshcall import RpcServer, WebSocketRouter, method, service
from meshcall.client import ClientBase
from meshcall.drivers import WebSocketClientDriver, WebSocketRouterServerDriver


class Reply(BaseModel):
    message: str


class Tick(BaseModel):
    step: int


@service("demo.v1.Greeter")
class Greeter:
    """问候与倒计时服务。"""

    @method(read_only=True)
    async def greet(self, name: str) -> Reply:
        """向指定名字返回问候语。"""
        return Reply(message=f"hello, {name}")

    @method.server_stream(read_only=True)
    async def countdown(self, start: int) -> AsyncIterator[Tick]:
        """从 start 倒数到 1 并流式推送。"""
        for n in range(start, 0, -1):
            yield Tick(step=n)


async def main() -> None:
    async with WebSocketRouter(host="127.0.0.1", port=8765):
        server = RpcServer(
            services=[Greeter()],
            driver=WebSocketRouterServerDriver("ws://127.0.0.1:8765", instance_id="greeter-1"),
        )
        await server.start()

        async with ClientBase(WebSocketClientDriver("ws://127.0.0.1:8765")) as client:
            reply = await client.call_unary("demo.v1.Greeter", "greet", {"name": "Alice"})
            print(reply)  # {'message': 'hello, Alice'}

            async for tick in client.call_server_stream("demo.v1.Greeter", "countdown", {"start": 3}):
                print(tick)  # {'step': 3}, {'step': 2}, {'step': 1}

        await server.stop(drain=True)


if __name__ == "__main__":
    asyncio.run(main())
```

这段代码做了什么：

- `WebSocketRouter` 在后台启动内置的 Go Router
- `RpcServer` 通过 `WebSocketRouterServerDriver` 将 `Greeter` 注册到 Router
- `greet` 处理请求/响应调用，并根据 Pydantic 模型自动校验入参和返回值
- `countdown` 通过 `yield` 流式推送 `Tick` 条目，底层自动完成信用窗口背压控制

---

## 为什么选择 MeshCall？

在构建跨语言服务（例如 Python AI 后端与 TypeScript/Node 应用协同）时，开发者往往被迫在**繁重的工具链仪式**（手写 `.proto`、配置 `protoc` 插件、维护生成桩代码）与**脆弱的手写胶水代码**（FastAPI + 原生 WebSocket，缺少类型校验、流控背压与取消传播）之间二选一。

MeshCall 试图回答一个更纯粹的问题：

**如何让跨语言远程调用拥有像写本地异步函数一样的开发体验，同时保留生产级 RPC 的严格语义？**

1. **代码即契约** —— 直接使用 Python（Pydantic v2）或 TypeScript（JSON Schema）声明类型，框架自动提取可移植契约。
2. **统一的流式模型** —— 一元调用（`request -> response`）、服务端流（`request -> stream`）与客户端流（`stream -> response`）共享同一套连接、错误模型与背压机制。
3. **运行时复杂度收敛于 Router** —— 独立的 Go Router 负责服务发现、负载均衡、健康检查、熔断与超时兜底，且自身保持无状态，不会成为系统的状态单点。
4. **运行时自描述** —— 任何在线服务无需翻阅源码，即可在终端中直接查看签名与发起调试调用。

---

## 核心能力递进

### 1. 按需注入上下文与日志

如果处理函数需要读取调用元数据、剩余超时时间、设置响应尾部（Trailers）或输出结构化日志，只需在函数签名中声明 `ctx: RpcContext` 或 `logger: RpcLogger`：

```python
from meshcall import RpcContext, RpcLogger, method

@method(idempotent=True)
async def greet(self, name: str, ctx: RpcContext, logger: RpcLogger) -> Reply:
    """返回问候语，并在响应尾部附带处理该请求的实例 ID。"""
    logger.info("greeting {}", name)
    ctx.set_trailer("served_by", ctx.instance_id or "direct")
    return Reply(message=f"hello, {name}")
```

`ctx` 与 `logger` 由运行时在每次调用时自动注入，**不会**出现在对外的请求契约或生成的客户端参数中。

---

### 2. 跨语言强类型客户端生成

无需手写字典调用 `ClientBase`，只需一条命令即可为 Python 或 TypeScript 生成自带类型提示的客户端包：

```bash
# 从 Python 服务直接生成 TypeScript 客户端包
uv run meshcall generate my_app.service:Greeter --language typescript --output ./greeter-ts

# 或生成单文件版 Python 客户端
uv run meshcall generate my_app.service:Greeter --single-file --output ./greeter_client.py
```

在 TypeScript 中直接获得完整的 IDE 自动补全与运行时响应校验：

```typescript
import { GreeterClient } from "meshcall-demo-v1-greeter-client";

const client = new GreeterClient("ws://127.0.0.1:8765");
try {
  const reply = await client.greet("Alice");
  console.log(reply.message);

  for await (const tick of client.countdown(3)) {
    console.log(tick.step);
  }
} finally {
  await client.close();
}
```

---

### 3. 在终端中探索与调用服务

当 Router 运行时，你可以随时通过 CLI 查看网格中的所有服务并直接发起调用：

```bash
# 查看已注册的服务、在线实例、健康状态与方法签名
uv run meshcall router inspect

# 直接用 JSON 调用一元方法或流式方法
uv run meshcall call demo.v1.Greeter.greet '{"name": "Alice"}'
uv run meshcall call demo.v1.Greeter.countdown '{"start": 3}'
```

如果传错了字段名或类型，`meshcall call` 会自动结合服务端实时契约给出精确到字段路径的修复提示。

---

### 4. 原生 AI 编程助手集成

如果你使用 AI 编程助手开发项目，在项目根目录执行一次 `meshcall init`：

```bash
# 默认：安装通用开放标准（.agents/skills/meshcall/SKILL.md + 更新 AGENTS.md）
# 原生支持 OpenAI Codex、Google Antigravity、OpenCode、Amp 等主流 Agent
uv run meshcall init

# 或通过 --agent 定向生成对应工具的配置（claude | cursor | copilot | windsurf | all）：
uv run meshcall init --agent claude
uv run meshcall init --agent cursor
uv run meshcall init --agent all
```

安装后，AI 助手会自动掌握用 `meshcall router inspect` 探测在线服务、用 `meshcall call` 调试接口，并按照 MeshCall 最佳实践编写服务与客户端。

---

### 5. 直连模式与 Router 模式无缝切换

当部署拓扑从单机点对点演进为多实例服务网格时，你的服务代码无需任何修改，只需替换 `driver`：

```python
# 点对点直连模式（客户端直接连接服务端，无需启动 Router）
driver = WebSocketDirectServerDriver(host="127.0.0.1", port=8765)

# Router 网格模式（多服务复用连接、自动负载均衡与容灾）
driver = WebSocketRouterServerDriver("ws://127.0.0.1:8765", instance_id="greeter-1")
```

---

## 可运行示例

`demo/` 与 `best-practice/` 下的每个目录都是独立可运行的示例项目：

```bash
uv run --project demo/quickstart demo   # Python 最小一元与流式调用示例
uv run --project demo/showcase demo     # Python 全特性演练
uv run --project demo/router demo       # 负载均衡与 Router 路由特性演示
uv run --project demo/py2ts demo        # Python 服务 -> 生成 TypeScript 客户端跨语言调用
uv run --project demo/ts2py demo        # TypeScript 服务 -> 生成 Python 客户端跨语言调用
```

---

## 进阶文档导航

当你需要了解生产环境进阶特性——负载均衡策略、有状态实例定向调用（`targeted=True`）、超时与幂等重试、优雅停机（Drain）、Router 账号鉴权或底层帧协议时，请查阅以下文档：

| 文档 | 内容概览 | English |
| --- | --- | --- |
| **[快速上手与实战指南 (Cookbook)](docs/zh-CN/guide.md)** | 涵盖流式调用、负载均衡、有状态节点定向调用、超时重试、优雅停机、鉴权与拦截器的完整实战范例 | **[User Guide & Cookbook](docs/guide.md)** |
| **[Python SDK 指南](meshcall-py/README.zh-CN.md)** | Python 包安装与核心 API 速查 | [Python SDK](meshcall-py/README.md) |
| **[TypeScript SDK 指南](meshcall-ts/README.zh-CN.md)** | TypeScript 包安装与核心 API 速查 | [TypeScript SDK](meshcall-ts/README.md) |
| **[Go Router 部署指南](meshcall-router/README.zh-CN.md)** | Router 启动参数、密码哈希生成与 Docker 部署 | [Go Router & CLI](meshcall-router/README.md) |
| **[Router 架构与鉴权](docs/zh-CN/router.md)** | 路由模型、多 Router 容灾、ACL 账号配置与受限临时 Token | [Router & Auth Guide](docs/router.md) |
| **[运行时架构设计](docs/zh-CN/architecture.md)** | 双事件循环隔离、信用窗口流控、故障边界与契约静态检查规则 | [Runtime Architecture](docs/architecture.md) |
| **[协议规范 (`meshcall/1`)](docs/zh-CN/protocol.md)** | 逻辑帧格式、握手流程、三层超时计算与标准错误码 | [Protocol Specification](docs/protocol.md) |
| **[贡献指南](CONTRIBUTING.zh-CN.md)** | 本地开发检查（`scripts/check.sh`）、一致性测试与发布流程 | [Contributing](CONTRIBUTING.md) |

---

## 一句话设计哲学

MeshCall 将普通强类型异步函数的极简开发体验延伸至 Python 与 TypeScript 的跨语言流式通信，并将所有分布式路由复杂度收敛于无状态的 Go Router 边界之内。

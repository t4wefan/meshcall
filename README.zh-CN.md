<p align="center">
  <img src="docs/assets/wordmark.svg" alt="MeshCall — Typed Streaming RPC for Python, TypeScript & Go" width="300">
</p>

<p align="center">
  <a href="./README.md">English</a> | <strong>简体中文</strong>
</p>

# MeshCall

**用写普通 Python / TypeScript / Go 函数的方式，编写跨语言流式 RPC。**

在构建跨进程或跨语言应用（例如 Python 算法/AI 后端与 TypeScript 业务应用协同）时，你不必在繁重的 `.proto` 编译工具链与脆弱的手写 WebSocket / JSON 胶水代码之间妥协。

MeshCall 是一个面向 **Python**、**TypeScript** 与 **Go** 的异步流式 RPC 框架——**写远程服务就像写普通异步函数，调远程接口就像调本地方法：**

- **代码即契约，告别手写协议与裸传字典** —— 直接用函数参数声明输入、用返回模型声明输出，无需维护 `.proto` 文件；生成的客户端提供完整的类型签名与 IDE 自动补全，调用方无需手拼字典或翻阅服务端源码查字段
- **接口变更在静态检查阶段暴露，运行时双向校验** —— 一条命令即可从 Python 服务生成 TypeScript 客户端（反之亦然）。服务端修改参数或字段后，调用方在静态检查与编译阶段即可发现不匹配；运行时还会自动校验入参和返回值，非法数据在进入业务函数前即被拦截
- **像写循环一样写流式通信** —— 服务端流只需 `yield`，客户端消费只需 `async for`，天然契合进度推送、实时数据流与 AI 流式输出
- **专注业务逻辑** —— 框架负责连接管理、请求路由、超时取消和流量控制，业务函数无需关心底层网络细节
- **需要时即可观测** —— 可选的 OpenTelemetry 拦截器（Python `meshcall[otel]`、TypeScript `meshcall/otel`）跨语言、穿过 Router 传递 W3C trace context；Router 通过 `--metrics-listen` 提供 Prometheus 指标
- **对人类与 AI 同样透明** —— 在线服务自带接口描述，无需翻阅源码或编写临时脚本，在终端即可直接查看签名并发起调试调用

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

安装包已内置路由进程（`Router`）与 `meshcall` 命令行工具，开箱即用，无需额外安装其他组件。

---

## 快速开始

下面通过 4 个步骤，走完从**定义服务**到**发起调用**的完整流程，并依次认识 MeshCall 中的核心概念。

### 1. 定义“服务”与“方法”

新建 `service.py`。你会发现代码中没有任何网络传输或连接管理样板：

```python
from pydantic import BaseModel
from meshcall import query, service


class Reply(BaseModel):
    message: str


@service("demo.v1.Greeter")
class Greeter:
    """问候服务。"""

    @query
    async def greet(self, name: str) -> Reply:
        """向指定名字返回问候语。"""
        return Reply(message=f"hello, {name}")
```

这里出现了 MeshCall 最基础的三个概念：

- **服务（`@service("demo.v1.Greeter")`）**：将一组相关的远程能力组织在同一个服务名下（`v1` 为版本标识），作为注册和生成客户端的基本单元。
- **方法（`@query` / `@mutates` / `@stream`）**：服务中对外暴露的可调用函数。`@query` 声明纯读取（只读且幂等），`@mutates` 声明会修改状态，`@stream` 声明流式方法。
- **服务契约（Contract）**：方法的参数（`name: str`）与返回模型（`Reply`）共同描述了请求与响应的数据结构。框架会据此自动提取契约，并在每次调用时自动校验入参和返回值——不合法的数据在进入业务函数前就会被直接拦截。

### 2. 从契约生成客户端代码

在 `service.py` 所在目录执行一条命令，即可从 `Greeter` 类生成带完整类型签名的客户端文件 `greeter_client.py`，把服务契约还原为可直接调用的 Python 方法与模型类：

```bash
uv run meshcall generate service:Greeter --single-file --output greeter_client.py
```

这样调用方无需手写方法名字符串或拼装字典，在编辑器中即可获得完整的方法与字段自动补全，并由类型检查器对参数和返回值做静态检查。

### 3. 通过“Router”启动并注册服务

服务端与客户端如何找到彼此？在 MeshCall 中，我们推荐所有节点统一通过 **Router（路由中转进程）** 通信，而不是让客户端直接连接各个服务端的端口：

- **服务端**连向 Router 并注册自己的**服务实例**（`instance_id`）；同一个服务可以启动多个实例，由 Router 自动负责请求转发与负载均衡。
- **客户端**连向同一个 Router 发起调用，只需知道服务名（如 `demo.v1.Greeter`），无需关心目标服务运行在哪个端口或哪台机器上。
- 因为所有节点都连向 Router，**任何一个节点都可以同时对外提供服务，并作为客户端调用其他服务**。

新建 `server.py` 并运行：

```python
from meshcall import run
from service import Greeter


if __name__ == "__main__":
    run(Greeter())
```

```bash
uv run python server.py
```

`run` 会解析 Router 地址（`MESHCALL_ROUTER_URL`，默认为 `ws://127.0.0.1:8765`）；当地址是本机且没有 Router 在监听时，自动拉起内置 Router，注册服务，并在收到 `SIGINT` / `SIGTERM` 时先排空再退出。需要鉴权、指标、多个 Router 端点，或想关闭自动拉起（`spawn_if_local=False`）时，传入显式的 `router=Router(...)`。

### 4. 像调本地函数一样调用服务

保持 `server.py` 运行，新建 `client.py` 并在另一个终端执行：

```python
import asyncio
from greeter_client import GreeterClient


async def main() -> None:
    async with GreeterClient.connect() as client:
        reply = await client.greet("Alice")
        print(reply.message)  # hello, Alice

        # 单次调用选项不进入业务方法签名，统一通过 with_options 设置
        again = await client.with_options(timeout=5.0).greet("Bob")
        print(again.message)


if __name__ == "__main__":
    asyncio.run(main())
```

`connect()` 解析 Router 的方式与 `run()` 完全一致。多个客户端可以复用一条连接：`router = await Router.connect(); GreeterClient(router); BillingClient(router)`。

```bash
uv run python client.py
```

`client.greet` 的入参与返回值均具备完整类型签名，支持 IDE 自动补全与静态类型检查，参数缺失或类型不匹配会在编辑阶段直接暴露。

---

## 流式方法：把 `return` 换成 `yield`

除了一问一答的普通方法，MeshCall 还原生支持**流式方法**。服务端流就是被 `@stream` 装饰的异步生成器；客户端流把入站 `AsyncIterator` 作为最后一个参数，用 `@mutates` 声明（上传流同样是有副作用的调用）。两者都遵循 1:1 签名规则：生成的客户端接收相同的业务参数、顺序一致。对于需要在断线后继续的事件与通知，`@method.subscription(cursor=…, resume=…)` 声明一个服务端流，客户端会自动从最后收到的条目之后恢复；`Topic[T]` 把事件分发给进程内的每个订阅者。订阅与 `Topic` 在 v0.3 中是**实验特性**（见[订阅](docs/zh-CN/protocol.md#订阅subscriptions)）。

修改 `service.py`，新增流式方法 `countdown`：

```python
from collections.abc import AsyncIterator
from pydantic import BaseModel
from meshcall import query, stream, service


class Reply(BaseModel):
    message: str


class Tick(BaseModel):
    step: int


@service("demo.v1.Greeter")
class Greeter:
    """问候与倒计时服务。"""

    @query
    async def greet(self, name: str) -> Reply:
        """向指定名字返回问候语。"""
        return Reply(message=f"hello, {name}")

    @stream
    async def countdown(self, start: int) -> AsyncIterator[Tick]:
        """从 start 倒数到 1 并逐项返回。"""
        for n in range(start, 0, -1):
            yield Tick(step=n)
```

重新生成客户端并重启 `server.py` 后，调用方即可直接用 `async for` 逐项接收 `Tick` 结果：

```bash
uv run meshcall generate service:Greeter --single-file --output greeter_client.py
```

```python
async with GreeterClient.connect() as client:
    async for tick in client.countdown(3):
        print(tick.step)  # 3, 2, 1
```

`countdown` 逐项返回结果，调用方可以边接收边处理；框架会根据消费速度自动控制发送速度，避免慢消费者导致内存堆积。

---

## 跨语言协同：从 TypeScript 直接调用 Python 服务

在“Python 写算法/AI 逻辑 + TypeScript 写业务/全栈应用”的项目中，最容易出问题的地方往往是：**两边各自手写一套数据结构，Python 改了字段名，TypeScript 代码却没同步，直到线上运行才报错。**

通过 `meshcall generate`，同一个 `service.py` 可以直接生成原生的 TypeScript 客户端包。一旦 Python 服务修改了接口，重新生成后 TypeScript 编译器就会直接标红所有不匹配的调用点。

1. 从 `service.py` 中的 `Greeter` 类生成 TypeScript 客户端目录 `./greeter-ts`（默认包名为 `meshcall-demo-v1-greeter-client`）：

```bash
uv run meshcall generate service:Greeter --language typescript --output ./greeter-ts
```

2. 构建生成的本地包，并安装到你的 TypeScript 项目中：

```bash
yarn --cwd ./greeter-ts install && yarn --cwd ./greeter-ts build
yarn add file:./greeter-ts
```

3. 保持前面的 Python `server.py` 运行，在 TypeScript 中发起调用：

```typescript
import { Router } from "meshcall";
import { GreeterClient } from "meshcall-demo-v1-greeter-client";

const router = await Router.connect("ws://127.0.0.1:8765");
const client = await GreeterClient.connect(router);
try {
  const reply = await client.greet("Alice");
  console.log(reply.message); // hello, Alice

  for await (const tick of client.countdown(3)) {
    console.log(tick.step); // 3, 2, 1
  }
} finally {
  await router.close();
}
```

`Router.connect` 会读取 `MESHCALL_ROUTER_URL`（默认为 `ws://127.0.0.1:8765`）；`withOptions({ timeoutMs, metadata, idempotencyKey, instance })` 用于设置单次调用选项，不改变业务方法签名。

---

## 依赖注入与元数据

处理器可以声明只在服务端解析的依赖，而不把它们加进契约。依赖按调用解析（同一次调用内缓存），带 `yield` 的提供者在调用结束时执行清理——成功时提交，出错或被取消时回滚。`meta("x-caller-id")` 是给"以调用元数据而非业务字段到达的值"留的逃生舱：

```python
from typing import Annotated
from meshcall import dependency, meta, query

@dependency
async def current_user(authorization: Annotated[str, meta("authorization")]) -> str:
    return authorization.removeprefix("Bearer ")

@query
async def whoami(self, user: str = current_user) -> Reply:
    return Reply(message=f"hello, {user}")
```

`user` 和 `authorization` 都不会出现在契约或生成的客户端里。[`best-practice/`](best-practice/) 用会话存储展示了事务式依赖，并附带 TypeScript CLI 和 Go 客户端。

---

## 告别黑盒：在终端中查看与调用服务

由于每个服务在注册到 Router 时都会上报自己的服务契约，排查问题或联调接口时，你不需要翻找源码或专门编写测试脚本。

保持 `server.py` 运行，在另一个终端执行以下命令（默认连接 `ws://127.0.0.1:8765`）：

```bash
# 查看 Router 上已注册的服务、在线实例与方法签名
uv run meshcall router inspect

# 直接用 JSON 调用普通方法或流式方法
uv run meshcall call demo.v1.Greeter.greet '{"name": "Alice"}'
uv run meshcall call demo.v1.Greeter.countdown '{"start": 3}'
```

如果传错了字段名或类型，`meshcall call` 会自动对照服务契约给出精确到字段路径的修复提示。

### 普通 HTTP 调用

Router 在同一端口上也接受 HTTP 调用，任何能发 HTTP 请求的程序都无需 MeshCall 客户端即可调用你的服务：

```bash
# 一元调用：JSON 进，JSON 出
curl http://127.0.0.1:8765/v1/call/demo.v1.Greeter/greet -d '{"name": "Alice"}'

# 服务端流：每个条目一行 {"item": …}，最后一行 {"end": …}
curl -N http://127.0.0.1:8765/v1/call/demo.v1.Greeter/countdown -d '{"start": 3}'

# 发现服务：Registry，或供 Swagger UI 与客户端生成器使用的 OpenAPI 3.1 文档
curl http://127.0.0.1:8765/v1/services
curl http://127.0.0.1:8765/v1/openapi.json
```

在浏览器（或 Deno、Bun、边缘运行时）中，使用 TypeScript 包的 `meshcall/web` 入口：`new ChatClient(new MeshCallHttpClient("https://router.example", { token }))` 让生成的客户端通过 `fetch` 获得同样的类型化调用、错误、本地校验与订阅恢复（跨域时 Router 需要以 `--http-cors-origin https://app.example` 启动）。

一元调用与服务端流完整可用，客户端流使用 NDJSON 请求体，duplex 调用（作为最后手段的受限形状，见[双向流调用](docs/zh-CN/protocol.md#双向流调用duplex-calls)）只能走 WebSocket。账号、令牌、超时（`MeshCall-Timeout-Ms`）、幂等键（`Idempotency-Key`）与元数据（`MeshCall-Meta-*`）都通过请求头传递，详见 [HTTP 调用](docs/zh-CN/protocol.md#http-调用http-calls)。

---

## Go

Go 服务与客户端使用 Go 运行时 [`meshcall-go`](meshcall-go/README.md)：同样的调用、错误、超时、流、订阅与 Router 特性，并通过同一套一致性测试（conformance suite）验证。`meshcall generate service.meshcall.json --language go --output internal/chatclient` 可为任一语言编写的服务生成类型化的 Go 客户端，`meshcall.WriteContract` 则导出 Go 服务的契约，供 Python 与 TypeScript 客户端使用。

---

## 让 AI 编程助手看懂你的服务

如果你使用 AI 编程助手协助开发，在项目根目录执行一次 `meshcall init`：

```bash
uv run meshcall init
```

这会安装供 AI 助手参考的开发说明（`.agents/skills/meshcall/SKILL.md` 并更新 `AGENTS.md`），让 AI 了解如何定义服务与方法、生成客户端，以及通过 `meshcall router inspect` 和 `meshcall call` 探索与验证在线接口。针对 Claude Code、Cursor 等特定工具的配置方式详见[快速上手与实战指南](docs/zh-CN/guide.md)。

---

## 运行仓库示例

克隆本仓库后，可以直接运行 `demo/` 下的独立示例项目，体验不同场景下的完整用法：

```bash
git clone https://github.com/t4wefan/meshcall.git
cd meshcall

uv run --project demo/quickstart demo   # 最小服务、Router 与客户端
uv run --project demo/showcase demo     # @query / @mutates / @stream、元数据、注册表
uv run --project demo/router demo       # 独立 Router 进程与注册表
uv run --project demo/py2ts demo        # Python 服务 -> TypeScript 客户端
uv run --project demo/ts2py demo        # TypeScript 服务 -> Python 客户端
uv run --project best-practice demo     # 依赖注入、TypeScript CLI 与 Go 客户端
```

---

## 文档导航

当你的项目进入更复杂的生产场景时，可以按需查阅以下指南：

| 文档 | 适用场景 | English |
| --- | --- | --- |
| **[快速上手与实战指南](docs/zh-CN/guide.md)** | 处理流式调用、超时与重试、上下文与日志注入、负载均衡及鉴权配置 | **[User Guide & Cookbook](docs/guide.md)** |
| **[Python SDK 指南](meshcall-py/README.zh-CN.md)** | 查阅 Python 包安装与 API 用法 | [Python SDK](meshcall-py/README.md) |
| **[TypeScript SDK 指南](meshcall-ts/README.zh-CN.md)** | 查阅 TypeScript 包安装与 API 用法 | [TypeScript SDK](meshcall-ts/README.md) |
| **Go SDK**（暂无中文版） | 使用 Go 运行时与生成的 Go 客户端 | [Go SDK](meshcall-go/README.md) |
| **[Router 与 CLI 部署指南](meshcall-router/README.zh-CN.md)** | 独立部署 Router 进程、配置命令行参数与 Docker 运行 | [Router & CLI](meshcall-router/README.md) |
| **[Router 架构与鉴权](docs/zh-CN/router.md)** | 配置多实例路由、账号权限与访问令牌 | [Router & Auth Guide](docs/router.md) |
| **[运行时架构设计](docs/zh-CN/architecture.md)** | 了解客户端与服务端运行时的内部设计 | [Runtime Architecture](docs/architecture.md) |
| **[协议规范 (`meshcall/1`)](docs/zh-CN/protocol.md)** | 了解底层通信协议、超时计算与错误码定义 | [Protocol Specification](docs/protocol.md) |
| **[贡献指南](CONTRIBUTING.zh-CN.md)** | 参与项目开发、运行测试与发布流程 | [Contributing](CONTRIBUTING.md) |

---

## 设计哲学

**Focus on your procedure.**

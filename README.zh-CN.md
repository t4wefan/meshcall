<p align="center">
  <img src="docs/assets/wordmark.svg" alt="MeshCall — Typed Streaming RPC for Python & TypeScript" width="300">
</p>

<p align="center">
  <a href="./README.md">English</a> | <strong>简体中文</strong>
</p>

# MeshCall

**用写普通 Python / TypeScript 异步函数的方式，编写跨语言流式 RPC。**

MeshCall 是一个面向 **Python** 与 **TypeScript** 的异步强类型流式 RPC 框架。

**Router** 是服务之间的中转进程。服务端向它注册服务，调用方通过它找到并调用服务；因为所有节点都通过 Router 通信，任何一个节点都可以同时对外提供服务并作为客户端调用其他服务。安装包已附带 Router，无需额外安装其他组件。

- **用普通函数声明服务** —— 函数参数即请求结构，返回值类型即响应结构，自动完成类型校验
- **原生流式结果** —— 服务端流只需 `yield`，客户端流只需 `async for`
- **跨语言生成强类型客户端** —— 一条命令即可从 Python 服务生成 TypeScript 客户端（反之亦然），无需手写 `.proto` 文件
- **专注业务逻辑** —— 框架负责连接管理、请求转发、超时取消和流量控制，服务函数只需处理业务
- **终端可直接查看与调用** —— 任何运行中的服务都自带接口描述，可在命令行直接查看签名并发起调用

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

---

## 快速开始

### 1. 定义服务

新建 `service.py`：

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

`demo.v1.Greeter` 是调用方使用的服务名，包含版本标识。`read_only=True` 声明这个方法不修改业务状态。函数参数和返回模型共同描述请求与响应的结构，本文称它为**服务契约**。

### 2. 生成强类型客户端

在 `service.py` 所在目录执行以下命令，从 `service.py` 中的 `Greeter` 类生成客户端文件 `greeter_client.py`：

```bash
uv run meshcall generate service:Greeter --single-file --output greeter_client.py
```

### 3. 启动服务端

新建 `server.py` 并运行，启动内置 Router 与 `Greeter` 服务：

```python
import asyncio
from meshcall import RpcServer, WebSocketRouter
from meshcall.drivers import WebSocketRouterServerDriver
from service import Greeter


async def main() -> None:
    async with WebSocketRouter(host="127.0.0.1", port=8765):
        server = RpcServer(
            services=[Greeter()],
            driver=WebSocketRouterServerDriver("ws://127.0.0.1:8765", instance_id="greeter-1"),
        )
        await server.start()
        try:
            await asyncio.Event().wait()
        finally:
            await server.stop(drain=True)


if __name__ == "__main__":
    asyncio.run(main())
```

```bash
uv run python server.py
```

其中 `instance_id="greeter-1"` 是这次运行的服务实例名称；`drain=True` 表示等待正在处理的请求完成后再停止服务。

### 4. 调用服务

保持 `server.py` 运行，新建 `client.py` 并在另一个终端执行：

```python
import asyncio
from meshcall.drivers import WebSocketClientDriver
from greeter_client import GreeterClient


async def main() -> None:
    async with GreeterClient(WebSocketClientDriver("ws://127.0.0.1:8765")) as client:
        reply = await client.greet("Alice")
        print(reply.message)  # hello, Alice


if __name__ == "__main__":
    asyncio.run(main())
```

```bash
uv run python client.py
```

---

## 流式返回多个结果

当一个方法需要持续返回多条结果（例如进度更新、日志流或分段生成内容）时，使用 `@method.server_stream` 装饰异步生成器。

修改 `service.py`，新增 `countdown` 方法：

```python
from collections.abc import AsyncIterator
from pydantic import BaseModel
from meshcall import method, service


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
        """从 start 倒数到 1 并逐项返回。"""
        for n in range(start, 0, -1):
            yield Tick(step=n)
```

重新生成客户端并重启 `server.py` 后，即可在 `client.py` 中用 `async for` 接收流式结果：

```bash
uv run meshcall generate service:Greeter --single-file --output greeter_client.py
```

```python
async with GreeterClient(WebSocketClientDriver("ws://127.0.0.1:8765")) as client:
    async for tick in client.countdown(3):
        print(tick.step)  # 3, 2, 1
```

`countdown` 逐项返回结果，调用方可以边接收边处理；框架会根据消费速度控制发送速度。

---

## 从 TypeScript 调用

同一个 `service.py` 可以直接生成供 TypeScript 项目使用的客户端包。

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
import { MeshCallClient } from "meshcall";
import { GreeterClient } from "meshcall-demo-v1-greeter-client";

const rpc = new MeshCallClient("ws://127.0.0.1:8765");
const client = new GreeterClient(rpc);
try {
  const reply = await client.greet("Alice");
  console.log(reply.message); // hello, Alice

  for await (const tick of client.countdown(3)) {
    console.log(tick.step); // 3, 2, 1
  }
} finally {
  await rpc.close();
}
```

---

## 在终端中查看与调用服务

保持 `server.py` 运行，在另一个终端执行以下命令（默认连接 `ws://127.0.0.1:8765`）：

```bash
# 查看已注册的服务、在线实例与方法签名
uv run meshcall router inspect

# 直接用 JSON 调用普通方法或流式方法
uv run meshcall call demo.v1.Greeter.greet '{"name": "Alice"}'
uv run meshcall call demo.v1.Greeter.countdown '{"start": 3}'
```

如果传错了字段名或类型，`meshcall call` 会根据服务契约输出具体的字段修复提示。

---

## AI 编程助手集成

如果你使用 AI 编程助手，可以在项目根目录执行一次 `meshcall init`，安装供 AI 助手参考的开发说明：

```bash
uv run meshcall init
```

这会写入 `.agents/skills/meshcall/SKILL.md` 并更新 `AGENTS.md`，帮助 AI 助手了解如何定义服务、生成客户端以及使用命令行调试接口。针对特定工具（如 Claude Code、Cursor 等）的配置参数详见[快速上手与实战指南](docs/zh-CN/guide.md)。

---

## 运行仓库示例

克隆本仓库后，可以直接运行 `demo/` 下的完整示例项目：

```bash
git clone https://github.com/t4wefan/meshcall.git
cd meshcall

uv run --project demo/quickstart demo   # Python 最小调用示例
uv run --project demo/showcase demo     # Python 完整特性示例
uv run --project demo/router demo       # 多实例负载均衡与路由示例
uv run --project demo/py2ts demo        # Python 服务 -> TypeScript 客户端调用
uv run --project demo/ts2py demo        # TypeScript 服务 -> Python 客户端调用
```

---

## 文档导航

| 文档 | 适用场景 | English |
| --- | --- | --- |
| **[快速上手与实战指南](docs/zh-CN/guide.md)** | 处理流式调用、超时与重试、上下文与日志注入、负载均衡及鉴权配置 | **[User Guide & Cookbook](docs/guide.md)** |
| **[Python SDK 指南](meshcall-py/README.zh-CN.md)** | 查阅 Python 包安装与 API 用法 | [Python SDK](meshcall-py/README.md) |
| **[TypeScript SDK 指南](meshcall-ts/README.zh-CN.md)** | 查阅 TypeScript 包安装与 API 用法 | [TypeScript SDK](meshcall-ts/README.md) |
| **[Router 与 CLI 部署指南](meshcall-router/README.zh-CN.md)** | 独立部署 Router 进程、配置命令行参数与 Docker 运行 | [Router & CLI](meshcall-router/README.md) |
| **[Router 架构与鉴权](docs/zh-CN/router.md)** | 配置多实例路由、账号权限与访问令牌 | [Router & Auth Guide](docs/router.md) |
| **[运行时架构设计](docs/zh-CN/architecture.md)** | 了解客户端与服务端运行时的内部设计 | [Runtime Architecture](docs/architecture.md) |
| **[协议规范 (`meshcall/1`)](docs/zh-CN/protocol.md)** | 了解底层通信协议、超时计算与错误码定义 | [Protocol Specification](docs/protocol.md) |
| **[贡献指南](CONTRIBUTING.zh-CN.md)** | 参与项目开发、运行测试与发布流程 | [Contributing](CONTRIBUTING.md) |

<p align="center">
  <img src="docs/assets/wordmark.svg" alt="MeshCall" width="300">
</p>

<p align="center">
  <a href="./README.md">English</a> | <strong>简体中文</strong>
</p>

# MeshCall

**面向 Python 与 Node.js / TypeScript 服务之间的类型化调用与流式通信框架。**

MeshCall 让你用普通异步函数定义远程服务，并在跨进程或跨语言调用时自动完成参数与返回值的类型校验。

- **普通函数即服务** —— 函数参数与返回模型自动描述请求和响应结构
- **支持流式结果** —— 服务端用 `yield` 逐项返回数据，调用方用 `async for` 或 `for await` 边接收边处理
- **生成强类型客户端** —— 一条命令生成 Python 或 TypeScript 客户端，无需手写 `.proto` 文件
- **框架托管通信细节** —— 框架负责连接管理、请求转发、超时、取消和流量控制，服务函数只需处理业务
- **支持终端直接调试** —— 随时在命令行查看已注册服务并发起调用

---

## 组件如何连接

MeshCall 使用 **Router** 作为服务之间的中转进程：

```text
调用方 (Client)  --->  Router (ws://127.0.0.1:8765)  <---  服务端 (Server)
```

- **服务端**连接到 Router 并注册自己提供的服务与方法。
- **调用方**连接到同一个 Router，按服务名和方法名发起调用。
- 由于所有参与者都连接到 Router，**我们推荐无论何时都通过 Router 通信**：同一个 Python 或 Node.js 进程可以同时注册自身服务，并作为客户端调用其他服务，无需为每个服务单独分配端口或维护节点地址。
- 安装包已附带预编译的 `meshcall-router` 与 `meshcall` CLI，无需另行安装 Go。

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

### 1. 定义并启动服务

将以下代码保存为 `server.py`：

```python
import asyncio
from pydantic import BaseModel
from meshcall import RpcServer, WebSocketRouter, method, service
from meshcall.drivers import WebSocketRouterServerDriver


class Reply(BaseModel):
    message: str


@service("demo.v1.Greeter")
class Greeter:
    """问候服务。"""

    @method(read_only=True)
    async def greet(self, name: str) -> Reply:
        """向指定名字返回问候语。"""
        return Reply(message=f"hello, {name}")


async def main() -> None:
    async with WebSocketRouter(host="127.0.0.1", port=8765):
        server = RpcServer(
            services=[Greeter()],
            driver=WebSocketRouterServerDriver(
                "ws://127.0.0.1:8765",
                instance_id="greeter-1",
            ),
        )
        await server.start()
        print("Router and Greeter running at ws://127.0.0.1:8765")
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
```

代码说明：

- `demo.v1.Greeter` 是调用方使用的服务名，包含版本标识（`v1`）。
- `read_only=True` 声明这个方法不修改业务状态。
- 函数参数（`name: str`）和返回模型（`Reply`）共同描述请求与响应的结构，本文称它为**服务契约**。
- `WebSocketRouter` 在本机启动 Router（监听 `ws://127.0.0.1:8765`），`RpcServer` 将当前运行的服务实例（实例名 `instance_id="greeter-1"`）注册到 Router。

在第一个终端启动服务：

```bash
uv run python server.py
```

### 2. 生成强类型客户端

保持 `server.py` 运行，打开第二个终端，根据 `server.py` 中的 `Greeter` 类（格式为 `模块名:类名`）生成单文件客户端：

```bash
uv run meshcall generate server:Greeter --single-file --output greeter_client.py
```

### 3. 发起调用

在同一目录下新建 `client.py`：

```python
import asyncio
from meshcall.drivers import WebSocketClientDriver
from greeter_client import GreeterClient


async def main() -> None:
    driver = WebSocketClientDriver("ws://127.0.0.1:8765")
    async with GreeterClient(driver) as client:
        reply = await client.greet("Alice")
        print(reply.message)


if __name__ == "__main__":
    asyncio.run(main())
```

在第二个终端运行客户端：

```bash
uv run python client.py
# 输出: hello, Alice
```

---

## 增加一个流式方法

如果需要分批返回多条结果，可以在 `server.py` 的 `Greeter` 中增加一个 `@method.server_stream` 生成器方法：

```python
from collections.abc import AsyncIterator


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
        """从 start 倒数到 1 并依次返回。"""
        for n in range(start, 0, -1):
            yield Tick(step=n)
```

`countdown` 逐项返回结果，调用方可以边接收边处理；框架会根据消费速度自动控制发送速度。

重启 `server.py` 并重新执行上一步的生成命令后，即可在 `client.py` 中遍历流式结果：

```python
async for tick in client.countdown(3):
    print(tick.step)  # 依次输出: 3, 2, 1
```

### 从 TypeScript 调用同一个服务

同样可以从 `server.py` 生成 TypeScript 客户端包：

```bash
# 1. 在 ./greeter-ts 目录生成 TypeScript 客户端包
uv run meshcall generate server:Greeter --language typescript --output ./greeter-ts

# 2. 构建生成的本地包并安装到你的 TypeScript 项目中
yarn --cwd ./greeter-ts install
yarn --cwd ./greeter-ts build
yarn add ./greeter-ts
```

随后在 TypeScript 中调用正在运行的 Python 服务：

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

## 终端调试与 AI 开发支持

### 在终端查看与调用服务

保持 `server.py` 运行，在另一个终端执行以下命令（默认连接 `ws://127.0.0.1:8765`，也可通过 `--router ws://host:port` 指定）：

```bash
# 查看已注册的服务、在线实例和方法签名
uv run meshcall router inspect

# 使用 JSON 直接调用方法
uv run meshcall call demo.v1.Greeter.greet '{"name": "Alice"}'
uv run meshcall call demo.v1.Greeter.countdown '{"start": 3}'
```

如果传入的字段名或类型有误，`meshcall call` 会对照服务契约指出具体错误字段与期望格式。

### 安装 AI 编程助手开发说明

在项目根目录运行以下命令，可安装供 AI 编程助手参考的开发说明：

```bash
uv run meshcall init
```

该命令会在项目中生成 `.agents/skills/meshcall/SKILL.md` 并更新 `AGENTS.md`，帮助 AI 助手了解如何使用命令行探测服务、编写服务契约和生成客户端。如需为特定工具生成配置文件（`--agent claude|cursor|copilot|windsurf|all`），请参阅[快速上手与实战指南](docs/zh-CN/guide.md)。

---

## 运行仓库示例项目

如需运行仓库自带的示例项目，请先克隆本仓库：

```bash
git clone https://github.com/t4wefan/meshcall.git
cd meshcall

uv run --project demo/quickstart demo   # Python 基础调用与流式调用示例
uv run --project demo/showcase demo     # Python 完整特性示例
uv run --project demo/router demo       # 多实例路由与负载均衡示例
uv run --project demo/py2ts demo        # Python 服务 -> TypeScript 客户端调用
uv run --project demo/ts2py demo        # TypeScript 服务 -> Python 客户端调用
```

---

## 文档导航

| 使用目的 | 文档入口 | English |
| --- | --- | --- |
| **学习常用开发模式**（上下文与日志、客户端流、超时与重试、负载均衡、鉴权） | **[快速上手与实战指南](docs/zh-CN/guide.md)** | **[User Guide & Cookbook](docs/guide.md)** |
| **查阅 Python SDK 用法** | **[Python SDK 指南](meshcall-py/README.zh-CN.md)** | [Python SDK](meshcall-py/README.md) |
| **查阅 TypeScript SDK 用法** | **[TypeScript SDK 指南](meshcall-ts/README.zh-CN.md)** | [TypeScript SDK](meshcall-ts/README.md) |
| **部署 Router 与配置权限** | **[Go Router 部署指南](meshcall-router/README.zh-CN.md)** / **[Router 架构与鉴权](docs/zh-CN/router.md)** | [Go Router & CLI](meshcall-router/README.md) / [Router Guide](docs/router.md) |
| **了解运行时设计与底层协议** | **[运行时架构设计](docs/zh-CN/architecture.md)** / **[协议规范](docs/zh-CN/protocol.md)** | [Runtime Architecture](docs/architecture.md) / [Protocol Spec](docs/protocol.md) |
| **参与项目开发与测试** | **[贡献指南](CONTRIBUTING.zh-CN.md)** | [Contributing](CONTRIBUTING.md) |

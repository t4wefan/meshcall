<p align="center">
  <img src="docs/assets/wordmark.svg" alt="MeshCall — Typed Streaming RPC for Python & TypeScript" width="300">
</p>

<p align="center">
  <a href="./README.md">English</a> | <strong>简体中文</strong>
</p>

# MeshCall

**用写普通 Python / TypeScript 异步函数的方式，编写跨语言流式 RPC。**

在构建跨语言服务（例如 Python 后端/AI 服务与 TypeScript 应用协同）时，你不必在繁重的 `.proto` 编译工具链与脆弱的手写 WebSocket 胶水代码之间妥协。

MeshCall 是一个面向 **Python** 与 **TypeScript** 的异步强类型流式 RPC 框架——**写服务端就像写普通异步函数，调远程接口就像调本地方法。**

- **代码即契约，零样板负担** —— 直接用函数参数声明请求、用返回模型声明响应，无需手写 `.proto` 或消息序列化逻辑
- **像写循环一样写流式通信** —— 服务端流只需 `yield`，客户端消费只需 `async for`，天然适用于进度推送、实时数据流与 AI 流式输出
- **跨语言端到端强类型** —— 一条命令即可从 Python 服务生成带完整类型提示的 TypeScript 客户端（反之亦然），字段变更在编译期即可感知
- **中心化 Router，节点天然对等** —— **Router** 是服务之间的中转进程：服务端向它注册服务，调用方通过它找到并调用服务。因为所有节点统一连接至 Router，你无需管理各服务的端口与地址，任何进程都能同时提供服务并调用其他服务
- **对人类与 AI 同样透明** —— 运行中的服务自带接口描述，无需翻阅源码或编写临时脚本，在终端即可直接查看签名并发起调试调用

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

安装包已附带 `Router` 与 `meshcall` 命令行工具，开箱即用，无需额外安装其他组件。

---

## 快速开始

下面用 4 个最小步骤，走完从**定义服务**到**强类型调用**的完整闭环。

### 1. 像写普通类一样定义服务

新建 `service.py`。你会发现代码中没有任何网络传输、连接管理或序列化样板：

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

`demo.v1.Greeter` 是调用方使用的服务名（含版本标识），`read_only=True` 声明该方法不修改业务状态。函数参数（`name: str`）与返回模型（`Reply`）共同构成了**服务契约**，框架会据此自动完成入参和返回值的类型校验。

### 2. 一条命令生成强类型客户端

无需手写字符串方法名或拼装字典，在 `service.py` 所在目录执行以下命令，即可从 `Greeter` 类生成自带类型提示的客户端 `greeter_client.py`：

```bash
uv run meshcall generate service:Greeter --single-file --output greeter_client.py
```

### 3. 启动 Router 与服务端

新建 `server.py` 并运行，一行上下文即可拉起内置 Router 并注册服务：

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

其中 `instance_id="greeter-1"` 是当前运行实例的名称；`drain=True` 确保在停止服务前等待正在处理的请求优雅完成。

### 4. 像调本地函数一样发起远程调用

保持 `server.py` 运行，新建 `client.py` 并在另一个终端执行——你将获得完整的 IDE 参数补全与返回类型推导：

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

## 流式返回多个结果：把 `return` 换成 `yield`

当接口需要源源不断地返回多条结果（例如 AI Token 流式生成、实时任务进度或日志流）时，你不需要引入复杂的回调或手写状态机——只需用 `@method.server_stream` 装饰一个异步生成器。

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

重新生成客户端并重启 `server.py` 后，调用方即可直接用 `async for` 逐项消费：

```bash
uv run meshcall generate service:Greeter --single-file --output greeter_client.py
```

```python
async with GreeterClient(WebSocketClientDriver("ws://127.0.0.1:8765")) as client:
    async for tick in client.countdown(3):
        print(tick.step)  # 3, 2, 1
```

`countdown` 逐项产出结果，调用方边接收边处理；框架会根据消费速度自动控制发送节奏，避免慢消费者导致内存堆积。

---

## 跨语言协同：从 TypeScript 直接调用 Python 服务

在“Python 写算法/AI 逻辑 + TypeScript 写业务/全栈应用”的架构中，你无需维护两套数据结构定义。同一个 `service.py` 可以直接生成原生的 TypeScript 客户端包。

1. 从 `service.py` 中的 `Greeter` 类生成 TypeScript 客户端目录 `./greeter-ts`（默认包名为 `meshcall-demo-v1-greeter-client`）：

```bash
uv run meshcall generate service:Greeter --language typescript --output ./greeter-ts
```

2. 构建生成的本地包，并安装到你的 TypeScript 项目中：

```bash
yarn --cwd ./greeter-ts install && yarn --cwd ./greeter-ts build
yarn add file:./greeter-ts
```

3. 保持前面的 Python `server.py` 运行，在 TypeScript 中享受同样丝滑的强类型调用与流式迭代：

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

## 告别黑盒：在终端中查看与调试在线服务

排查问题或联调接口时，你不需要翻找源码或专门写一段测试脚本。只要 `server.py` 正在运行，打开另一个终端即可直接查看网格中的所有服务并发起调用（默认连接 `ws://127.0.0.1:8765`）：

```bash
# 查看当前在线的服务、实例健康状态与方法签名
uv run meshcall router inspect

# 直接用 JSON 调用普通方法或流式方法
uv run meshcall call demo.v1.Greeter.greet '{"name": "Alice"}'
uv run meshcall call demo.v1.Greeter.countdown '{"start": 3}'
```

如果传错了字段名或类型，`meshcall call` 会自动对照服务契约给出精确到字段路径的修复提示。

---

## 让 AI 编程助手看懂你的服务

如果你使用 AI 编程助手协助开发，在项目根目录执行一次 `meshcall init`：

```bash
uv run meshcall init
```

这会安装供 AI 助手参考的开发说明（`.agents/skills/meshcall/SKILL.md` 并更新 `AGENTS.md`），让 AI 知道如何规范地定义服务、生成强类型客户端，以及自主通过 `meshcall router inspect` 和 `meshcall call` 探索与验证在线接口。针对 Claude Code、Cursor 等特定工具的配置方式详见[快速上手与实战指南](docs/zh-CN/guide.md)。

---

## 运行仓库示例

克隆本仓库后，可以直接运行 `demo/` 下的独立示例项目，体验不同场景下的完整用法：

```bash
git clone https://github.com/t4wefan/meshcall.git
cd meshcall

uv run --project demo/quickstart demo   # Python 最小调用示例
uv run --project demo/showcase demo     # Python 完整特性演练
uv run --project demo/router demo       # 多实例负载均衡与路由示例
uv run --project demo/py2ts demo        # Python 服务 -> TypeScript 客户端调用
uv run --project demo/ts2py demo        # TypeScript 服务 -> Python 客户端调用
```

---

## 文档导航

当你的项目进入更复杂的生产场景时，可以按需查阅以下指南：

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

---

## 一句话设计哲学

MeshCall 让跨语言流式 RPC 回归普通强类型异步函数的直觉体验，同时将连接管理、流量控制与服务路由安静地收敛在业务代码之外。

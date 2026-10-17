# meshcall (Python SDK)

[English](./README.md) | **简体中文**

面向 Python、TypeScript 与 Go 的异步强类型流式 RPC 框架，支持 WebSocket 与 HTTP 传输。
服务使用 Pydantic v2 模型定义契约，通过 `Mesh` 注册到独立 Go Router，并暴露一元调用（Unary）、服务端流（Server Stream）、客户端流（Client Stream）与受限双向流（Duplex）方法。

## 安装

```bash
uv add meshcall
# 或：pip install meshcall
```

预编译的 `meshcall-router` 与 `meshcall` CLI 二进制已内置在 Linux（`manylinux` 与 `musllinux`）、macOS 与 Windows 的平台 wheel 包中——无需安装 Go 工具链，也无需额外下载。在回退到纯 Python wheel 的平台上，运行 `uv run meshcall router install` 即可将对应版本的发行二进制下载并校验至用户级缓存。

如果项目中使用 AI 编程助手，只需安装一次官方 Agent 技能：

```bash
uv run meshcall init                  # 默认生成通用标准：.agents/skills/meshcall/SKILL.md + AGENTS.md
uv run meshcall init --agent claude   # 或定向生成：claude | cursor | copilot | windsurf | all
```

## 定义服务

```python
from collections.abc import AsyncIterator

from pydantic import BaseModel

from meshcall import (
    LocalRouter,
    Mesh,
    RpcLogger,
    mutates,
    query,
    service,
    stream,
)


class Message(BaseModel):
    text: str


@service("chat.v1.Chat")
class Chat:
    """聊天会话服务。"""

    @query
    async def last_message(self, session_id: str) -> Message:
        """读取会话的最新消息（只读且幂等）。"""
        return Message(text=f"last message in {session_id}")

    @mutates(idempotent=True)
    async def send(self, session_id: str, text: str, logger: RpcLogger) -> Message:
        """追加一条消息并返回回复（可安全重试的写入）。"""
        logger.info("session={} text={}", session_id, text)  # noqa: PLE1205
        return Message(text=f"echo: {text}")

    @stream
    async def watch(self, session_id: str) -> AsyncIterator[Message]:
        """持续推送会话的回复（服务端流）。"""
        yield Message(text=f"watching {session_id}")

    @mutates(idempotent=True)
    async def ingest(self, session_id: str, items: AsyncIterator[Message]) -> Message:
        """写入一批消息流（客户端流）。"""
        count = 0
        async for _ in items:
            count += 1
        return Message(text=f"ingested {count}")


if __name__ == "__main__":
    mesh = Mesh(LocalRouter(port=8765))
    mesh.mount(Chat())
    mesh.run()
```

服务方法使用以下三个装饰器声明：

- `@query` 声明纯读取（`read_only=True`、`idempotent=True`）。
- `@mutates(...)` 声明会修改状态的一元、客户端流或双向流方法。裸 `@mutates` 默认为 `destructive=True`；可安全重试的写入用 `@mutates(idempotent=True)`，需要显式声明破坏性时用 `@mutates(destructive=True)`。
- `@stream` 声明服务端流。裸 `@stream` 默认按只读处理，除非方法名看起来像变更操作；有副作用的服务端流请显式传 `read_only=False, idempotent=False`。

双向流（一次调用中输入与输出交错）是受限的最后手段：用 `@mutates(reason="…")` 声明，处理函数接收 `DuplexChannel[In, Out]` 参数并返回 `None`。双向流不重试、不去重、不接受幂等键，也不能通过 HTTP 调用。

处理函数也可以声明 `ctx: RpcContext` 与 `logger: RpcLogger` 参数；二者由运行时注入，不会进入契约参数。

在开发与 CI 阶段设置 `MESHCALL_STRICT=1`，可在导入模块时立即发现缺失的文档字符串或未标注副作用的变更方法，并给出精确的修复提示。

## 挂载、运行与托管

在 `Mesh` 上挂载服务并调用阻塞式入口 `mesh.run()`，它会接管进程直到收到 `SIGINT` / `SIGTERM`，随后优雅排空在途调用。使用 `LocalRouter(port=8765)` 可自动拉起并托管本地 `meshcall-router` 子进程，使用 `Router("ws://127.0.0.1:8765")` 则连接到已运行的外部 Router：

```python
from meshcall import LocalRouter, Mesh, Router

# 在 8765 端口启动本地 Router 子进程（若连接外部 Router 则传入 Router()）：
mesh = Mesh(LocalRouter(port=8765))
mesh.mount(Chat())
mesh.run()
```

在异步应用或测试中，配合 `LocalRouter` 与 `mesh.serve()` 托管服务：

```python
import asyncio

from meshcall import LocalRouter, Mesh


async def main() -> None:
    async with LocalRouter(port=0) as router:
        mesh = Mesh(router)
        mesh.mount(Chat())
        async with mesh.serve() as server:
            print(f"serving on {server.url}")  # 例如 ws://127.0.0.1:53281
            await asyncio.Event().wait()  # 离开代码块时会先排空再停止


asyncio.run(main())
```

`mesh.serve(...)` 返回 `RunningServer`（`server.url`、`await server.stop()`、`await server.wait_closed()`），也可以直接 `await mesh.serve(...)`。`mesh.run` 与 `mesh.serve` 都接受 `instance_id`、`max_inflight`、`strict`、`interceptors` 与日志选项。

无参的 `Router()` 会读取 `MESHCALL_ROUTER_URL`（多个 Router 用逗号分隔），未设置时回退到 `ws://127.0.0.1:8765`。需要由 Python 显式启动和停止本地 `meshcall-router` 子进程时，请使用 `LocalRouter(...)`。

## 生成并调用强类型客户端

```bash
# 生成可安装的 Python uv 包（或添加 --single-file 生成单文件 .py 客户端）
uv run meshcall generate my_app.service:Chat --output generated/chat-client

# 从同一个 Python 服务生成 TypeScript 客户端包或 Go 客户端包
uv run meshcall generate my_app.service:Chat --language typescript --output generated/chat-ts
uv run meshcall generate my_app.service:Chat --language go --output internal/chatclient
```

保持服务端运行，在另一个终端通过生成的客户端发起调用：

```python
from meshcall import Mesh, Router
from meshcall_chat_v1_chat_client import ChatClient


async def main() -> None:
    async with Mesh(Router("ws://127.0.0.1:8765")) as mesh:
        chat = ChatClient(mesh)  # 复用 Mesh 的唯一连接
        reply = await chat.with_options(
            timeout=5.0,
            idempotency_key="send-s1-1",
            metadata={"x-caller-id": "cli"},
        ).send("s1", "hello")
        print(reply.text)
```

生成的客户端方法只接收业务参数，顺序与服务端处理函数一致（客户端流方法先接收普通参数，最后接收入站 `AsyncIterable`）；定向服务（`targeted=True`）还会要求必填的 `instance=` 关键字。单次调用选项不会改变业务方法签名：`with_options(timeout=..., idle_timeout=..., metadata=..., idempotency_key=..., instance=...)` 返回一个应用了这些选项的客户端视图。

## 使用 CLI 探索与调用

```bash
# 查看已注册的服务、实例、健康状态与方法签名
uv run meshcall router inspect --router ws://127.0.0.1:8765

# 直接在命令行发起调用
uv run meshcall call chat.v1.Chat.send '{"session_id": "s1", "text": "hello"}'
```

## 依赖注入与元数据

处理函数可以请求仅存在于服务端的值，而不把它们加入契约参数：

```python
from typing import Annotated

from meshcall import dependency, meta, mutates, service


@dependency
async def current_user(authorization: Annotated[str, meta("authorization")]) -> str:
    return authorization.removeprefix("Bearer ")


CallerId = Annotated[str | None, meta("x-caller-id")]


@service("chat.v1.Chat")
class Chat:
    @mutates(idempotent=True)
    async def send(
        self,
        text: str,
        user: str = current_user,
        caller: CallerId = None,
    ) -> Message:
        """追加一条消息并返回回复。"""
        return Message(text=f"{user}: {text} (caller={caller})")
```

`@dependency` provider 可以是同步函数、异步函数或生成器；生成器的清理逻辑在调用结束时运行，因此可以在成功时提交、在出错或取消时回滚（参见 `best-practice/`）。依赖也可以写成 `Annotated[T, dependency(provider)]`，或通过类的 `_provider()` 解析。`meta("key")` 注入一条调用元数据。被注入的参数会从契约与生成的客户端中移除；调用方可通过 `with_options(metadata=...)` 或 CLI 的 `--metadata k=v` 发送元数据。

## `Router`、`LocalRouter` 与 `Mesh`

`Router` 描述远程 Router 端点；`LocalRouter` 管理本地 `meshcall-router` 子进程；`Mesh` 是进程内的对端运行时，负责挂载本地服务、在多个客户端间复用同一条连接，并通过 `mesh.registry` 与 `mesh.auth` 暴露 Router 的内置服务：

```python
from meshcall import Mesh, Router, RouterCredentials
from meshcall_chat_v1_chat_client import ChatClient


async def main() -> None:
    async with Mesh(
        Router("ws://127.0.0.1:8765"),
        credentials=RouterCredentials(token="…"),
    ) as mesh:
        services = await mesh.registry.list()
        print([service.name for service in services])
        chat = ChatClient(mesh)  # 多个客户端共享同一条连接
        reply = await chat.with_options(timeout=5.0).last_message("s1")
```

Router 的内置服务挂载在 `mesh.registry`（`await mesh.registry.list()`、`await mesh.registry.describe(name)`）与 `mesh.auth`（`await mesh.auth.whoami()`、`await mesh.auth.issue_token(scopes, ttl_seconds=900)`、`await mesh.auth.revoke_token(token_id)`）上。

## 本地传输

`Router` 接受 `unix_path`（Linux、macOS、FreeBSD）或 `pipe`（Windows 命名管道）代替 TCP URL；`Router("unix:///path")` 与 `Router("pipe://NAME")` 同样可用。CLI 以 `unix:///path` 与 `pipe://NAME` 寻址同样的监听地址。

需要显式管理本地 Router 进程时可使用 `LocalRouter`：支持配置 `host` / `port`、`unix_path`、`pipe`、`max_frame_size`（默认且最大 64 MiB）、`outbox_size`（为不读取数据的对端排队的字节数，默认使用 Router 的 256 MiB，不得小于 `max_frame_size`）、`max_connections`、`binary_path`、`auth_file` 与就绪超时。它提供 `pid`、`is_running`、`bound_port`、`wait_closed()` 以及异步上下文管理器（`async with`）。如需覆盖内置二进制，可设置环境变量 `MESHCALL_ROUTER_BINARY`。底层原语（`RpcServer`、`ClientBase`、`WebSocketClientDriver` 与 `WebSocketRouterServerDriver`）支持自定义传输接线，并同样接受 `unix_path=` / `pipe=`。

## HTTP 传输

当 `Router` 传入 `http://` 或 `https://` 地址时，`Mesh` 会在底层自动切换为 HTTP 客户端传输（基于 `httpx`）：

```python
from meshcall import Mesh, Router

mesh = Mesh(Router("https://router.example"), token="mk_…")
chat = ChatClient(mesh)
reply = await chat.with_options(timeout=5).greet("Alice")
services = await mesh.registry.list()
```

双向流（Duplex）方法仅支持 WebSocket（在 HTTP 上会抛出 `method_not_found`，`reason: "duplex_over_http"`），且在 HTTP `Mesh` 上调用 `mesh.mount(...)` 会抛出 `ContractError`。其余能力与 WebSocket 完全一致：一元调用及其幂等重试、服务端流、客户端流，以及内置的 `mesh.registry` / `mesh.auth` 调用。

## Router 鉴权

向 `Mesh(router, ...)` 传入 `token="…"` 或 `credentials=RouterCredentials(username=..., password=...)` / `RouterCredentials(token=...)`（直接使用底层驱动时，则传给 `WebSocketClientDriver` / `WebSocketRouterServerDriver` 的 `auth=` 参数）。`mesh.auth` 提供 `whoami()`、`issue_token(scopes=..., ...)` 与 `revoke_token(...)`；`RouterAuthClient` 是同一个内置服务的底层类型化客户端。使用 `RouterScope(service=..., methods=[...])` 限制令牌权限范围，或为临时服务实例开启 `register_service=True`。

## OpenTelemetry

```bash
pip install "meshcall[otel]"
```

```python
from meshcall import Mesh, Router
from meshcall.otel import client_interceptor, server_interceptor
from meshcall_chat_v1_chat_client import ChatClient


async def main() -> None:
    async with Mesh(
        Router("ws://127.0.0.1:8765"),
        interceptors=[client_interceptor()],
    ) as mesh:
        chat = ChatClient(mesh)
        ...

    server_mesh = Mesh(Router("ws://127.0.0.1:8765"))
    server_mesh.mount(Chat())
    async with server_mesh.serve(interceptors=[server_interceptor()]):
        ...
```

每次调用都会生成一个 span 以及 `meshcall.calls` / `meshcall.call.duration` 指标；W3C trace context 作为调用元数据传递，因此链路可以穿过 Router、跨语言延续（TypeScript 对应 `meshcall/otel`）。未配置 OpenTelemetry SDK 时，这些拦截器不做任何事。

## 相关文档

- [仓库总览与快速上手](../README.zh-CN.md)
- [使用指南与场景速查手册 (Cookbook)](../docs/zh-CN/guide.md)
- [协议规范](../docs/zh-CN/protocol.md)
- [运行时架构](../docs/zh-CN/architecture.md)
- [Router 鉴权与管理](../docs/zh-CN/router.md)

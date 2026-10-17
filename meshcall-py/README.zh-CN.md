# meshcall (Python SDK)

[English](./README.md) | **简体中文**

基于 WebSocket、面向 Python、TypeScript 与 Go 的异步强类型流式 RPC 框架。
服务使用 Pydantic v2 模型定义契约，注册到独立 Go Router，并暴露一元调用（Unary）、服务端流（Server Stream）与客户端流（Client Stream）方法。订阅在 v0.3 中属于实验特性，双向流（duplex）仍是受限制的最后手段。

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
    Router,
    RpcLogger,
    mutates,
    query,
    run,
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
    run(Chat(), router=Router("ws://127.0.0.1:8765"))
```

v0.3 推荐用这些简写声明方法：

- `@query` 声明纯读取（`read_only=True`、`idempotent=True`）。
- `@mutates(...)` 声明会修改状态的一元、客户端流或双向流方法。裸 `@mutates` 默认就是 destructive；可重试的写入用 `@mutates(idempotent=True)`，需要显式声明破坏性时用 `@mutates(destructive=True)`。
- `@stream` 声明服务端流。裸 `@stream` 默认按只读处理，除非方法名看起来像变更操作；有副作用的服务端流请显式传 `read_only=False, idempotent=False`。

长写法的 `@method`、`@method.server_stream`、`@method.client_stream`、`@method.duplex` 与 `@method.subscription` 仍然可用，但新服务应使用上面的简写。双向流（一次调用中输入与输出交错）仍是最后手段：用 `@mutates(reason="…")`（或 `@method.duplex(reason=…)`）声明，处理函数接收 `DuplexChannel[In, Out]` 参数并返回 `None`。duplex 不重试、不去重、不接受幂等键，也不能通过 HTTP 调用。

处理函数也可以请求 `ctx: RpcContext` 与 `logger: RpcLogger`；二者由运行时注入，不会进入契约参数。

在开发与 CI 阶段设置 `MESHCALL_STRICT=1`，可在导入模块时立即发现缺失的文档字符串或未标注副作用的变更方法，并给出精确的修复提示。

## 运行与托管

阻塞式 `run` 入口会接管进程，直到收到 `SIGINT` / `SIGTERM`，随后先排空再停止它启动的 Router：

```python
from meshcall import Router, run

run(Chat(), router=Router("ws://127.0.0.1:8765"))
```

在异步应用中用 `serve` 托管服务；当地址是本机且没有进程在监听时，会自动拉起一个临时本地 Router：

```python
import asyncio

from meshcall import Router, serve


async def main() -> None:
    async with serve(Chat(), router=Router("ws://127.0.0.1:0")) as server:
        print(f"serving on {server.url}")  # 例如 ws://127.0.0.1:53281
        await asyncio.Event().wait()       # 离开代码块时会先排空再停止


asyncio.run(main())
```

`serve(...)` 返回 `RunningServer`（`server.url`、`await server.stop()`、`await server.wait_closed()`），也可以直接 `server = await serve(...)`。`run` 与 `serve` 都接受 `instance_id`、`max_inflight`、`strict`、`interceptors` 与日志选项，也可以用 URL 字符串代替 `Router`。

两者的 Router 解析规则一致：显式地址优先，否则读取 `MESHCALL_ROUTER_URL`（多个 Router 用逗号分隔），最后回退到 `ws://127.0.0.1:8765`。自动拉起只适用于本地地址，且只发生在 `run` / `serve` 中；设置 `Router(..., spawn_if_local=False)` 可关闭。我们推荐在所有场景下都通过 Router 连接：这样同一个节点既能注册自身服务，也能通过同一条连接作为客户端调用网格中的其他服务（`WebSocketDirectServerDriver` 仅作为早期兼容性方法保留，不推荐使用）。

## 生成并调用强类型客户端

```bash
# 生成可安装的 Python uv 包（或添加 --single-file 生成单文件 .py 客户端）
uv run meshcall generate my_app.service:Chat --output generated/chat-client

# 从同一个 Python 服务生成 TypeScript 客户端包或 Go 客户端包
uv run meshcall generate my_app.service:Chat --language typescript --output generated/chat-ts
uv run meshcall generate my_app.service:Chat --language go --output internal/chatclient
```

通过生成的客户端发起调用：

```python
from meshcall import Router
from meshcall_chat_v1_chat_client import ChatClient


async def main() -> None:
    async with Router.connect("ws://127.0.0.1:8765") as router:
        chat = ChatClient(router)  # 复用 Router 的唯一连接
        reply = await chat.with_options(
            timeout=5.0,
            idempotency_key="send-s1-1",
            metadata={"x-caller-id": "cli"},
        ).send("s1", "hello")
        print(reply.text)
```

`ChatClient.connect(...)` 是独立形式，解析 Router 的方式与 `run` 完全一致：`async with ChatClient.connect() as client:` 使用 `MESHCALL_ROUTER_URL` / 默认地址，`ChatClient.connect("ws://…")` 则使用显式地址。生成的客户端方法只接收业务参数，顺序与服务端处理函数一致（客户端流方法先接收这些参数，最后接收入站 `AsyncIterable`）；定向服务（targeted）还会要求必填的 `instance=` 关键字。单次调用选项不会改变业务方法签名：`with_options(timeout=..., idle_timeout=..., metadata=..., idempotency_key=..., instance=...)` 返回一个应用了这些选项的客户端视图。

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

## Router

`Router` 是统一句柄，用于启动服务、共享客户端连接，以及调用 Router 的内置服务：

```python
from meshcall import Router, RouterCredentials
from meshcall_chat_v1_chat_client import ChatClient


async def main() -> None:
    async with Router.connect(
        "ws://127.0.0.1:8765",
        auth=RouterCredentials(token="…"),
    ) as router:
        services = await router.list_services()
        print([service.name for service in services])
        chat = ChatClient(router)  # 多个客户端共享同一条连接
        reply = await chat.with_options(timeout=5.0).last_message("s1")
```

`Router.connect(...)` 既可以写成 `async with Router.connect(...) as router`，也可以写成 `await Router.connect(...)`；不传地址时会依次解析 `MESHCALL_ROUTER_URL`（多个 Router 可用逗号分隔）与 `ws://127.0.0.1:8765`。传给 `run` / `serve` 时直接构造 `Router(url)` 即可。用 `ChatClient(router)`（或在已连接的 Router 上调用 `ChatClient.connect(router)`）创建的客户端会复用同一条连接；关闭 Router 会一并关闭它们。

Router 的内置服务可直接调用：`await router.list_services()`、`await router.describe_service(name)`、`await router.whoami()`、`await router.issue_token(scopes, ttl_seconds=900)` 与 `await router.revoke_token(token_id)`。

## Router 与本地传输

`Router` 接受 `unix_path`（Linux、macOS、FreeBSD）或 `pipe`（Windows 命名管道）代替 TCP URL；`Router("unix:///path")` 与 `Router("pipe://NAME")` 同样可用。CLI 以 `unix:///path` 与 `pipe://NAME` 寻址同样的监听地址。

需要自己管理 Router 进程时，`WebSocketRouter` 仍然可用：`host` / `port`、`unix_path`、`pipe`、`max_frame_size`（默认且最大 64 MiB）、`outbox_size`（为不读取数据的对端排队的字节数，默认使用 Router 的 256 MiB，不得小于 `max_frame_size`）、`max_connections`、`binary_path`、`auth_file` 与就绪超时。它提供 `pid`、`is_running`、`bound_port`、`wait_closed()` 以及异步上下文管理器（`async with`）。如需覆盖内置二进制，可设置环境变量 `MESHCALL_ROUTER_BINARY`。底层 `WebSocketClientDriver` 与 `WebSocketRouterServerDriver` 仍可用于自定义接线，并同样接受 `unix_path=` / `pipe=`。

## HTTP 客户端

`HTTPRouter` 是只用 HTTP 通信的客户端 Router：像普通 Router 一样传给生成的客户端即可。它依赖 `httpx`（默认依赖）。

```python
from meshcall import HTTPRouter

http = HTTPRouter("https://router.example", token="mk_…")
chat = ChatClient(http)                    # 或 ChatClient.connect(http)
reply = await chat.with_options(timeout=5).greet("Alice")
services = await http.list_services()
```

duplex 方法只能走 WebSocket（会抛出 `method_not_found`，`reason: "duplex_over_http"`）。其余能力与 WebSocket 一致：一元调用及其幂等重试、服务端流、客户端流、订阅（按 cursor 恢复），以及内置的 Registry/AuthService 调用。

## Router 鉴权

向 `Router(...)` / `Router.connect(...)` 的 `auth` 参数传入 `RouterCredentials(username=..., password=...)` 或 `RouterCredentials(token=...)`（直接使用驱动时，则传给 `WebSocketClientDriver` / `WebSocketRouterServerDriver`）。Router 提供 `whoami()`、`issue_token(scopes=..., ...)` 与 `revoke_token(...)`；`RouterAuthClient` 是同一个内置服务的类型化客户端。使用 `RouterScope(service=..., methods=[...])` 限制令牌权限范围，或为临时服务实例开启 `register_service=True`。

## OpenTelemetry

```bash
pip install "meshcall[otel]"
```

```python
from meshcall import Router, serve
from meshcall.otel import client_interceptor, server_interceptor
from meshcall_chat_v1_chat_client import ChatClient


async def main() -> None:
    async with Router.connect(
        "ws://127.0.0.1:8765",
        interceptors=[client_interceptor()],
    ) as router:
        chat = ChatClient(router)
        ...

    async with serve(
        Chat(),
        router=Router("ws://127.0.0.1:8765"),
        interceptors=[server_interceptor()],
    ):
        ...
```

每次调用都会生成一个 span 以及 `meshcall.calls` / `meshcall.call.duration` 指标；W3C trace context 作为调用元数据传递，因此链路可以穿过 Router、跨语言延续（TypeScript 对应 `meshcall/otel`）。未配置 OpenTelemetry SDK 时，这些拦截器不做任何事。

## 实验特性：订阅与 Topic

订阅与 `Topic` 仍然可用，但在 v0.3 中是**实验特性**：API 可能变化。除非确实需要断线后自动恢复，否则优先使用普通的服务端流。

`@method.subscription(cursor="seq", resume="since")` 声明一个服务端流：客户端在遇到可重试失败后会自动重新打开调用，并把最后一个条目的 `cursor` 字段作为 `resume` 参数传回。`Topic[T]` 把发布的条目分发给进程内的每个订阅者：

```python
from meshcall import Topic, method, service

topic: Topic[Event] = Topic(history=100, cursor=lambda e: e.seq)


@service("events.v1.Events")
class Events:
    """事件流服务。"""

    @method.subscription(cursor="seq", resume="since", read_only=True)
    async def events(self, since: int | None = None) -> AsyncIterator[Event]:
        """推送 since 之后的事件。"""
        async for event in topic.subscribe(after=since):
            yield event
```

`topic.publish(event)` 可以从任意线程或事件循环调用。落后太多的订阅者会以可重试的 `resource_exhausted`（`reason: "slow_subscriber"`）被断开并重新订阅；早于历史缓冲的 cursor 以 `invalid_argument`（`reason: "cursor_expired"`）拒绝。详见[协议规范：订阅](../docs/zh-CN/protocol.md#订阅subscriptions)。

## 相关文档

- [仓库总览与快速上手](../README.zh-CN.md)
- [使用指南与场景速查手册 (Cookbook)](../docs/zh-CN/guide.md)
- [协议规范](../docs/zh-CN/protocol.md)
- [运行时架构](../docs/zh-CN/architecture.md)
- [Router 鉴权与管理](../docs/zh-CN/router.md)

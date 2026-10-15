# meshcall (Python SDK)

[English](./README.md) | **简体中文**

基于 WebSocket、面向 Python、TypeScript 与 Go 的异步强类型流式 RPC 框架。
服务使用 Pydantic v2 模型定义契约，注册到独立 Go Router，并暴露一元调用（Unary）、服务端流（Server Stream）、客户端流（Client Stream）与订阅（Subscription）方法（以及作为最后手段、受限制的双向流 duplex 方法）。

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

## 定义并运行服务

```python
import asyncio
from collections.abc import AsyncIterator
from pydantic import BaseModel
from meshcall import (
    RpcContext,
    RpcInputStream,
    RpcLogger,
    RpcServer,
    WebSocketRouter,
    method,
    service,
)
from meshcall.drivers import WebSocketRouterServerDriver


class Message(BaseModel):
    text: str


@service("chat.v1.Chat")
class Chat:
    """聊天会话服务。"""

    @method(idempotent=True)
    async def send(self, session_id: str, text: str, ctx: RpcContext, logger: RpcLogger) -> Message:
        """追加一条消息并返回回复。"""
        logger.info("session={} text={}", session_id, text)
        return Message(text=f"echo: {text}")

    @method.server_stream(read_only=True)
    async def history(self, session_id: str) -> AsyncIterator[Message]:
        """回放指定会话的历史消息。"""
        yield Message(text=f"history for {session_id}")

    @method.client_stream(idempotent=True)
    async def ingest(self, items: RpcInputStream[Message], session_id: str) -> Message:
        """批量写入消息流。"""
        count = 0
        async for _ in items:
            count += 1
        return Message(text=f"ingested {count}")


async def main() -> None:
    async with WebSocketRouter(host="127.0.0.1", port=8765):
        driver = WebSocketRouterServerDriver("ws://127.0.0.1:8765", instance_id="chat-1")
        server = RpcServer(services=[Chat()], driver=driver)
        await server.start()
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
```

在开发与 CI 阶段设置 `MESHCALL_STRICT=1`，可在导入模块时立即发现缺失的文档字符串或未标注副作用的变更方法，并给出精确的修复提示。

我们推荐在所有场景下都通过 Router 连接（`WebSocketRouterServerDriver`），这样同一个节点既能注册自身服务，也能同时作为客户端调用网格中的其他服务（`WebSocketDirectServerDriver` 仅作为早期兼容性方法保留，不推荐使用）。

## 订阅

`@method.subscription(cursor="seq", resume="since")` 声明一个服务端流：客户端在遇到可重试失败后会自动重新打开调用，并把最后一个条目的 `cursor` 字段作为 `resume` 参数传回。`Topic[T]` 把发布的条目分发给进程内的每个订阅者：

```python
topic: Topic[Event] = Topic(history=100, cursor=lambda e: e.seq)

@method.subscription(cursor="seq", resume="since", read_only=True)
async def events(self, since: int | None = None) -> AsyncIterator[Event]:
    """推送 since 之后的事件。"""
    async for event in topic.subscribe(after=since):
        yield event
```

`topic.publish(event)` 可以从任意线程或事件循环调用。落后太多的订阅者会以可重试的 `resource_exhausted`（`reason: "slow_subscriber"`）被断开并由客户端重新订阅；早于历史缓冲的 cursor 以 `invalid_argument`（`reason: "cursor_expired"`）拒绝。详见[协议规范：订阅](../docs/zh-CN/protocol.md#订阅subscriptions)。

双向流方法用 `@method.duplex(reason="…")` 声明，`reason` 必填，说明为何服务端流加一元调用、或客户端流都不能满足需求；Handler 接收 `RpcDuplex[In, Out]` 通道并返回 `None`。duplex 不重试、不去重、不接受幂等键、不能通过 HTTP 调用，也不能标注 `idempotent` / `read_only`，详见[双向流调用](../docs/zh-CN/protocol.md#双向流调用duplex-calls)。

## 使用 CLI 探索与调用

```bash
# 查看已注册的服务、实例、健康状态与方法签名
uv run meshcall router inspect --router ws://127.0.0.1:8765

# 直接在命令行发起调用
uv run meshcall call chat.v1.Chat.send '{"session_id": "s1", "text": "hello"}'
```

## 生成强类型客户端

```bash
# 生成可安装的 Python uv 包（或添加 --single-file 生成单文件 .py 客户端）
uv run meshcall generate my_app.service:Chat --output generated/chat-client

# 从同一个 Python 服务生成 TypeScript 客户端包或 Go 客户端包
uv run meshcall generate my_app.service:Chat --language typescript --output generated/chat-ts
uv run meshcall generate my_app.service:Chat --language go --output internal/chatclient
```

通过生成的客户端发起调用：

```python
from meshcall.drivers import WebSocketClientDriver
from meshcall_chat_v1_chat_client import ChatClient

async with ChatClient(WebSocketClientDriver("ws://127.0.0.1:8765")) as client:
    reply = await client.send("s1", "hello", timeout=5.0)
    print(reply.text)
```

## Router 与本地传输

`WebSocketRouter` 接受 `host` / `port`，或使用本地传输：`unix_path`（Linux、macOS、FreeBSD）或 `pipe`（Windows 命名管道）；此外还有 `max_frame_size`（默认且最大 64 MiB）、`outbox_size`（为不读取数据的对端排队的字节数，默认使用 Router 的 256 MiB，不得小于 `max_frame_size`）、`max_connections`、`binary_path`、`auth_file` 与就绪超时。它提供 `pid`、`is_running`、`bound_port`、`wait_closed()` 以及异步上下文管理器（`async with`）。如需覆盖内置二进制，可设置环境变量 `MESHCALL_ROUTER_BINARY`。

`WebSocketClientDriver` 与 `WebSocketRouterServerDriver` 可以用 `unix_path=` 或 `pipe=` 代替 URI 连接本地 Router；CLI 以 `unix:///path` 与 `pipe://NAME` 寻址同样的监听地址。

## Router 鉴权

向 `WebSocketClientDriver` 或 `WebSocketRouterServerDriver` 的 `auth` 参数传入 `RouterCredentials(username=..., password=...)` 或 `RouterCredentials(token=...)`。`RouterAuthClient` 提供 `whoami()`、`issue_token(scopes=..., ...)` 与 `revoke_token(...)`。使用 `RouterScope(service=..., methods=[...])` 限制令牌权限范围，或为临时服务实例开启 `register_service=True`。

## OpenTelemetry

```bash
pip install "meshcall[otel]"
```

```python
from meshcall.otel import client_interceptor, server_interceptor

client = ChatClient(driver, interceptors=[client_interceptor()])
server = RpcServer(services=[Chat()], driver=driver, interceptors=[server_interceptor()])
```

每次调用都会生成一个 span 以及 `meshcall.calls` / `meshcall.call.duration` 指标；W3C trace context 作为调用元数据传递，因此链路可以穿过 Router、跨语言延续（TypeScript 对应 `meshcall/otel`）。未配置 OpenTelemetry SDK 时，这些拦截器不做任何事。

## 相关文档

- [仓库总览与快速上手](../README.zh-CN.md)
- [使用指南与场景速查手册 (Cookbook)](../docs/zh-CN/guide.md)
- [协议规范](../docs/zh-CN/protocol.md)
- [运行时架构](../docs/zh-CN/architecture.md)
- [Router 鉴权与管理](../docs/zh-CN/router.md)

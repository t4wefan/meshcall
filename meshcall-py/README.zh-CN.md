# meshcall (Python SDK)

[English](./README.md) | **简体中文**

基于 WebSocket 的 Python 与 TypeScript 异步强类型流式 RPC 框架。
服务使用 Pydantic v2 模型定义契约，注册到独立 Go Router（或直接监听），并暴露一元调用（Unary）、服务端流（Server Stream）与客户端流（Client Stream）方法。

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

若需无 Router 的点对点直连模式，向 `RpcServer` 传入 `WebSocketDirectServerDriver(host="127.0.0.1", port=8765)`（或 `unix_path=...`）即可。

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

# 从同一个 Python 服务生成 TypeScript 客户端包
uv run meshcall generate my_app.service:Chat --language typescript --output generated/chat-ts
```

通过生成的客户端发起调用：

```python
from meshcall.drivers import WebSocketClientDriver
from meshcall_chat_v1_chat_client import ChatClient

async with ChatClient(WebSocketClientDriver("ws://127.0.0.1:8765")) as client:
    reply = await client.send("s1", "hello", timeout=5.0)
    print(reply.text)
```

## Router 鉴权

`WebSocketRouter` 支持 `binary_path`、`auth_file`、就绪超时、`pid`、`is_running`、`bound_port`、`wait_closed()` 以及异步上下文管理器（`async with`）。如需覆盖内置二进制，可设置环境变量 `MESHCALL_ROUTER_BINARY`。

向 `WebSocketClientDriver` 或 `WebSocketRouterServerDriver` 的 `auth` 参数传入 `RouterCredentials(username=..., password=...)` 或 `RouterCredentials(token=...)`。`RouterAuthClient` 提供 `whoami()`、`issue_token(scopes=..., ...)` 与 `revoke_token(...)`。使用 `RouterScope(service=..., methods=[...])` 限制令牌权限范围，或为临时服务实例开启 `register_service=True`。

## 相关文档

- [仓库总览与快速上手](../README.zh-CN.md)
- [使用指南与场景速查手册 (Cookbook)](../docs/zh-CN/guide.md)
- [协议规范](../docs/zh-CN/protocol.md)
- [运行时架构](../docs/zh-CN/architecture.md)
- [Router 鉴权与管理](../docs/zh-CN/router.md)

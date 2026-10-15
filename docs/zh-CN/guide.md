# MeshCall 快速上手与实战指南

[English](../guide.md) | **简体中文**

本指南同时面向**人类工程师**与 **AI 编程助手**。每一节都会先讲清楚**为什么要这样用、适用什么业务场景**，随后提供可直接复制使用的 **Python** 与 **TypeScript** 完整代码范例；Go 运行时（`meshcall-go`）写法不同之处也会给出 **Go** 示例。

---

## 目录

1. [Router 优先架构：为什么推荐始终通过 Router 通信](#1-router-优先架构为什么推荐始终通过-router-通信)
   - [本地传输：Unix 套接字与命名管道](#本地传输unix-套接字与命名管道)
2. [服务契约编写规范与严格模式 (`MESHCALL_STRICT=1`)](#2-服务契约编写规范与严格模式-meshcall_strict1)
3. [三种核心调用模式：Unary、Server Stream、Client Stream](#3-三种核心调用模式unaryserver-streamclient-stream)
   - [订阅：可恢复的流](#订阅可恢复的流)
   - [双向流（duplex）：最后的选择](#双向流duplex最后的选择)
4. [生成与使用跨语言强类型客户端](#4-生成与使用跨语言强类型客户端)
   - [从浏览器、边缘运行时或 curl 调用](#从浏览器边缘运行时或-curl-调用)
5. [使用 CLI 实时探测与调试在线服务](#5-使用-cli-实时探测与调试在线服务)
6. [无状态负载均衡 vs. 有状态实例定向调用 (`targeted`)](#6-无状态负载均衡-vs-有状态实例定向调用-targeted)
7. [生产环境高可用：三层超时、自动重试、幂等去重与优雅停机](#7-生产环境高可用三层超时自动重试幂等去重与优雅停机)
   - [Router 停机、容量限制与指标](#6-router-停机容量限制与指标)
8. [安全鉴权、ACL 权限控制与临时范围 Token](#8-安全鉴权acl-权限控制与临时范围-token)
   - [匿名调用方与 API Key](#3-匿名调用方与-api-key)
9. [拦截器（Interceptors）、元数据（Metadata）与结构化错误处理](#9-拦截器interceptors元数据metadata与结构化错误处理)
   - [OpenTelemetry 链路追踪](#opentelemetry-链路追踪)
10. [AI 编程助手集成（`meshcall init`）](#10-ai-编程助手集成meshcall-init)

---

## 1. Router 优先架构：为什么推荐始终通过 Router 通信

**核心建议：** 无论是在单机开发还是分布式生产环境中，**我们推荐始终通过 Go Router（`meshcall-router` / `WebSocketRouter`）进行通信**。由于预编译的 Router 二进制已直接内置在 Python 与 TypeScript 安装包中，启动 Router 无需任何额外依赖。

为什么在所有场景下都应该使用 Router？

1. **单个节点可同时承担服务端与客户端角色** —— 在传统的点对点 RPC 中，每个服务都必须分配并监听自己的端口，调用方也必须维护所有对端节点的地址。而在 Router 模式下，全网只有一个统一入口（如 `ws://127.0.0.1:8765`）。任何一个 Python 或 TypeScript 节点只需主动连接 Router，既可以向 Router 注册自身提供的 `@service`，又可以同时作为客户端调用网格中的任何其他服务。
2. **原生 CLI 与 AI 实时可观测** —— 只要通过 Router 运行，你和 AI 编程助手随时都可以使用 `meshcall router inspect` 查看全网服务契约，并用 `meshcall call` 直接发起调用调试。
3. **平滑扩缩容与高可用** —— 随时拉起第二个实例即可自动获得负载均衡、健康检查、熔断保护与无损滚动停机（`drain=True`），业务代码零改动。

### Python：同一节点兼具服务端与客户端

```python
from meshcall import RpcServer, WebSocketRouter
from meshcall.drivers import WebSocketClientDriver, WebSocketRouterServerDriver

# 1. 启动 SDK 内置的 Go Router（也可在外部独立运行 `meshcall-router --port 8765`）
router = WebSocketRouter(host="127.0.0.1", port=8765)
await router.start()

# 2. 任何节点都可以同时创建客户端调用其他服务，并通过 RpcServer 注册自身服务
other_service_client = OtherServiceClient(WebSocketClientDriver("ws://127.0.0.1:8765"))

server = RpcServer(
    services=[MyService(downstream=other_service_client)],
    driver=WebSocketRouterServerDriver("ws://127.0.0.1:8765", instance_id="worker-1"),
)
await server.start()
```

### TypeScript：同一节点兼具服务端与客户端

```typescript
import { MeshCallClient, MeshCallServer, WebSocketRouter } from "meshcall";

// 1. 启动内置的 Go Router（或连接已有的 Router）
const router = new WebSocketRouter({ host: "127.0.0.1", port: 8765 });
await router.start();

// 2. 该节点既可以调用其他服务，也可以向 Router 注册自身服务
const client = new MeshCallClient("ws://127.0.0.1:8765");

const server = new MeshCallServer({
  services: [myService],
  router: { endpoint: "ws://127.0.0.1:8765", instanceId: "worker-ts-1" },
});
await server.start();
```

### Go：同一节点兼具服务端与客户端

Go 运行时没有 Router 启动器：先运行 `meshcall-router --port 8765`（或使用已部署的 Router），再连接它。

```go
chat := meshcall.NewService("chat.v1.Chat", meshcall.Describe("Chat sessions."))
// meshcall.HandleUnary(chat, "send", handler, meshcall.Doc("…"), meshcall.Idempotent())

server, err := meshcall.NewServer(meshcall.ServerOptions{Services: []*meshcall.Service{chat}, InstanceID: "worker-go-1"})
err = server.ConnectRouter(ctx, "ws://127.0.0.1:8765")

rpc, err := meshcall.NewClient(meshcall.ClientOptions{}, "ws://127.0.0.1:8765")
```

### 本地传输：Unix 套接字与命名管道

所有节点都在同一台机器上时可以不走 TCP：Router 监听 Unix 套接字（Linux、macOS、FreeBSD）或命名管道（Windows），只有 Router 自身的用户（命名管道另加 SYSTEM 与 Administrators）可以访问。帧与行为完全一致。

```bash
meshcall-router --unix-socket /tmp/mesh.sock               # Windows：--pipe mesh
meshcall --router unix:///tmp/mesh.sock router inspect     # Windows：pipe://mesh
```

- **Python**：`WebSocketRouter(unix_path="/tmp/mesh.sock")`、`WebSocketClientDriver(unix_path=...)`、`WebSocketRouterServerDriver(unix_path=...)`；Windows 上用 `pipe="mesh"`。
- **TypeScript**：`new WebSocketRouter({ unixPath })`、`new MeshCallClient({ unixPath: "/tmp/mesh.sock" })`、`router: { endpoint: { unixPath } }`；Windows 上用 `{ pipe: "mesh" }`。
- **Go** 与 CLI：端点字符串 `unix:///tmp/mesh.sock` 与 `pipe://mesh`。

> **关于直连模式（`WebSocketDirectServerDriver` / `MeshCallServer({ port })`）的说明：** 点对点直连模式仅作为架构早期演进的痕迹与底层兼容性方法保留，**不推荐在实际项目中使用**。直连模式需要手动管理服务端监听端口，无法让节点在统一网格中对等地兼具服务端与客户端身份，且无法使用 `meshcall router inspect` 等路由与可观测能力。

---

## 2. 服务契约编写规范与严格模式 (`MESHCALL_STRICT=1`)

**核心优势：** MeshCall 的契约不仅包含字段类型 Schema，还携带**方法语义注解**（`read_only`、`idempotent`、`destructive`、`deprecated`、`description`）。客户端重试策略会根据这些注解自动判断哪些方法断线后可以安全重试；CLI 与 AI 助手也会直接读取这些元信息。

### 核心设计准则

1. **服务名必须包含版本段**：推荐格式为 `<命名空间>.v<主版本>.<服务名>`（例如 `billing.v1.InvoiceService`）。未写版本号的名称（如 `chat`）会被自动规范化为 `chat.v0`。
2. **函数参数即请求体（Parameters are the request）**：不要把所有参数包进一个 `Request` 类里。直接按自然习惯写多个函数参数（如 `session_id: str, limit: int = 20`），MeshCall 会自动合成请求模型。
3. **返回值必须是结构化模型**：始终返回 Pydantic `BaseModel`（Python）或对象 `TypeContract`（TypeScript），不要直接返回裸的 `str` / `int` / `dict`，以便未来平滑扩展字段。
4. **开发与 CI 阶段开启 `MESHCALL_STRICT=1`**：开启后，缺少 docstring/description（`W001`）、修改状态的方法未标注副作用属性（`W002`）、注解自相矛盾（`W003`）等契约告警（`W001`–`W006`）会在导入或启动阶段立即报错，并给出精确到行的修复提示（`hint`）。

| 语义注解 | 业务含义 | `RetryPolicy` 是否自动重试？ |
| --- | --- | --- |
| `read_only=True` (`readOnly: true`) | 纯读取查询，不修改任何状态（自动隐含 `idempotent`） | **是**（遇到可重试错误自动重试） |
| `idempotent=True` (`idempotent: true`) | 会修改状态，但使用相同参数重复执行多次的结果完全一致（如按 ID 覆盖更新） | **是** |
| `destructive=True` (`destructive: true`) | 不可直接重复执行的写操作（如扣款、追加日志、删除） | **否**（除非调用方显式传入了 `idempotency_key`） |
| `deprecated="use_v2"` | 将方法标记为废弃，生成的客户端会带上 `@deprecated` 编译器警告 | 取决于读写注解 |

---

## 3. 三种核心调用模式：Unary、Server Stream、Client Stream

**核心优势：** 流式调用按方向独立维护**条目信用窗口（Item Credit，初始窗口为 16）**。当消费端处理较慢时，生产端会自动等待窗口授信，绝不会在内存中无限堆积数据；同时单连接上的多个并发流按 `call_id` 公平轮转发送，大流量流不会饿死其他普通调用。

### Python 服务端与客户端范例

```python
from collections.abc import AsyncIterator
from pydantic import BaseModel
from meshcall import RpcContext, RpcInputStream, RpcLogger, method, service


class Chunk(BaseModel):
    text: str


class Summary(BaseModel):
    count: int
    combined: str


@service("text.v1.TextProcessor")
class TextProcessor:
    """提供文本一元处理、服务端流分词与客户端流聚合。"""

    # 1. 一元调用 (Unary): 普通参数 -> 响应模型
    @method(read_only=True)
    async def echo(self, text: str, ctx: RpcContext) -> Chunk:
        """原样返回输入文本，并通过 Trailer 附带字符长度。"""
        ctx.set_trailer("length", str(len(text)))
        return Chunk(text=text)

    # 2. 服务端流 (Server Stream): 普通参数 -> AsyncIterator[条目模型]
    @method.server_stream(read_only=True)
    async def tokenize(self, sentence: str, logger: RpcLogger) -> AsyncIterator[Chunk]:
        """将句子按空格切分并逐词流式推送给客户端。"""
        logger.info("tokenizing sentence")
        for word in sentence.split():
            yield Chunk(text=word)

    # 3. 客户端流 (Client Stream): RpcInputStream[条目模型] + 普通参数 -> 响应模型
    @method.client_stream(idempotent=True)
    async def join_chunks(
        self,
        chunks: RpcInputStream[Chunk],
        separator: str = " ",
    ) -> Summary:
        """接收客户端持续发送的文本块流，并以 separator 拼接后返回汇总结果。"""
        parts: list[str] = []
        async for chunk in chunks:
            parts.append(chunk.text)
        return Summary(count=len(parts), combined=separator.join(parts))
```

**在生成的 Python 客户端中调用这三种模式：**

```python
async with TextProcessorClient(driver) as client:
    # 1. 一元调用
    chunk = await client.echo("hello", timeout=5.0)

    # 2. 服务端流（如果可能提前 break 退出循环，建议使用 `async with` 以自动通知服务端取消流）
    async with client.tokenize("one two three", timeout=10.0, idle_timeout=3.0) as stream:
        async for item in stream:
            print(item.text)

    # 3. 客户端流
    call = await client.join_chunks(separator="-", timeout=10.0)
    await call.send(Chunk(text="a"))
    await call.send(Chunk(text="b"))
    await call.close_send()
    summary = await call
    print(summary.combined)  # "a-b"
```

### 订阅：可恢复的流

事件、通知与实时数据适合声明为**订阅**：一个服务端流，其条目带有有序的 cursor（如 `seq`），请求带一个可选的恢复参数（如 `since`）。连接断开或遇到可重试错误后，生成的客户端会自动重新打开调用，把最后的 `seq` 作为 `since` 传回，对调用方而言仍是同一个迭代器；订阅没有默认超时。Handler 必须只产出 `since` 之后的条目。

`Topic`（Python 与 TypeScript）把条目分发给同一进程内的每个订阅者，并保留有限的历史用于重放。落后太多的订阅者以可重试的 `resource_exhausted`（`reason: "slow_subscriber"`）被断开并重新订阅；早于历史的 cursor 以 `invalid_argument`（`reason: "cursor_expired"`）失败，而不是悄悄跳过事件。跨实例时，请把产生事件的服务设为 `targeted`，或把消息中间件桥接到每个实例的 Topic：Router 不是消息中间件。

```python
from meshcall import Topic


class Event(BaseModel):
    seq: int
    text: str


@service("feed.v1.Feed")
class Feed:
    """实时事件。"""

    def __init__(self) -> None:
        self.topic: Topic[Event] = Topic(history=1000, cursor=lambda e: e.seq)

    @method.subscription(cursor="seq", resume="since", read_only=True)
    async def events(self, since: int | None = None) -> AsyncIterator[Event]:
        """推送 since 之后的每个事件。"""
        async for event in self.topic.subscribe(after=since):
            yield event

# 在任意位置（任意线程）发布：feed.topic.publish(Event(seq=n, text="..."))
# 消费：async for event in client.events(): ...
```

TypeScript 使用 `subscriptionMethod({ cursor: "seq", resume: "since", … })` 与 `new Topic<Event>({ history: 1000, cursor: (e) => e.seq })`，Handler 中 `for await (const event of topic.subscribe(since)) yield event;`。Go 用 `meshcall.HandleSubscription(svc, "events", "seq", "since", handler)` 声明同样的契约（Go 没有 `Topic`），生成的 Go 客户端返回以同样方式自动重新订阅的 `*meshcall.Subscription`。完整规则见[协议规范：订阅](protocol.md#订阅subscriptions)。

### 双向流（duplex）：最后的选择

duplex 方法（Python `@method.duplex(reason="…")` 加一个 `channel: RpcDuplex[In, Out]` 参数，TypeScript `duplexMethod({ reason, … })`，Go `meshcall.HandleDuplex(svc, name, reason, handler)`）同时流式传输输入与输出。只有当输出必须与仍在不断到达的输入在同一调用内交错时才使用它，例如用户还在说话时就回应的语音轮次。它从不重试、不去重，不接受幂等键，不能标注 `idempotent` / `read_only`，不能通过 HTTP 调用，并在整个生命周期内固定在一个实例上；`reason` 会写入契约。只发送不读取的一方会在对方信用耗尽后卡住，因此请在发送的同时并发读取输出。请先尝试"服务端流 + 一元调用"或客户端流。详见[协议规范：双向流调用](protocol.md#双向流调用duplex-calls)。

---

## 4. 生成与使用跨语言强类型客户端

**核心优势：** 统一的 Go CLI（`meshcall generate`）既可以直接读取 Python `module:Class`，也可以读取通用的 `.meshcall.json` 契约文件，一键生成 Python、TypeScript 或 Go 客户端。**生成的客户端不仅校验入参，还会自动校验服务端返回的响应与流条目**，第一时间拦截服务端与客户端契约版本不一致的问题。

```bash
# 1. 从 Python 服务 -> 生成可安装的 Python uv 客户端包
uv run meshcall generate my_app.service:TextProcessor --output generated/text-py

# 2. 从 Python 服务 -> 生成单文件 Python 客户端（适合脚本或快速集成）
uv run meshcall generate my_app.service:TextProcessor \
  --single-file \
  --output src/my_app/text_client.py

# 3. 从 Python 服务 -> 生成 TypeScript Yarn/npm 客户端包
uv run meshcall generate my_app.service:TextProcessor \
  --language typescript \
  --output generated/text-ts

# 4. 从通用 .meshcall.json 契约文件（由 TS `writeContract`、Go `meshcall.WriteContract(path, svc)`
#    或 `meshcall export` 导出）生成客户端
npx meshcall generate contract.meshcall.json \
  --language python \
  --output generated/py-client

# 5. 生成 Go 客户端：单个文件 <output>/client.go（或你指定的 .go 路径）
meshcall generate contract.meshcall.json \
  --language go \
  --package-name textclient \
  --output internal/textclient
```

### 从浏览器、边缘运行时或 curl 调用

浏览器无法在 WebSocket 上设置 `Authorization` 头，因此 Web 与边缘代码调用 Router 的 HTTP API（同一端口，`/v1/` 路径下）。生成的 TypeScript 客户端可以用 `meshcall/web` 的 `MeshCallHttpClient`（不依赖 Node）代替 `MeshCallClient`；一元调用、流与订阅均可用，duplex 不可用。

```typescript
import { MeshCallHttpClient } from "meshcall/web";
import { TextProcessorClient } from "./generated/text-ts/src/index.js";

const client = new TextProcessorClient(new MeshCallHttpClient("https://router.example", { token }));
const chunk = await client.echo("hello", { timeoutMs: 5_000 });
```

`token` 传入短期 Token（`mt_…`）或 API Key（`mk_…`）；其他源的页面需要 Router 以 `--http-cors-origin https://app.example` 启动。任何 HTTP 客户端都可以这样调用：

```bash
curl http://127.0.0.1:8765/v1/call/text.v1.TextProcessor/echo -d '{"text": "hi"}'
curl -N http://127.0.0.1:8765/v1/call/text.v1.TextProcessor/tokenize -d '{"sentence": "a b"}'  # 若干 {"item":…} 行，最后 {"end":…}
curl http://127.0.0.1:8765/v1/openapi.json    # 你有权调用的所有方法的 OpenAPI 3.1 文档
```

完整映射见[协议规范：HTTP 调用](protocol.md#http-调用http-calls)。

---

## 5. 使用 CLI 实时探测与调试在线服务

**核心优势：** 服务实例注册到 Router 时会自动上报完整契约。无论人类开发者还是 AI Agent，都不需要翻找服务端源码或写临时测试脚本，直接用两条 CLI 命令即可摸清整个服务网格并完成调用调试。

```bash
# 1. 查看 Router 上注册的所有服务、在线实例、当前并发数、熔断器状态与方法签名
uv run meshcall router inspect --router ws://127.0.0.1:8765

# 2. 导出某个在线服务的完整 JSON 契约
uv run meshcall router inspect text.v1.TextProcessor --json

# 3. 直接调用一元或服务端流方法（附加超时与自定义 Metadata）
uv run meshcall call text.v1.TextProcessor.tokenize '{"sentence": "hello mesh"}' \
  --timeout 5s \
  --metadata trace_id=req-123

# 4. 通过管道向客户端流方法灌入多行 JSON 数据 (--stdin)
printf '{"text":"alpha"}\n{"text":"beta"}\n' | \
  uv run meshcall call text.v1.TextProcessor.join_chunks '{"separator": ":"}' --stdin
```

> **智能错误诊断**：当你传错字段名或类型时，`meshcall call` 会自动查询 Router 的 `Registry` 契约，在终端高亮打印出哪个字段违反了哪条校验规则（`violations`），并展示该方法的正确参数格式。

---

## 6. 无状态负载均衡 vs. 有状态实例定向调用 (`targeted`)

**核心优势：** 无状态服务可通过 5 种策略按方法自动均衡流量；而有状态服务（如加载了特定权重的 GPU 推理节点、内存游戏房间、Actor 实例）则可通过 `targeted=True` 强制按 `instance_id` 精确寻址。

### 无状态负载均衡策略

可在每个方法上单独指定策略（默认为 `round_robin` 轮询）：

```python
from meshcall import Balance, method, service

@service("compute.v1.Worker")
class Worker:
    """无状态计算与会话亲和计算服务。"""

    # 选择当前正在处理的并发请求数（in-flight）最少的实例
    @method(read_only=True, balance=Balance.least_inflight())
    async def heavy_compute(self, payload: str) -> Result:
        """将耗时计算调度给最空闲的节点。"""
        ...

    # 按请求中的 session_id 字段哈希路由到固定实例（优先命中本地内存缓存）
    @method(idempotent=True, balance=Balance.sticky("session_id"))
    async def step_session(self, session_id: str, prompt: str) -> Result:
        """在保持会话亲和性的前提下处理请求。"""
        ...
```

### 有状态实例定向服务 (`targeted=True`)

当每个服务实例承载不同状态时（例如 `gpu-worker-0` 加载了模型 A，`gpu-worker-1` 加载了模型 B），在服务上声明 `targeted=True`：
- 若调用方未指定 `instance_id`，Router 会在路由前直接拒绝并返回 `invalid_argument`（`reason: "instance_required"`）。
- **编译期防呆**：生成的 Python 与 TypeScript 客户端会将 `instance` 生成为**必填参数**，一旦漏传，IDE 和类型检查器（`pyright` / `tsc`）在编写代码阶段就会报错！

```python
@service("gpu.v1.ModelRunner", targeted=True)
class ModelRunner:
    """绑定特定 GPU 显存状态的推理节点。"""

    @method(read_only=True)
    async def infer(self, prompt: str, ctx: RpcContext) -> Output:
        """在当前指定的 GPU 实例上执行推理。"""
        return Output(text=f"Processed by {ctx.instance_id}: {prompt}")
```

调用定向服务：

```python
# 生成的客户端要求显式传入 instance= 参数：
res = await client.infer("Hello!", instance="gpu-worker-1", timeout=10.0)
```

或者通过 CLI 指定 `--instance`：

```bash
uv run meshcall call gpu.v1.ModelRunner.infer '{"prompt": "Hello!"}' --instance gpu-worker-1
```

---

## 7. 生产环境高可用：三层超时、自动重试、幂等去重与优雅停机

**核心优势：** 无需在 Router 中引入数据库或状态同步，即可实现超时兜底、自动故障转移、重复请求去重回放与零中断滚动发版。

### 1. 三层相对超时机制（无需机器间时钟同步）

调用时传入 `timeout=`（Python，单位秒）或 `timeoutMs`（TypeScript，单位毫秒）：
- **第一层（Server）**：在 `T` 时刻触发，立即取消业务协程并返回 `deadline_exceeded`（`source: "server"`）。
- **第二层（Router）**：在 `T + 500ms` 触发，防止某个服务实例假死卡住连接，自动向客户端返回 `deadline_exceeded`（`source: "router"`）并回收路由。
- **第三层（Client）**：在 `T + max(1s, 10%)` 触发，防止网络黑洞或 Router 故障导致调用方永久挂起（`source: "client"`）。
- 在服务端 Handler 内部，可在执行耗时子任务前通过 `ctx.remaining()`（Python）或 `ctx.remainingMs()`（TS）查看剩余时间预算。

### 2. 客户端自动重试（`RetryPolicy`）与幂等键（`idempotency_key`）

为客户端配置 `RetryPolicy` 后，当**一元调用**遇到可重试错误（`retryable=True`，如实例断线 `unavailable` 或满载 `resource_exhausted`）且满足以下任一条件时，客户端会自动重发：
1. 方法在契约中标注了 `read_only=True` 或 `idempotent=True`；
2. 或者调用时传入了 `idempotency_key`（如每个业务操作生成一个 UUID）。

当一元调用携带 `idempotency_key` 时，Router 会按 `(service, method, idempotency_key, instance_id)` 自动合并同一时刻的并发重复请求，并在完成后将结果缓存 60 秒以供重试直接回放。

```python
from meshcall import RetryPolicy

client = CounterServiceClient(
    driver,
    retry=RetryPolicy(max_attempts=3, initial_backoff=0.05, max_backoff=1.0),
)
```

### 3. 单实例并发限制（`max_inflight`）与自动熔断

为有容量上限的服务实例（如数据库连接池、GPU 推理）设置 `max_inflight`，当该实例满载时 Router 会自动将新请求绕行到其他空闲实例：

```python
server = RpcServer(services=[Worker()], driver=driver, max_inflight=32)
```

当某个实例连续超时或返回 `internal`/`unavailable` 达到阈值（默认连续 5 次）时，Router 的实例级熔断器会自动将其隔离 10 秒，随后放行 1 个半开探测请求（Probe），成功则恢复流量。

### 4. 多 Router 无状态双活容灾

可以同时启动多个独立的 `meshcall-router` 进程，并将地址列表同时传给服务端与客户端——服务端会同时向**所有** Router 保持注册，客户端会在当前 Router 故障时自动切换到下一个可用 Router：

```python
routers = ["ws://router-1.internal:8765", "ws://router-2.internal:8765"]
server_driver = WebSocketRouterServerDriver(routers, instance_id="worker-1")
client_driver = WebSocketClientDriver(routers)
```

### 5. 零中断优雅停机（Graceful Drain）

在服务滚动更新或收到 `SIGTERM` 信号时，启用 `drain=True`。服务实例会先通知 Router 将自己从负载均衡池中摘除（不再接收新请求），等待已在进行中的调用全部处理完毕后再关闭连接：

```python
# Python
await server.stop(drain=True, drain_timeout=30.0)
```

```typescript
// TypeScript
await server.close({ drain: true, drainTimeoutMs: 30_000 });
```

Go：`server.Drain(ctx)`，然后 `server.Close()`。

### 6. Router 停机、容量限制与指标

用 SIGTERM（或 SIGINT）重启 Router 时，它会先排空：`/readyz` 返回 503，新调用收到可重试的 `unavailable`（`reason: "router_draining"`），空闲连接以 1001 关闭，客户端与服务随之转到其他 Router；超过 `--shutdown-timeout`（默认 10 秒）仍在进行的调用以可重试的 `unavailable`（`reason: "router_shutdown"`）结束。第二次信号跳过等待。由 Python 或 TypeScript 启动器托管的 Router 通过关闭 stdin 停止，会立即结束进行中的调用。

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `--max-calls-per-connection` | 1000（`0` 表示不限） | 该连接上超出的调用以可重试的 `resource_exhausted` / `overloaded` 失败，连接保持打开 |
| `--max-frame-size` | 64 MiB（也是最大值） | 单个负载或流条目必须放进一帧；更大的数据请用流发送 |
| `--outbox-size` | 256 MiB | Router 对停止读取的对端最多可欠多少数据，超出即断开 |
| `--metrics-listen` | 关闭 | 在独立监听地址（如 `127.0.0.1:9464`）的 `http://ADDR/metrics` 提供 Prometheus 指标 |

每个参数都有对应的 `MESHCALL_ROUTER_<FLAG>` 环境变量；启动器接受 `max_frame_size` / `maxFrameSize` 与 `outbox_size` / `outboxSize`。

---

## 8. 安全鉴权、ACL 权限控制与临时范围 Token

**核心优势：** 生产环境下可通过 `router-auth.json` 为不同服务和客户端配置最小权限 ACL（基于 PBKDF2-HMAC-SHA256 哈希），并通过内置的 `AuthService` 动态签发带过期时间和精确方法范围的临时 Token。

### 1. 生成密码哈希并配置 `router-auth.json`

使用内置的 `meshcall-router` 生成密码哈希：

```bash
python3 -c 'import getpass,sys; sys.stdout.write(getpass.getpass("Password: "))' \
  | uv run meshcall-router hash-password
```

编写 `router-auth.json`：

```json
{
  "users": [
    {
      "username": "backend-app",
      "password_hash": "<替换为生成的哈希字符串>",
      "roles": ["client"],
      "call": [
        "chat.v1.Chat/*",
        "meshcall.router.v1.AuthService/issue_token",
        "meshcall.router.v1.AuthService/revoke_token",
        "meshcall.router.v1.Registry/*"
      ]
    },
    {
      "username": "chat-worker",
      "password_hash": "<替换为生成的哈希字符串>",
      "roles": ["server"],
      "register": ["chat.v1.Chat"]
    }
  ]
}
```

启动 Router 时传入 `WebSocketRouter(port=8765, auth_file="router-auth.json")`（或命令行 `--auth-file router-auth.json`）。

### 2. 动态签发与吊销临时范围 Token

```python
from meshcall import RouterAuthClient, RouterCredentials, RouterScope
from meshcall.drivers import WebSocketClientDriver

admin_creds = RouterCredentials(username="backend-app", password="...")
async with RouterAuthClient(WebSocketClientDriver("ws://127.0.0.1:8765", auth=admin_creds)) as auth:
    # 签发一个有效期 15 分钟、仅允许调用 `chat.v1.Chat/send` 的受限临时 Token
    issued = await auth.issue_token(
        ttl_seconds=900,
        scopes=[RouterScope(service="chat.v1.Chat", methods=["send"])],
    )

# 将 `issued.token` 交给下游客户端使用：
scoped_driver = WebSocketClientDriver(
    "ws://127.0.0.1:8765",
    auth=RouterCredentials(token=issued.token),
)
```

### 3. 匿名调用方与 API Key

不发送 `Authorization` 头的调用方是保留用户 `anonymous`。它可以连接，但除非鉴权文件为它授予 `call` 规则，否则每个调用都返回 `permission_denied`；只有鉴权文件能让某个方法公开。

```json
{ "username": "anonymous", "roles": ["client"], "call": ["status.v1.Health/*"] }
```

为机器调用方（Webhook、CI、外部服务）发放长期 **API Key**，而不是密码：

```bash
meshcall-router new-api-key ci-bot   # 只打印一次 Key，以及应加入 "api_keys" 的条目
```

填好条目的 `call` 列表后重启 Router。调用方以 `Authorization: Bearer mk_…` 发送 Key：Python 为 `RouterCredentials(token="mk_…")`，TypeScript 为 `{ auth: { token: "mk_…" } }`，Go 为 `ClientOptions{Authorization: "Bearer mk_…"}`，curl 为 `-H 'Authorization: Bearer mk_…'`。校验 Key 只需一次哈希查找；轮换方式是先加新 Key 再删旧 Key。详见 [Router 架构与鉴权](router.md#连接时认证调用时授权)。

---

## 9. 拦截器（Interceptors）、元数据（Metadata）与结构化错误处理

**核心优势：** 通过拦截器统一注入链路追踪（Trace ID）或鉴权信息；通过强类型异常子类与字段级 `violations` 精准处理异常，告别字符串正则匹配。

### 客户端与服务端拦截器（Python）

```python
from meshcall import ClientCallInfo, ClientCallNext, RpcContext, ServerCallNext

# 客户端拦截器：自动为每次调用注入 trace-id 元数据
async def trace_client_interceptor(info: ClientCallInfo, call_next: ClientCallNext):
    metadata = dict(info.options.metadata or {})
    metadata.setdefault("trace-id", "trace-abc-123")
    info.options = info.options.with_overrides(metadata=metadata)
    return await call_next(info)

# 服务端拦截器：读取调用方元数据并在响应 Trailer 中回传
async def audit_server_interceptor(ctx: RpcContext, request: object, call_next: ServerCallNext):
    trace_id = ctx.metadata.get("trace-id", "none")
    ctx.set_trailer("echo-trace-id", trace_id)
    return await call_next(ctx, request)
```

### OpenTelemetry 链路追踪

在两端都加上 OpenTelemetry 拦截器。Trace context 作为 W3C `traceparent` / `tracestate` 调用元数据传递，因此同一条链路可以穿过 Router、跨语言延续；每个调用生成一个 `service/method` span 与 `meshcall.calls` / `meshcall.call.duration` 指标。未配置 OpenTelemetry SDK 时它们什么也不做。

```python
# pip install "meshcall[otel]"
from meshcall.otel import client_interceptor, server_interceptor

client = TextProcessorClient(driver, interceptors=[client_interceptor()])
server = RpcServer(services=[TextProcessor()], driver=server_driver, interceptors=[server_interceptor()])
```

```typescript
// 需要可选的 peer dependency @opentelemetry/api
import { otelClientInterceptor, otelServerInterceptor } from "meshcall/otel";

const client = new MeshCallClient(url, undefined, { interceptors: [otelClientInterceptor()] });
const server = new MeshCallServer({ services, router: { endpoint: url }, interceptors: [otelServerInterceptor()] });
```

Go 没有 OpenTelemetry 包；`ClientInterceptor` / `ServerInterceptor` 可以自行读写 `traceparent` 元数据键。

### 捕获结构化错误

每个标准协议错误码都对应一个专用的异常子类（`InvalidArgumentError`、`DeadlineExceededError`、`UnavailableError`、`ResourceExhaustedError`、`PermissionDeniedError`、`MethodNotFoundError`、`ProtocolError`、`InternalError`、`CallCancelledError`）：

```python
from meshcall import DeadlineExceededError, InvalidArgumentError, UnavailableError

try:
    await client.add(10, 20, timeout=2.0)
except InvalidArgumentError as err:
    # 遍历字段级参数校验失败详情（协议保证绝不回显敏感原始输入值）
    for v in err.violations:
        print(f"字段 '{v.path}' 校验失败 ({v.code}): {v.message}")
except DeadlineExceededError as err:
    print(f"调用超时 (由 {err.source} 判定, 原因={err.reason})")
except UnavailableError as err:
    print(f"服务暂不可用 (原因={err.reason}, 是否可重试={err.retryable})")
```

---

## 10. AI 编程助手集成（`meshcall init`）

将 MeshCall 的开发说明安装到当前项目中，供 AI 编程助手在编写服务、生成客户端和命令行调试时参考：

```bash
# 默认：写入 .agents/skills/meshcall/SKILL.md 并更新 AGENTS.md
# 适用于 OpenAI Codex、Google Antigravity、OpenCode、Amp 等支持开放标准的工具
uv run meshcall init

# 改为面向特定工具生成（可重复传入，或用逗号分隔）：
uv run meshcall init --agent claude     # .claude/skills/meshcall/SKILL.md + CLAUDE.md
uv run meshcall init --agent cursor     # .cursor/rules/meshcall.mdc + AGENTS.md
uv run meshcall init --agent copilot    # .github/skills/meshcall/SKILL.md + .github/copilot-instructions.md
uv run meshcall init --agent windsurf   # .windsurf/rules/meshcall.md + AGENTS.md
uv run meshcall init --agent all        # 以上全部目标，外加默认的 universal 目标
```

指定 `--agent` 时只写入所列目标；若还想保留 `.agents/` 默认位置，请同时加上 `universal`。重复执行是幂等的；被修改过的技能文件会保留，除非传入 `--force`。


# MeshCall 快速上手与实战指南

[English](../guide.md) | **简体中文**

本指南同时面向**人类工程师**与 **AI 编程助手**。每一节都会先讲清楚**为什么要这样用、适用什么业务场景**，随后提供可直接复制使用的 **Python** 与 **TypeScript** 完整代码范例；Go 运行时（`meshcall-go`）写法不同之处也会给出 **Go** 示例。

MeshCall 在 Python、TypeScript 与 Go 中围绕清晰的**四层 API（4-Layer API）**构建：

| 层级 | 职责 | Python | TypeScript | Go |
| --- | --- | --- | --- | --- |
| **第 1 层：服务与契约（Service & Contract）** | 纯业务逻辑与 Schema（`@service`、`@query`、`@mutates`、`@stream`、`@dependency`、`meta`） | `@service`, `@query`, `@mutates`, `@stream` | `@service`, `@query`, `@mutates`, `@stream` | `meshcall.ServiceMeta` + `meshcall.MethodMeta`（或 `NewService`） |
| **第 2 层：Router（`Router` / `LocalRouter`）** | 路由平面所在的位置（`Router`），或显式管理的本地 `meshcall-router` 子进程（`LocalRouter`） | `Router(url=…)`, `LocalRouter(port=…)` | `new Router(…)`, `LocalRouter.start(…)` | `NewRouter(…)`, `StartLocalRouter(ctx, …)` |
| **第 3 层：Mesh（`Mesh`）** | 多路复用连接、服务挂载（`mount`）、生命周期（`run`、`serve`、`drain`）以及内置 `.registry` / `.auth` | `Mesh(router)`, `mesh.mount(svc)`, `mesh.run()`, `mesh.serve()` | `new Mesh(router)`, `mesh.mount(svc)`, `mesh.serve()`, `mesh.startServing()` | `NewMesh(router)`, `mesh.Mount(svc)`, `mesh.Run()`, `mesh.Serve(ctx)`, `mesh.StartServing(ctx)` |
| **第 4 层：生成客户端（`<Service>Client`）** | 绑定到 `Mesh` 的强类型 RPC 客户端，通过 `with_options` 设置单次调用控制参数 | `MyClient(mesh)`, `client.with_options(…)` | `new MyClient(mesh)`, `client.withOptions(…)` | `NewMyClient(mesh)`, `client.WithOptions(…)` |

---

## 目录

1. [Router 优先架构与四层运行时](#1-router-优先架构与四层运行时)
   - [第 2 层与第 3 层：`Router`、`LocalRouter` 与 `Mesh`](#第-2-层与第-3-层routerlocalrouter-与-mesh)
   - [挂载与启动服务：`mesh.mount`、`mesh.run` 与 `mesh.serve`](#挂载与启动服务meshmountmeshrun-与-meshserve)
   - [本地传输：Unix 套接字与命名管道](#本地传输unix-套接字与命名管道)
2. [服务契约编写规范与严格模式 (`MESHCALL_STRICT=1`)](#2-服务契约编写规范与严格模式-meshcall_strict1)
   - [方法声明：`@query`、`@mutates`、`@stream`](#方法声明querymutatesstream)
   - [依赖注入与 `meta()`](#依赖注入与-meta)
3. [三种核心调用模式：Unary、Server Stream、Client Stream](#3-三种核心调用模式unaryserver-streamclient-stream)
   - [双向流（duplex）：最后的选择](#双向流duplex最后的选择)
4. [生成与使用跨语言强类型客户端](#4-生成与使用跨语言强类型客户端)
   - [单次调用选项 `with_options`](#单次调用选项-with_options)
   - [从浏览器、边缘运行时或 curl 调用](#从浏览器边缘运行时或-curl-调用)
5. [使用 CLI 实时探测与调试在线服务](#5-使用-cli-实时探测与调试在线服务)
6. [无状态负载均衡 vs. 有状态实例定向调用 (`targeted`)](#6-无状态负载均衡-vs-有状态实例定向调用-targeted)
7. [生产环境高可用：三层超时、自动重试、幂等去重与优雅停机](#7-生产环境高可用三层超时自动重试幂等去重与优雅停机)
   - [Router 停机、容量限制与指标](#router-停机容量限制与指标)
8. [安全鉴权、ACL 权限控制与临时范围 Token](#8-安全鉴权acl-权限控制与临时范围-token)
   - [匿名调用方与 API Key](#匿名调用方与-api-key)
9. [拦截器（Interceptors）、元数据（Metadata）与结构化错误处理](#9-拦截器interceptors元数据metadata与结构化错误处理)
   - [OpenTelemetry 链路追踪](#opentelemetry-链路追踪)
10. [AI 编程助手集成（`meshcall init`）](#10-ai-编程助手集成meshcall-init)

---

## 1. Router 优先架构与四层运行时

**核心建议：** 无论是在单机开发还是分布式生产环境中，**始终通过 Go Router（`meshcall-router` 或 `LocalRouter`）运行 MeshCall**。由于预编译的 Router 二进制已直接内置在 SDK 安装包中，本地启动 Router 无需任何额外安装。

为什么在所有场景下都应该通过 Router 通信？

1. **单个节点可同时承担服务端与客户端角色** —— 在传统的点对点 RPC 中，每个服务都必须分配并监听自己的入站端口，调用方也必须维护所有对端节点的地址。而在 Router 模式下，全网只有一个统一路由入口（如 `ws://127.0.0.1:8765`）。任何一个 Python、TypeScript 或 Go 进程只需通过 `Mesh` 主动连接 Router，挂载自身提供的 `@service`，即可同时使用同一个 `Mesh` 调用网格中的任何其他服务。
2. **原生 CLI 与 AI 实时可观测** —— 只要通过 Router 运行，你和 AI 编程助手随时都可以使用 `meshcall router inspect` 查看全网服务契约，并用 `meshcall call` 直接发起调用调试。
3. **平滑扩缩容与高可用** —— 随时拉起第二个实例即可自动获得负载均衡、超时预算执行、熔断保护与无损滚动停机，业务代码零改动。

### 第 2 层与第 3 层：`Router`、`LocalRouter` 与 `Mesh`

MeshCall 将**路由平面在哪里**（第 2 层）与**你的连接和运行时平面**（第 3 层）清晰解耦：

- **第 2 层（`Router` / `LocalRouter`）**：
  - `Router("ws://127.0.0.1:8765")` 指向外部 `meshcall-router`（省略时读取 `MESHCALL_ROUTER_URL`，回退 `ws://127.0.0.1:8765`）。它不执行任何网络 I/O，也绝不会隐式拉起子进程。
  - `LocalRouter(port=0)` 显式拉起并持有一个本地 `meshcall-router` 子进程，适用于测试、演示或单机自包含编排。
- **第 3 层（`Mesh`）**：
  - `Mesh(router, ...)` 持有你的多路复用连接、鉴权凭据、重试/重连策略与客户端拦截器。
  - `mesh.registry`（`list`、`describe`）与 `mesh.auth`（`whoami`、`issue_token`、`revoke_token`）提供对 Router 内置发现服务与 Token 服务的强类型访问。

```python
from meshcall import Mesh, Router, RouterScope

router = Router("ws://127.0.0.1:8765")
async with Mesh(router) as mesh:
    services = await mesh.registry.list()
    info = await mesh.registry.describe("chat.v1.Chat")
    me = await mesh.auth.whoami()
    issued = await mesh.auth.issue_token(
        scopes=[RouterScope(service="chat.v1.Chat", methods=["send"])],
        ttl_seconds=900,
    )
    await mesh.auth.revoke_token(issued.token_id)
```

```typescript
import { Mesh, Router } from "meshcall";

await using mesh = await Mesh.connect(new Router("ws://127.0.0.1:8765"));
const services = await mesh.registry.list();
const info = await mesh.registry.describe("chat.v1.Chat");
const me = await mesh.auth.whoami();
const issued = await mesh.auth.issueToken(
  [{ service: "chat.v1.Chat", methods: ["send"] }],
  900,
);
await mesh.auth.revokeToken(issued.token_id);
```

```go
router := meshcall.NewRouter("ws://127.0.0.1:8765")
mesh, err := meshcall.ConnectMesh(ctx, router)
if err != nil {
    return err
}
defer func() { _ = mesh.Close() }()

services, err := mesh.Registry().List(ctx)
if err != nil {
    return err
}
info, err := mesh.Registry().Describe(ctx, "chat.v1.Chat")
if err != nil {
    return err
}
me, err := mesh.Auth().Whoami(ctx)
if err != nil {
    return err
}
issued, err := mesh.Auth().IssueToken(ctx,
    []meshcall.RouterScope{{Service: "chat.v1.Chat", Methods: []string{"send"}}}, 900)
if err != nil {
    return err
}
revoked, err := mesh.Auth().RevokeToken(ctx, issued.TokenID)
if err != nil {
    return err
}
_, _, _ = services, info, me
_, _ = issued, revoked
```

### 挂载与启动服务：`mesh.mount`、`mesh.run` 与 `mesh.serve`

通过 `mesh.mount(...)` 将一个或多个 `@service` 实例挂载到 `Mesh` 上，随后启动服务：

- `mesh.run()`（Go `mesh.Run()`）是生产环境阻塞式入口：它连接 `Router`，注册所有已挂载的服务，并在收到 `SIGINT` / `SIGTERM` 时优雅排空进行中的调用后退出。
- `mesh.serve()`（TypeScript `mesh.startServing()`，Go `mesh.StartServing(ctx)`）是面向测试与多服务脚本的异步构件：配合 `LocalRouter(port=0)` 即可在临时端口上运行自包含的服务网格。

```python
from meshcall import Mesh, Router
from my_app.service import MyService

if __name__ == "__main__":
    mesh = Mesh(Router())
    mesh.mount(MyService(), instance_id="worker-py-1")
    mesh.run()
```

```python
from meshcall import LocalRouter, Mesh

async with LocalRouter(port=0) as router:
    mesh = Mesh(router)
    mesh.mount(MyService())
    async with mesh.serve() as server:
        print(router.url)               # ws://127.0.0.1:<实际端口>
        ...
# 退出上下文即排空进行中的调用，并停止 LocalRouter
```

```typescript
import { Mesh, Router } from "meshcall";

const mesh = new Mesh(new Router());
mesh.mount(new MyService(), { instanceId: "worker-ts-1" });
await mesh.serve();
```

```typescript
import { LocalRouter, Mesh } from "meshcall";

await using router = await LocalRouter.start({ port: 0 });
const mesh = new Mesh(router);
mesh.mount(new MyService());
await using server = await mesh.startServing();
console.log(router.url);
```

```go
// 阻塞式生产入口：收到 SIGINT / SIGTERM 后先排空再返回。
mesh := meshcall.NewMesh(meshcall.NewRouter())
mesh.Mount(NewMyService(), meshcall.WithInstanceID("worker-go-1"))
if err := mesh.Run(); err != nil {
    log.Fatal(err)
}

// 配合内嵌 LocalRouter 的非阻塞形式：
router, err := meshcall.StartLocalRouter(ctx, meshcall.WithPort(0))
if err != nil {
    return err
}
defer func() { _ = router.Stop() }()

mesh := meshcall.NewMesh(router)
mesh.Mount(NewMyService())
srv, err := mesh.StartServing(ctx)
if err != nil {
    return err
}
defer func() { _ = srv.Close() }()      // Close 会先排空
```

`mesh.mount` 接受服务端实例级选项（`instance_id`、`max_inflight`、`strict`、`interceptors`）。

### 本地传输：Unix 套接字与命名管道

所有节点都在同一台机器上时可以不走 TCP：Router 监听 Unix 套接字（Linux、macOS、FreeBSD）或命名管道（Windows），只有 Router 自身的用户（命名管道另加 SYSTEM 与 Administrators）可以访问。帧与行为完全一致。

```bash
meshcall-router --unix-socket /tmp/mesh.sock               # Windows：--pipe mesh
meshcall --router unix:///tmp/mesh.sock router inspect     # Windows：pipe://mesh
```

- **Python**：`Router(unix_path="/tmp/mesh.sock")` 或 `Router(pipe="mesh")`（或 `LocalRouter(unix_path=...)`）—— 传给 `Mesh(router)`。
- **TypeScript**：`new Router({ unixPath: "/tmp/mesh.sock" })` 或 `new Router({ pipe: "mesh" })`（或 `LocalRouter.start({ unixPath: ... })`）。
- **Go** 与 CLI：端点字符串 `unix:///tmp/mesh.sock` 与 `pipe://mesh`（`meshcall.NewRouter(meshcall.WithUnixSocket(path))` / `meshcall.WithPipe(name)`）。

---

## 2. 服务契约编写规范与严格模式 (`MESHCALL_STRICT=1`)

**核心优势：** MeshCall 的契约不仅包含字段类型 Schema，还携带**方法语义注解**（`read_only`、`idempotent`、`destructive`、`deprecated`、`description`）。客户端重试策略会根据 `idempotent`/`read_only` 自动判断哪些方法断线后可以安全重试；AI 助手也会根据 `description` 与语义注解安全调用工具。

### 核心设计准则

1. **服务名必须包含版本段**：推荐格式为 `<命名空间>.v<主版本>.<服务名>`（例如 `billing.v1.InvoiceService`）。未写版本号的名称（如 `chat`）会被自动规范化为 `chat.v0`。
2. **函数参数即请求体（Parameters are the request）**：不要把所有参数包进一个 `Request` 类里。直接按自然习惯写多个函数参数（如 `session_id: str, limit: int = 20`），MeshCall 会自动合成请求模型。
3. **返回值必须是结构化模型**：始终返回 Pydantic `BaseModel`（Python）、Zod 对象 Schema（TypeScript）或 struct（Go），不要直接返回裸的标量，以便契约未来平滑演进。
4. **开发与 CI 阶段开启 `MESHCALL_STRICT=1`**：严格模式会将契约告警（`W001`–`W006`）转化为启动阶段的立即报错，并附带 `hint` 提示具体修复方法。

### 方法声明：`@query`、`@mutates`、`@stream`

方法装饰器一次性声明语义；同一份契约同时驱动校验、重试、`meshcall router inspect` 与生成的客户端。

```python
from collections.abc import AsyncIterator
from meshcall import mutates, query, service, stream


@service("billing.v1.Invoices")
class Invoices:
    """发票操作。"""

    @query
    async def get(self, invoice_id: str) -> Invoice:
        """读取单张发票（只读且幂等）。"""
        ...

    @mutates(idempotent=True)
    async def rename(self, invoice_id: str, title: str) -> Invoice:
        """修改标题；重复执行同一调用是安全的。"""
        ...

    @mutates(destructive=True)
    async def delete(self, invoice_id: str) -> Deleted:
        """删除发票（没有幂等键时绝不自动重试）。"""
        ...

    @stream
    async def export(self, account_id: str) -> AsyncIterator[Row]:
        """流式导出；普通 `@stream` 方法默认只读。"""
        ...
```

| 装饰器 | 业务含义 | `RetryPolicy` 是否自动重试？ |
| --- | --- | --- |
| `@query` | 纯读取查询；隐含 `read_only=True, idempotent=True` | **是** |
| `@mutates(idempotent=True)` | 会修改状态，但使用相同参数重复执行多次的结果完全一致 | **是** |
| `@mutates(destructive=True)` 或裸 `@mutates` | 不可直接重复执行的写操作（如扣款、追加日志） | **否**（除非调用方显式传入了 `idempotency_key`） |
| `@stream` | 服务端流；默认只读/幂等，可用 `read_only=False` / `idempotent=False` 覆盖；名字看起来像写操作（`delete_*` 等）的方法不会获得默认值 | 流从不重试 |
| `deprecated="use_v2"` | 将方法标记为废弃，生成的客户端会带上 `@deprecated` 编译器警告 | 取决于读写注解 |

TypeScript 使用同样的形状，并显式给出 Zod Schema：

```typescript
import { mutates, query, service, stream, z } from "meshcall";

const Invoice = z.object({ id: z.string(), title: z.string() }).meta({ id: "Invoice" });

@service("billing.v1.Invoices", { description: "发票操作。" })
class Invoices {
  @query({ description: "读取单张发票。", input: { invoice_id: z.string() }, output: Invoice })
  async get(invoice_id: string) { ... }

  @mutates({ description: "删除发票。", input: { invoice_id: z.string() }, output: Invoice, destructive: true })
  async delete(invoice_id: string) { ... }

  @stream({ description: "流式导出。", input: { account_id: z.string() }, output: z.object({ line: z.string() }) })
  async *export(account_id: string) { ... }
}
```

Go 服务既可以通过结构体标签（`meshcall.ServiceMeta` 与 `meshcall.MethodMeta`）声明，也可以通过 `HandleUnary`、`HandleServerStream`、`HandleClientStream` 与 `HandleDuplex` 注册：

```go
chat := meshcall.NewService("chat.v1.Chat", meshcall.Describe("Chat sessions."))

meshcall.HandleUnary(chat, "send", func(ctx *meshcall.Context, req SendRequest) (Message, error) {
    return Message{Text: "echo " + req.Text}, nil
}, meshcall.Doc("Append a message and return the reply."), meshcall.Idempotent())
```

### 依赖注入与 `meta()`

Handler 可以声明服务端专有依赖，而不把这些参数写进契约。依赖提供者在**每次调用中最多执行一次**，并在该调用内缓存；如果提供者是生成器（`yield`），调用结束时会执行清理逻辑，因此“成功提交 / 失败回滚”可以自然表达。被注入的参数——依赖、`meta("key")` 值、`ctx: RpcContext` 与 `logger: RpcLogger`——都会从契约与所有生成的客户端中剔除。

```python
from collections.abc import AsyncIterator
from typing import Annotated
from meshcall import dependency, meta, mutates

@dependency
async def transaction() -> AsyncIterator[SessionUnit]:
    """每个调用一个工作单元：成功提交，出错或取消则丢弃。"""
    unit = SessionUnit(store)
    try:
        yield unit
    except BaseException:
        await unit.rollback()
        raise
    else:
        await unit.commit()

Tx = Annotated[SessionUnit, transaction._provider()]   # 或 Annotated[T, dependency(provider)]
CallerId = Annotated[str | None, meta("x-caller-id")]

@mutates(idempotent=True)
async def send(self, session_id: str, text: str, *, tx: Tx, caller: CallerId) -> Message:
    """`tx` 与 `caller` 只在服务端注入。"""
    ...
```

依赖也可以继续声明依赖或元数据：

```python
@dependency
async def current_user(authorization: Annotated[str, meta("authorization")]) -> str:
    return authorization.removeprefix("Bearer ")
```

TypeScript 用 `dependency(provider, { inject })` 声明依赖，并通过方法的 `inject` 注入：

```typescript
import { dependency, meta, mutates } from "meshcall";

const CurrentUser = dependency(
  async function* (authorization: string | undefined) {
    yield authorization?.replace(/^Bearer\s+/, "") ?? "";
  },
  { inject: [meta("authorization")] },
);

@mutates({
  description: "追加一条消息。",
  input: { session_id: z.string(), text: z.string() },
  output: Message,
  inject: [CurrentUser],
})
async send(session_id: string, text: string, user: string) {
  return { text: `${user}: ${text}` };
}
```

Go 在 `mesh.Mount` 时通过 `WithDependencies` 一次性注册提供者；参数类型已注册的 Handler 参数会被注入并从契约中移除：

```go
type CallerID string

func NewWorkspace() *Workspace { return &Workspace{} }

type Workspace struct {
    meshcall.ServiceMeta `name:"workspace.v1.Workspace" description:"Remote workspace."`
}

func (w *Workspace) WriteFile(ctx *meshcall.Context, path, content string, caller CallerID) (FileStat, error) {
    return FileStat{Path: path, Bytes: len(content), Caller: string(caller)}, nil
}

mesh := meshcall.NewMesh(meshcall.NewRouter())
mesh.Mount(NewWorkspace(), meshcall.WithDependencies(
    meshcall.Meta[CallerID]("x-caller-id", meshcall.WithMetaDefault(CallerID("anonymous"))),
))
if err := mesh.Run(); err != nil {
    log.Fatal(err)
}
```

`meta()` 读取调用元数据（按大小写不敏感匹配，符合 HTTP 头的习惯）；它用于网关、CLI 身份等传输层信息，而不是业务参数。

---

## 3. 三种核心调用模式：Unary、Server Stream、Client Stream

**核心优势：** 流式调用按方向独立维护**条目信用窗口（Item Credit，初始窗口为 16）**。当消费端处理较慢时，生产端会自动等待窗口授信，绝不会在内存中无限堆积数据；同时单连接上的多个并发流按 `call_id` 公平轮转发送，大流量流不会饿死其他普通调用。

三种运行时遵循同一条规则：**普通业务参数在前，客户端流的入站迭代器在最后**。生成的客户端 1:1 镜像这个参数列表。

### Python 服务端与客户端范例

```python
from collections.abc import AsyncIterator
from pydantic import BaseModel
from meshcall import RpcContext, RpcInputStream, RpcLogger, mutates, query, service, stream


class Chunk(BaseModel):
    text: str


class Summary(BaseModel):
    count: int
    combined: str


@service("text.v1.TextProcessor")
class TextProcessor:
    """提供文本一元处理、服务端流分词与客户端流聚合。"""

    # 1. 一元调用 (Unary): 普通参数 -> 响应模型
    @query
    async def echo(self, text: str, ctx: RpcContext) -> Chunk:
        """原样返回输入文本，并通过 Trailer 附带字符长度。"""
        ctx.set_trailer("length", str(len(text)))
        return Chunk(text=text)

    # 2. 服务端流 (Server Stream): 普通参数 -> AsyncIterator[条目模型]
    @stream
    async def tokenize(self, sentence: str, logger: RpcLogger) -> AsyncIterator[Chunk]:
        """将句子按空格切分并逐词流式推送给客户端。"""
        logger.info("tokenizing sentence")
        for word in sentence.split():
            yield Chunk(text=word)

    # 3. 客户端流 (Client Stream): 普通参数在前、入站流在后；用 @mutates 声明
    @mutates(idempotent=True)
    async def join_chunks(
        self,
        separator: str,
        items: RpcInputStream[Chunk],
    ) -> Summary:
        """接收客户端持续发送的文本块流，并以 separator 拼接后返回汇总结果。"""
        parts: list[str] = []
        async for chunk in items:
            parts.append(chunk.text)
        return Summary(count=len(parts), combined=separator.join(parts))
```

**在生成的 Python 客户端中调用这三种模式：**

```python
from meshcall import Mesh, Router
from text_client import TextProcessorClient

async with Mesh(Router("ws://127.0.0.1:8765")) as mesh:
    client = TextProcessorClient(mesh)

    # 1. 一元调用
    chunk = await client.echo("hello")

    # 2. 服务端流（如果可能提前 break 退出循环，建议使用 `async with` 以自动通知服务端取消流）
    async with client.with_options(timeout=10.0, idle_timeout=3.0).tokenize("one two three") as stream:
        async for item in stream:
            print(item.text)

    # 3. 客户端流：生成的客户端方法最后一个参数是 AsyncIterable
    async def chunks() -> AsyncIterator[Chunk]:
        yield Chunk(text="a")
        yield Chunk(text="b")

    summary = await client.with_options(timeout=10.0).join_chunks("-", chunks())
    print(summary.combined)  # "a-b"
```

### TypeScript 服务端与客户端范例

```typescript
import { Mesh, mutates, query, Router, service, stream, z } from "meshcall";

const Chunk = z.object({ text: z.string() }).meta({ id: "Chunk" });
const Summary = z.object({ count: z.int(), combined: z.string() }).meta({ id: "Summary" });

@service("text.v1.TextProcessor", { description: "提供文本一元处理、服务端流分词与客户端流聚合。" })
class TextProcessor {
  @query({ description: "原样返回输入文本。", input: { text: z.string() }, output: Chunk })
  async echo(text: string) {
    return { text };
  }

  @stream({ description: "将句子逐词流式推送。", input: { sentence: z.string() }, output: Chunk })
  async *tokenize(sentence: string) {
    for (const word of sentence.split(" ")) yield { text: word };
  }

  @mutates({
    description: "接收客户端文本块流并拼接。",
    input: { separator: z.string().default(" ") },
    streamInput: Chunk,
    output: Summary,
    idempotent: true,
  })
  async join_chunks(separator: string, items: AsyncIterable<{ text: string }>) {
    const parts: string[] = [];
    for await (const chunk of items) parts.push(chunk.text);
    return { count: parts.length, combined: parts.join(separator) };
  }
}

const mesh = new Mesh(new Router("ws://127.0.0.1:8765"));
mesh.mount(new TextProcessor());
await mesh.serve();
```

```typescript
import { Mesh, Router } from "meshcall";
import { TextProcessorClient } from "./generated/text-ts/src/index.js";

await using mesh = await Mesh.connect(new Router("ws://127.0.0.1:8765"));
const client = new TextProcessorClient(mesh);

const chunk = await client.echo("hello");
for await (const item of client.withOptions({ timeoutMs: 10_000, idleTimeoutMs: 3_000 }).tokenize("one two three")) {
  console.log(item.text);
}
async function* chunks() { yield { text: "a" }; yield { text: "b" }; }
const summary = await client.join_chunks("-", chunks());
console.log(summary.combined); // "a-b"
```

### 双向流（duplex）：最后的选择

duplex 方法同时流式传输输入与输出。只有当输出必须与仍在不断到达的输入在同一调用内交错时才使用它，例如用户还在说话时就回应的语音轮次。它从不重试、不去重，不接受幂等键，不能标注 `idempotent` / `read_only`，不能通过 HTTP 调用，并在整个生命周期内固定在一个实例上；`reason` 会写入契约。只发送不读取的一方会在对方信用耗尽后卡住，因此请在发送的同时并发读取输出。请先尝试“服务端流 + 一元调用”或客户端流。

- **Python**：`@mutates(reason="…")` 加一个 `channel: DuplexChannel[In, Out]` 参数；Handler 返回 `None`。
- **TypeScript**：`@mutates({ description, reason, input, streamInput: InSchema, streamOutput: OutSchema })`（或 `channel: { input, output }`），Handler 接收 `channel: DuplexChannel<In, Out>` 且不返回值。
- **Go**：`meshcall.HandleDuplex(svc, name, reason, handler)`，最后一个参数为 `*meshcall.DuplexChannel[In, Out]`。

详见[协议规范：双向流调用](protocol.md#双向流调用duplex-calls)。

---

## 4. 生成与使用跨语言强类型客户端

**核心优势：** 统一的 Go CLI（`meshcall generate` 与 `meshcall export`）支持直接传入一个或多个 Python（`module[:Class]` / `file.py[:Class]`）、TypeScript（`service.ts[:Class]`）、Go（`./pkg[:Struct]`）源文件或 `.meshcall.json` 契约文件，在内部拉起临时的只读（inspect-only）Router 收集标准中间契约，并一键生成 Python、TypeScript 或 Go 客户端。省略 `:Service` 时会自动发现源文件中的全部服务；传入多个服务时会将所有客户端类渲染到同一个输出文件或包中，并自动去重共享模型。默认输出单文件，传入 `--package` 则生成可安装的包目录。**生成的客户端不仅校验入参，还会自动校验服务端返回的响应与流条目**，并且方法签名与服务端业务参数 1:1 镜像（生成的 TypeScript 与 Go 方法还会额外接受一个可选的尾部 options/opts 参数）。

```bash
# 1. 从 Python 服务 -> 生成单文件 Python 客户端（默认模式）
uv run meshcall generate my_app.service:TextProcessor \
  --output src/my_app/text_client.py

# 2. 从 Python 服务 -> 生成可安装的 Python uv 客户端包（--package）
uv run meshcall generate my_app.service:TextProcessor \
  --package \
  --output generated/text-py

# 3. 从 Python 服务 -> 生成 TypeScript Yarn/npm 客户端包（--package）
uv run meshcall generate my_app.service:TextProcessor \
  --language typescript \
  --package \
  --output generated/text-ts

# 4. 同时从多个跨语言源文件（省略 :Service 时自动发现全部服务）导出契约或生成聚合客户端
meshcall export my_app/service.py src/service.ts ./go_svc --output contract.meshcall.json
meshcall generate my_app/service.py src/service.ts ./go_svc \
  --language python \
  --output generated/all_clients.py

# 5. 生成 Go 客户端：单个文件 <output>/client.go（或你指定的 .go 路径）
meshcall generate contract.meshcall.json \
  --language go \
  --package-name textclient \
  --output internal/textclient
```

生成的客户端包装一个 `Mesh` 对象（Python 中为 `TextProcessorClient(mesh)`，TypeScript 中为 `new TextProcessorClient(mesh)`，Go 中为 `textclient.NewTextProcessorClient(mesh)`）。传入同一个 `Mesh` 的多个生成客户端共享同一条多路复用连接。

```python
from meshcall import Mesh, Router
from text_client import TextProcessorClient

async with Mesh(Router("ws://127.0.0.1:8765")) as mesh:
    text = TextProcessorClient(mesh)
    chunk = await text.echo("hello")
```

```typescript
import { Mesh, Router } from "meshcall";
import { TextProcessorClient } from "./generated/text-ts/src/index.js";

await using mesh = await Mesh.connect(new Router("ws://127.0.0.1:8765"));
const client = new TextProcessorClient(mesh);
const chunk = await client.echo("hello");
```

```go
mesh := meshcall.NewMesh(meshcall.NewRouter())               // MESHCALL_ROUTER_URL
defer func() { _ = mesh.Close() }()
client := textclient.NewTextProcessorClient(mesh)
chunk, err := client.Echo(ctx, "hello")
_, _ = chunk, err
```

### 单次调用选项 `with_options`

控制参数从不进入业务方法签名。链式设置一次，然后照常调用方法（生成的 TypeScript 与 Go 方法也接受一个可选的尾部 options/opts 参数；`with_options` 让三种运行时的调用写法保持一致）：

```python
client = TextProcessorClient(mesh).with_options(
    timeout=5.0,                 # 整个调用的时间预算（秒）
    idle_timeout=2.0,            # 流条目之间的最大间隔
    metadata={"trace-id": "abc"},
    idempotency_key="op-1",      # 让写操作的一元调用可安全重试
    instance="worker-1",         # 仅定向服务需要
)
chunk = await client.echo("hello")
```

```typescript
const client = new TextProcessorClient(mesh).withOptions({
  timeoutMs: 5_000,
  idleTimeoutMs: 2_000,
  metadata: { "trace-id": "abc" },
  idempotencyKey: "op-1",
  instance: "worker-1",
});
const chunk = await client.echo("hello");
```

```go
client := textclient.NewTextProcessorClient(mesh).WithOptions(meshcall.CallOptions{
    Timeout:        5 * time.Second,
    IdleTimeout:    2 * time.Second,
    Metadata:       map[string]string{"trace-id": "abc"},
    IdempotencyKey: "op-1",
    Instance:       "worker-1",
})
chunk, err := client.Echo(ctx, "hello")
_, _ = chunk, err
```

定向服务的生成客户端把 `instance` 作为编译期必填项（Python 的 keyword-only `instance`、TypeScript 的必填选项、Go 的位置参数），其余选项走 `with_options`。

### 从浏览器、边缘运行时或 curl 调用

浏览器无法在 WebSocket 上设置 `Authorization` 头，因此 Web 与边缘代码调用 Router 的 HTTP API（同一端口，`/v1/` 路径下）。生成的 TypeScript 客户端可以直接接收来自 `meshcall/web`（不依赖 Node）的 `Mesh`（使用 `new Router("https://...")` 构造）；一元调用与服务端流均可用，duplex 不可用。

```typescript
import { Mesh, Router } from "meshcall/web";
import { TextProcessorClient } from "./generated/text-ts/src/index.js";

const mesh = new Mesh(new Router("https://router.example"), { token });
const client = new TextProcessorClient(mesh);
const chunk = await client.withOptions({ timeoutMs: 5_000 }).echo("hello");
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

**核心优势：** 无状态服务可通过多种策略按方法自动均衡流量；而有状态服务（如加载了特定权重的 GPU 推理节点、内存游戏房间、Actor 实例）则可通过 `targeted=True` 强制按 `instance_id` 精确寻址。

### 无状态负载均衡策略

可在每个方法上单独指定策略（默认为 `round_robin` 轮询）：

```python
from meshcall import Balance, mutates, query, service

@service("compute.v1.Worker")
class Worker:
    """无状态计算与会话亲和计算服务。"""

    # 选择当前正在处理的并发请求数（in-flight）最少的实例
    @query(balance=Balance.least_inflight())
    async def heavy_compute(self, payload: str) -> Result:
        """将耗时计算调度给最空闲的节点。"""
        ...

    # 按请求中的 session_id 字段哈希路由到固定实例（优先命中本地内存缓存）
    @mutates(idempotent=True, balance=Balance.sticky("session_id"))
    async def step_session(self, session_id: str, prompt: str) -> Result:
        """在保持会话亲和性的前提下处理请求。"""
        ...
```

### 有状态实例定向服务 (`targeted=True`)

当每个服务实例承载不同状态时（例如 `gpu-worker-0` 加载了模型 A，`gpu-worker-1` 加载了模型 B），在服务上声明 `targeted=True`：
- 若调用方未指定 `instance_id`，Router 会在路由前直接拒绝并返回 `invalid_argument`（`reason: "instance_required"`）。
- **编译期防呆**：生成的 Python 与 TypeScript 客户端会将 `instance` 生成为**必填参数**，一旦漏传，IDE 和类型检查器（`pyright` / `tsc`）在编写代码阶段就会报错！

```python
from meshcall import RpcContext, query, service

@service("gpu.v1.ModelRunner", targeted=True)
class ModelRunner:
    """绑定特定 GPU 显存状态的推理节点。"""

    @query
    async def infer(self, prompt: str, ctx: RpcContext) -> Output:
        """在当前指定的 GPU 实例上执行推理。"""
        return Output(text=f"Processed by {ctx.instance_id}: {prompt}")
```

调用定向服务：

```python
# 生成的客户端要求显式传入 instance= 参数：
res = await client.with_options(timeout=10.0).infer("Hello!", instance="gpu-worker-1")
```

```typescript
// 生成的 TypeScript 方法要求在选项里传入 instance：
const res = await client.infer("Hello!", { instance: "gpu-worker-1", timeoutMs: 10_000 });
```

```go
// 生成的 Go 方法紧跟 ctx 之后接收 instance：
res, err := client.Infer(ctx, "gpu-worker-1", "Hello!")
_, _ = res, err
```

或者通过 CLI 指定 `--instance`：

```bash
uv run meshcall call gpu.v1.ModelRunner.infer '{"prompt": "Hello!"}' --instance gpu-worker-1
```

---

## 7. 生产环境高可用：三层超时、自动重试、幂等去重与优雅停机

**核心优势：** 无需在 Router 中引入数据库或状态同步，即可实现超时兜底、自动故障转移、重复请求去重回放与零中断滚动发版。

### 1. 三层相对超时机制（无需机器间时钟同步）

调用时通过 `with_options(timeout=…)` / `withOptions({ timeoutMs })` / `WithOptions(CallOptions{Timeout: …})` 设置超时：
- **第一层（Server）**：在 `T` 时刻触发，立即取消业务协程并返回 `deadline_exceeded`（`source: "server"`）。
- **第二层（Router）**：在 `T + 500ms` 触发，防止某个服务实例假死卡住连接，自动向客户端返回 `deadline_exceeded`（`source: "router"`）并回收路由。
- **第三层（Client）**：在 `T + max(1s, 10%)` 触发，防止网络黑洞或 Router 故障导致调用方永久挂起（`source: "client"`）。
- 在服务端 Handler 内部，可在执行耗时子任务前通过 `ctx.remaining()`（Python）或 `ctx.remainingMs()`（TS）查看剩余时间预算。

### 2. 客户端自动重试（`RetryPolicy`）与幂等键（`idempotency_key`）

在 `Mesh` 上配置 `RetryPolicy` 后，当**一元调用**遇到可重试错误（`retryable=True`，如实例断线 `unavailable` 或满载 `resource_exhausted`）且满足以下任一条件时，客户端会自动重发：
1. 方法在契约中标注了 `read_only=True` 或 `idempotent=True`；
2. 或者调用时传入了 `idempotency_key`（如每个业务操作生成一个 UUID）。

当一元调用携带 `idempotency_key` 时，Router 会按 `(service, method, idempotency_key, instance_id)` 自动合并同一时刻的并发重复请求，并在完成后将结果缓存 60 秒以供重试直接回放。

```python
from meshcall import Mesh, RetryPolicy, Router

mesh = Mesh(
    Router("ws://127.0.0.1:8765"),
    retry=RetryPolicy(max_attempts=3, initial_delay=0.05, max_delay=1.0),
)
client = CounterServiceClient(mesh)
```

```typescript
import { Mesh, Router } from "meshcall";

const mesh = new Mesh(new Router("ws://127.0.0.1:8765"), {
  retry: { maxAttempts: 3, initialDelayMs: 50, maxDelayMs: 1000 },
});
const client = new CounterServiceClient(mesh);
```

```go
router := meshcall.NewRouter("ws://127.0.0.1:8765")
mesh := meshcall.NewMesh(router, meshcall.WithRetry(meshcall.RetryPolicy{
    MaxAttempts:  3,
    InitialDelay: 50 * time.Millisecond,
    MaxDelay:     time.Second,
    Multiplier:   2,
    Jitter:       0.2,
}))
client := counterclient.NewCounterServiceClient(mesh)
_ = client
```

### 3. 单实例并发限制（`max_inflight`）与自动熔断

在挂载服务时为有容量上限的服务实例（如数据库连接池、GPU 推理）设置 `max_inflight`，当该实例满载时 Router 会自动将新请求绕行到其他空闲实例：

```python
mesh.mount(Worker(), max_inflight=32)
async with mesh.serve() as server:
    ...
```

```typescript
mesh.mount(new Worker(), { maxInflight: 32 });
await using server = await mesh.startServing();
```

```go
if err := mesh.Mount(NewWorker(), meshcall.WithMaxInflight(32)); err != nil {
    return err
}
srv, err := mesh.StartServing(ctx)
if err != nil {
    return err
}
defer func() { _ = srv.Close() }()
```

当某个实例连续超时或返回 `internal`/`unavailable` 达到阈值（默认连续 5 次）时，Router 的实例级熔断器会自动将其隔离 10 秒，随后放行 1 个半开探测请求（Probe），成功则恢复流量。

### 4. 多 Router 无状态双活容灾

可以同时启动多个独立的 `meshcall-router` 进程，并把同一份地址列表交给传入 `Mesh` 的 `Router` 对象——挂载的服务会同时向**所有** Router 保持注册，客户端会在当前 Router 故障时自动切换到下一个可用 Router：

```python
router = Router(["ws://router-1.internal:8765", "ws://router-2.internal:8765"])
mesh = Mesh(router)
client = CounterServiceClient(mesh)
# 服务端：mesh.mount(Worker()); mesh.run()
```

```typescript
const router = new Router(["ws://router-1.internal:8765", "ws://router-2.internal:8765"]);
const mesh = new Mesh(router);
```

```go
router := meshcall.NewRouter(
    "ws://router-1.internal:8765",
    "ws://router-2.internal:8765",
)
mesh := meshcall.NewMesh(router)
```

### 5. 零中断优雅停机（Graceful Drain）

`mesh.run()` 在收到 `SIGINT` / `SIGTERM` 时会自动排空。使用 `mesh.serve()` / `mesh.startServing()` 时，可通过 `mesh.drain()` 或服务句柄停止（或离开上下文管理器）：

```python
# Python：排空是默认行为；可用 drain_timeout 限定等待时间
await mesh.drain()
await server.stop(drain=True, drain_timeout=30.0)
```

```typescript
// TypeScript
await mesh.drain();
await server.close({ drain: true, drainTimeoutMs: 30_000 });
```

Go：`srv.Close()` 会先排空；也可以在 `srv.Close()` 之前显式调用 `mesh.Drain(ctx)` 或 `srv.Drain(ctx)`。

### Router 停机、容量限制与指标

用 SIGTERM（或 SIGINT）重启 Router 时，它会先排空：`/readyz` 返回 503，新调用收到可重试的 `unavailable`（`reason: "router_draining"`），空闲连接以 1001 关闭，客户端与服务随之转到其他 Router；超过 `--shutdown-timeout`（默认 10 秒）仍在进行的调用以可重试的 `unavailable`（`reason: "router_shutdown"`）结束。第二次信号跳过等待。由 SDK 拉起的 `LocalRouter` 通过关闭 stdin 停止，会立即结束进行中的调用。

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `--max-calls-per-connection` | 1000（`0` 表示不限） | 该连接上超出的调用以可重试的 `resource_exhausted` / `overloaded` 失败，连接保持打开 |
| `--max-frame-size` | 64 MiB（也是最大值） | 单个负载或流条目必须放进一帧；更大的数据请用流发送 |
| `--outbox-size` | 256 MiB | Router 对停止读取的对端最多可欠多少数据，超出即断开 |
| `--metrics-listen` | 关闭 | 在独立监听地址（如 `127.0.0.1:9464`）的 `http://ADDR/metrics` 提供 Prometheus 指标 |

每个参数都有对应的 `MESHCALL_ROUTER_<FLAG>` 环境变量；`LocalRouter(...)` 接受传给本地内嵌子进程的选项（`metrics_listen`、`max_frame_size`、`outbox_size`、`auth_file` 等）。

---

## 8. 安全鉴权、ACL 权限控制与临时范围 Token

**核心优势：** 生产环境下可通过 `router-auth.json` 为不同服务和客户端配置最小权限 ACL（基于 PBKDF2-HMAC-SHA256 哈希），并通过 `mesh.auth` 动态签发带过期时间和精确方法范围的临时 Token。

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

启动 Router 时带上鉴权文件：拉起本地 Router 时传 `LocalRouter(port=8765, auth_file="router-auth.json")`，或直接用二进制 `--auth-file router-auth.json` 启动。

### 2. 动态签发与吊销临时范围 Token

```python
from meshcall import Mesh, Router, RouterCredentials, RouterScope

admin = RouterCredentials(username="backend-app", password="...")
async with Mesh(Router("ws://127.0.0.1:8765"), credentials=admin) as mesh:
    # 签发一个有效期 15 分钟、仅允许调用 `chat.v1.Chat/send` 的受限临时 Token
    issued = await mesh.auth.issue_token(
        scopes=[RouterScope(service="chat.v1.Chat", methods=["send"])],
        ttl_seconds=900,
    )

# 将 `issued.token` 交给下游客户端使用：
downstream = Mesh(Router("ws://127.0.0.1:8765"), token=issued.token)
chat = ChatClient(downstream)
```

`mesh.auth.revoke_token(issued.token_id)`（TypeScript `mesh.auth.revokeToken(tokenId)`，Go `mesh.Auth().RevokeToken(ctx, issued.TokenID)`）可让 Token 在到期前失效。

### 匿名调用方与 API Key

不发送 `Authorization` 头的调用方是保留用户 `anonymous`。它可以连接，但除非鉴权文件为它授予 `call` 规则，否则每个调用都返回 `permission_denied`；只有鉴权文件能让某个方法公开。

```json
{ "username": "anonymous", "roles": ["client"], "call": ["status.v1.Health/*"] }
```

为机器调用方（Webhook、CI、外部服务）发放长期 **API Key**，而不是密码：

```bash
meshcall-router new-api-key ci-bot   # 只打印一次 Key，以及应加入 "api_keys" 的条目
```

填好条目的 `call` 列表后重启 Router。调用方以 `Authorization: Bearer mk_…` 发送 Key：Python 为 `Mesh(router, token="mk_…")`，TypeScript 为 `new Mesh(router, { token: "mk_…" })`，Go 为 `meshcall.NewMesh(router, meshcall.WithToken("mk_…"))`，curl 为 `-H 'Authorization: Bearer mk_…'`。校验 Key 只需一次哈希查找；轮换方式是先加新 Key 再删旧 Key。详见 [Router 架构与鉴权](router.md#连接时认证调用时授权)。

---

## 9. 拦截器（Interceptors）、元数据（Metadata）与结构化错误处理

**核心优势：** 通过拦截器统一注入链路追踪（Trace ID）或鉴权信息；通过强类型异常子类与字段级 `violations` 精准处理异常，告别字符串正则匹配。

### 客户端与服务端拦截器（Python）

```python
from meshcall import ClientCallInfo, ClientCallNext, Mesh, Router, RpcContext, ServerCallNext

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

router = Router("ws://127.0.0.1:8765")
mesh = Mesh(router, interceptors=[trace_client_interceptor])
client = TextProcessorClient(mesh)

mesh.mount(TextProcessor(), interceptors=[audit_server_interceptor])
async with mesh.serve() as server:
    ...
```

### OpenTelemetry 链路追踪

在两端都加上 OpenTelemetry 拦截器。Trace context 作为 W3C `traceparent` / `tracestate` 调用元数据传递，因此同一条链路可以穿过 Router、跨语言延续；每个调用生成一个 `service/method` span 与 `meshcall.calls` / `meshcall.call.duration` 指标。未配置 OpenTelemetry SDK 时它们什么也不做。

```python
# pip install "meshcall[otel]"
from meshcall import Mesh, Router
from meshcall.otel import client_interceptor, server_interceptor

router = Router("ws://127.0.0.1:8765")
mesh = Mesh(router, interceptors=[client_interceptor()])
client = TextProcessorClient(mesh)

mesh.mount(TextProcessor(), interceptors=[server_interceptor()])
async with mesh.serve() as server:
    ...
```

```typescript
// 需要可选的 peer dependency @opentelemetry/api
import { Mesh, Router } from "meshcall";
import { otelClientInterceptor, otelServerInterceptor } from "meshcall/otel";

const mesh = new Mesh(new Router(url), { interceptors: [otelClientInterceptor()] });
mesh.mount(new TextProcessor(), {
  interceptors: [otelServerInterceptor()],
});
await using server = await mesh.startServing();
```

Go 没有 OpenTelemetry 包；`ClientInterceptor` / `ServerInterceptor` 可以自行读写 `traceparent` 元数据键。

### 捕获结构化错误

每个标准协议错误码都对应一个专用的异常子类（`InvalidArgumentError`、`DeadlineExceededError`、`UnavailableError`、`ResourceExhaustedError`、`PermissionDeniedError`、`MethodNotFoundError`、`ProtocolError`、`InternalError`、`CallCancelledError`）：

```python
from meshcall import DeadlineExceededError, InvalidArgumentError, UnavailableError

try:
    await client.with_options(timeout=2.0).add(10, 20)
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

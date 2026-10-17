# MeshCall v0.3 三语言接口设计与实现计划 (DX & API Plan)

本文档记录 MeshCall v0.3 的跨语言开发体验（DX）重塑方案、三语言（Python / TypeScript / Go）标准接口样例、两套标准开发流程以及五阶段落地计划。

---

## 一、 核心设计原则

1. **所写即所调（Server ↔ Client 签名 1:1 镜像，无特殊“参数展开/解包”规则）**
   - 服务端定义什么业务参数列表 `(p1, p2, ...)`、什么顺序，生成的三端客户端（Python / TypeScript / Go）就以完全相同的参数列表和顺序调用：
     - 服务端写 0 个参数 `()` $\rightarrow$ 三端客户端都调 `()`（Go 也不再传空结构体 `Empty{}`）。
     - 服务端写多个参数 `(path, content)` $\rightarrow$ 三端客户端都调 `(path, content)`。
     - 服务端写 1 个结构体/Model 参数 `(profile: WelcomeProfile)` $\rightarrow$ 三端客户端都调 `(profile)`（直接传 `WelcomeProfile`，既不拆散其内部字段，也不在外面套一层合成的 `<Service><Method>Request`）。
   - 核心 RPC 收敛为最纯粹的 **4 种基础形态**：**Unary**、**Server Stream**、**Client Stream**、**Duplex**。
   - **Client Stream** 与 **Duplex** 统一遵循：**普通业务参数在前，流（`chunks`）或双向通道（`channel`）在最后**。
   - `subscription` 与 `Topic` 保留底层实现但降级为 **Experimental（实验特性）**，从核心推荐流程与主示例中移出。
2. **极简样板代码（Zero Boilerplate）与统一的 `Router` 抽象**
   - 服务端一行启动：`meshcall.run(Workspace())` / `await run(new Workspace())` / `meshcall.Run(NewWorkspace())`。默认连接 `MESHCALL_ROUTER_URL`（回退 `ws://127.0.0.1:8765`）；若目标为本地地址且未监听，自动拉起内置 `meshcall-router` 并在退出时优雅排空回收。
   - 客户端一行连接：`WorkspaceClient.connect()` / `workspaceclient.Connect(ctx)`。
   - **统一的 `Router` 对象（服务端配置 + 客户端多路复用连接 + 内置服务调用）**：
     - 传给服务端启动：`meshcall.run(Workspace(), router=router)` 配置 Router 连接、重连、鉴权及本地内嵌网关参数。
     - 传给多个客户端复用连接：`async with Router.connect(...) as router:` 后，多个生成的 Client（`WorkspaceClient(router)`、`BillingClient(router)`）共享同一条底层 WebSocket 连接。
     - 直接调用 Router 内置服务：在 `router` 上直接提供 `list_services`、`describe_service`、`whoami`、`issue_token`、`revoke_token` 强类型方法。
   - 消除重复类型声明（TS 装饰器内联 `input: { ... }` 自动推导参数类型；Go 通过结构体反射自动注册方法与参数，使用 `iter.Seq2[T, error]` 直接 `for range` 消费流）。
3. **轻量依赖注入（DI，参考 [`FastEvents`](https://github.com/t4wefan/FastEvents)）**
   - 支持函数式依赖 `@dependency` 与类提供者 `_provider()`。
   - 在 `@service` 注册阶段预编译参数注入计划（含静态循环依赖检测），每次请求通过 `DependencyScope` 缓存同一依赖实例并在调用结束时执行 `yield` 清理逻辑。
   - **导出契约时自动剔除 DI 参数**，生成的客户端签名只包含纯业务参数。
4. **显式参数优先 + 元数据逃生舱（Explicit Parameters First, Clean Metadata Escape Hatch）**
   - 不鼓励通过 `metadata` 暗中传递业务参数，也不在生成的客户端方法签名末尾平铺 `timeout`、`idle_timeout`、`metadata`、`idempotency_key`、`instance` 等控制参数。
   - 单次调用控制参数统一收敛到客户端链式方法：`.with_options(...)` / `.withOptions(...)` / `.WithOptions(...)`。
   - 当不得不依赖网关透传 Header 等元数据时，服务端通过 DI 的 `meta("x-user-id")`（或在依赖函数中注入 `RpcContext`）按需提取，保持业务方法签名干净。

---

## 二、 三语言标准接口样例

### 1. Python 完整样例

```python
from collections.abc import AsyncIterator
from typing import Annotated
from pydantic import BaseModel

import meshcall
from meshcall import (
    DuplexChannel,
    ReconnectPolicy,
    RetryPolicy,
    Router,
    RouterCredentials,
    RouterScope,
    RpcContext,
    dependency,
    meta,
    mutates,
    service,
    stream,
)
from meshcall.otel import otel_client_interceptor, otel_server_interceptor


# ============================================================================
# 1. 依赖注入 (DI) 与元数据逃生舱 (不出现在生成的 Client 签名中)
# ============================================================================
class Database:
    async def save(self, path: str, content: str) -> int:
        return len(content.encode())

    @classmethod
    def _provider(cls):
        db = cls()
        def _provide() -> Database:
            return db
        return _provide

DB = Annotated[Database, dependency(Database._provider())]

# 函数式依赖：支持依赖链与 yield 生命周期清理
@dependency
async def get_tx(db: DB):
    yield db

# 元数据逃生舱：仅在对接 HTTP 网关 Header 等被动场景下按需提取
# 也可在 @dependency 函数中直接声明 ctx: RpcContext 读取底层上下文
CallerId = Annotated[str | None, meta("x-user-id")]


# ============================================================================
# 2. 数据模型与服务端定义 (4 种核心 RPC 形态，所写即所调)
# ============================================================================
class FileStat(BaseModel):
    path: str
    size: int

class TerminalIn(BaseModel):
    data: str

class TerminalOut(BaseModel):
    output: str


@service(name="workspace")
class Workspace:
    """远程工作区服务。"""

    # ① Unary：db 和 caller_id 由服务端自动注入，客户端只传 (path, content)
    @mutates
    async def write_file(
        self,
        path: str,
        content: str,
        db: DB,
        caller_id: CallerId = None,
    ) -> FileStat:
        """写入文件并返回统计信息。"""
        size = await db.save(path, content)
        return FileStat(path=path, size=size)

    # ② Server Stream：服务端流式产出
    @stream
    async def tail_file(self, path: str, lines: int = 10) -> AsyncIterator[str]:
        """流式读取文件末尾内容。"""
        for i in range(lines):
            yield f"{path}: line {i}\n"

    # ③ Client Stream：普通参数在前，流参数 chunks 在最后
    @mutates
    async def upload_file(
        self,
        path: str,
        chunks: AsyncIterator[bytes],
        tx: Database = get_tx,
    ) -> FileStat:
        """流式上传文件并返回最终大小。"""
        total = 0
        async for chunk in chunks:
            total += len(chunk)
        return FileStat(path=path, size=total)

    # ④ Duplex：普通参数在前，双向通道 channel 在最后
    @mutates
    async def open_terminal(
        self,
        shell: str,
        channel: DuplexChannel[TerminalIn, TerminalOut],
    ) -> None:
        """打开交互式终端会话。"""
        await channel.send(TerminalOut(output=f"started {shell}\n"))
        async for msg in channel:
            if msg.data == "exit":
                break
            await channel.send(TerminalOut(output=f"echo: {msg.data}"))


# ============================================================================
# 3. 服务端启动：极简模式 vs 传入 Router 对象展开配置
# ============================================================================
def start_minimal():
    # 极简一行启动（默认连 ws://127.0.0.1:8765，本地未运行则自动拉起内嵌 Router）
    meshcall.run(Workspace())


def start_expanded():
    # 构造 Router 对象：既可传给服务端启动，也可用于客户端连接与调用内置服务
    router = Router(
        # 连接地址：支持单 URL、多 URL 故障转移列表、unix:///tmp/mesh.sock、pipe://mesh
        url=["ws://127.0.0.1:8765", "ws://127.0.0.1:8766"],
        auth=RouterCredentials.bearer("mk_service_secret"),
        # Peer ↔ Router 链路保活、自动重连与客户端重试策略
        ping_interval=20.0,
        ping_timeout=10.0,
        reconnect=ReconnectPolicy(
            initial_delay=0.1,
            max_delay=5.0,
            multiplier=2.0,
            jitter=0.2,
        ),
        retry=RetryPolicy(max_attempts=3, initial_delay=0.1, max_delay=2.0, multiplier=2.0),
        max_frame_size=64 * 1024 * 1024,  # 64 MiB
        # 若本地自动拉起内嵌 Router，以下参数控制内嵌 Router 行为：
        spawn_if_local=True,
        http_listen="127.0.0.1:8080",     # 开启 HTTP/SSE 网关与 /openapi.json
        http_cors_origins=["https://app.example.com"],
        http_default_timeout=30.0,
        auth_file="/etc/meshcall/auth.json",
        outbox_size=16 * 1024 * 1024,
        max_connections=1024,
    )

    meshcall.run(
        Workspace(),
        router=router,                        # 也可直接传 "ws://..." 或 ["ws://r1:8765", ...]
        # --- Server 实例级配置 ---
        instance_id="workspace-node-1",       # 实例标识，用于指定实例调用与监控
        max_inflight=128,                     # 最大并发请求数（上报 Router 用于负载均衡）
        drain_timeout=10.0,                   # 收到 SIGTERM 后优雅排空进行中请求的超时时间
        strict=True,                          # 严格契约检查（同 MESHCALL_STRICT=1）
        access_log=True,                      # 是否输出每次 RPC 访问日志
        interceptors=[otel_server_interceptor()],
    )


# ============================================================================
# 4. 客户端连接、复用 Router 连接与调用内置服务
# ============================================================================
from generated.workspace_client import WorkspaceClient


async def main():
    # 方式 A：单服务极简连接（内部自动管理 Router 连接）
    # async with WorkspaceClient.connect() as ws: ...

    # 方式 B：显式连接 Router —— 支持直接调用 Router 内置服务，并供多个 Client 复用同一条连接
    async with Router.connect(
        url=["ws://127.0.0.1:8765", "ws://127.0.0.1:8766"],
        auth=RouterCredentials.bearer("mk_client_token"),
        peer_id="cli-caller-1",
        reconnect=ReconnectPolicy(initial_delay=0.1, max_delay=5.0, multiplier=2.0, jitter=0.2),
        retry=RetryPolicy(max_attempts=3, initial_delay=0.1, max_delay=2.0, multiplier=2.0),
        ping_interval=20.0,
        ping_timeout=10.0,
        deadline_grace=2.0,
        interceptors=[otel_client_interceptor()],
    ) as router:
        # 1) 调用 Router 内置服务 (Registry & AuthService)
        services = await router.list_services()
        info = await router.describe_service("workspace")
        me = await router.whoami()
        issued = await router.issue_token(
            scopes=[RouterScope(service="workspace", methods=["*"])],
            ttl_seconds=900,
        )
        await router.revoke_token(issued.token_id)

        # 2) 将 router 传给生成的 Client（可绑定多个不同服务的 Client，复用同一条连接）
        ws = WorkspaceClient(router)

        # ① Unary：参数顺序与服务端 (path, content) 完全一致
        stat = await ws.write_file("/hello.txt", "hello world")

        # 单次调用扩展选项：通过 .with_options() 链式附加，不污染业务函数参数
        stat = await ws.with_options(
            timeout=5.0,
            idempotency_key="write-hello-v1",
            instance="workspace-node-1",
            metadata={"x-user-id": "u_42"},
        ).write_file("/hello.txt", "hello world")

        # ② Server Stream：直接 async for
        async for line in ws.with_options(idle_timeout=3.0).tail_file("/hello.txt", lines=5):
            print(line, end="")

        # ③ Client Stream：普通参数在前，AsyncIterable 在最后
        async def file_chunks() -> AsyncIterator[bytes]:
            yield b"part1-"
            yield b"part2"

        uploaded = await ws.upload_file("/blob.bin", file_chunks())

        # ④ Duplex：普通参数与服务端一致，进入 async with 收发消息
        async with ws.open_terminal("zsh") as term:
            await term.send(TerminalIn(data="ls"))
            async for out in term:
                print(out.output)
                await term.send(TerminalIn(data="exit"))
```

---

### 2. TypeScript 完整样例

```typescript
import { z } from "zod";
import {
  DuplexChannel,
  Router,
  RouterCredentials,
  dependency,
  meta,
  mutates,
  run,
  service,
  stream,
} from "meshcall";
import { otelClientInterceptor, otelServerInterceptor } from "meshcall/otel";

// ============================================================================
// 1. 依赖注入 (DI) 与元数据逃生舱
// ============================================================================
class Database {
  async save(path: string, content: string): Promise<number> {
    return Buffer.byteLength(content);
  }
}

const dbDep = dependency(() => new Database());
const callerIdDep = meta("x-user-id"); // 仅在需要读取网关 Header 时使用

// ============================================================================
// 2. 数据模型与服务端定义 (Schema 一次定义，自动推导 TS 类型)
// ============================================================================
const FileStat = z.object({ path: z.string(), size: z.number().int() });
const TerminalIn = z.object({ data: z.string() });
const TerminalOut = z.object({ output: z.string() });

@service({ name: "workspace", description: "远程工作区服务。" })
export class Workspace {
  // ① Unary：inject 中的依赖按顺序注入在业务参数之后，不出现在 Client 签名中
  @mutates({
    description: "写入文件并返回统计信息。",
    input: { path: z.string(), content: z.string() },
    output: FileStat,
    inject: [dbDep, callerIdDep] as const,
  })
  async writeFile(
    path: string,
    content: string,
    db: Database,
    callerId?: string,
  ) {
    const size = await db.save(path, content);
    return { path, size };
  }

  // ② Server Stream
  @stream({
    description: "流式读取文件末尾内容。",
    input: { path: z.string(), lines: z.number().int().default(10) },
    output: z.string(),
  })
  async *tailFile(path: string, lines = 10) {
    for (let i = 0; i < lines; i++) {
      yield `${path}: line ${i}\n`;
    }
  }

  // ③ Client Stream：普通参数在前，chunks 流在最后
  @mutates({
    description: "流式上传文件并返回最终大小。",
    input: { path: z.string() },
    streamInput: z.instanceof(Uint8Array),
    output: FileStat,
  })
  async uploadFile(path: string, chunks: AsyncIterable<Uint8Array>) {
    let size = 0;
    for await (const chunk of chunks) {
      size += chunk.byteLength;
    }
    return { path, size };
  }

  // ④ Duplex：普通参数在前，双向 channel 在最后
  @mutates({
    description: "打开交互式终端会话。",
    input: { shell: z.string() },
    streamInput: TerminalIn,
    output: TerminalOut,
  })
  async openTerminal(
    shell: string,
    channel: DuplexChannel<z.infer<typeof TerminalIn>, z.infer<typeof TerminalOut>>,
  ) {
    await channel.send({ output: `started ${shell}\n` });
    for await (const msg of channel) {
      if (msg.data === "exit") break;
      await channel.send({ output: `echo: ${msg.data}` });
    }
  }
}

// ============================================================================
// 3. 服务端启动：极简模式 vs 传入 Router 对象展开配置
// ============================================================================
async function startMinimal() {
  // 极简一行启动
  await run(new Workspace());
}

async function startExpanded() {
  const router = new Router({
    url: ["ws://127.0.0.1:8765", "ws://127.0.0.1:8766"], // 或 unixPath: "/tmp/mesh.sock" / pipe: "mesh"
    auth: RouterCredentials.bearer("mk_service_secret"),
    pingIntervalMs: 20_000,
    pingTimeoutMs: 10_000,
    reconnect: {
      initialDelayMs: 100,
      maxDelayMs: 5_000,
      multiplier: 2.0,
      jitter: 0.2,
    },
    maxFrameSize: 64 * 1024 * 1024,
    // 若本地自动拉起内嵌 Router，以下参数控制内嵌 Router 行为：
    spawnIfLocal: true,
    httpListen: "127.0.0.1:8080",
    httpCorsOrigins: ["https://app.example.com"],
    httpDefaultTimeoutMs: 30_000,
    authFile: "/etc/meshcall/auth.json",
    outboxSize: 16 * 1024 * 1024,
    maxConnections: 1024,
  });

  await run(new Workspace(), {
    router, // 也可直接传 "ws://127.0.0.1:8765" 或 ["ws://r1:8765", ...]
    // --- Server 实例级配置 ---
    instanceId: "workspace-node-1",
    maxInflight: 128,
    drainTimeoutMs: 10_000,
    strict: true,
    accessLog: true,
    interceptors: [otelServerInterceptor()],
  });
}

// ============================================================================
// 4. 客户端连接、复用 Router 连接与调用内置服务
// ============================================================================
import { WorkspaceClient } from "./generated/workspace-client.js";

async function main() {
  // 方式 A：单服务极简连接
  // await using ws = await WorkspaceClient.connect();

  // 方式 B：显式连接 Router —— 调用内置服务 + 多 Client 复用同一条连接
  await using router = await Router.connect({
    url: ["ws://127.0.0.1:8765", "ws://127.0.0.1:8766"],
    auth: RouterCredentials.bearer("mk_client_token"),
    peerId: "ts-caller-1",
    reconnect: {
      initialDelayMs: 100,
      maxDelayMs: 5_000,
      multiplier: 2.0,
      jitter: 0.2,
    },
    retry: {
      maxAttempts: 3,
      initialDelayMs: 100,
      maxDelayMs: 2_000,
      multiplier: 2.0,
    },
    pingIntervalMs: 20_000,
    pingTimeoutMs: 10_000,
    deadlineGraceMs: 2_000,
    interceptors: [otelClientInterceptor()],
  });

  // 1) 调用 Router 内置服务 (Registry & AuthService)
  const services = await router.listServices();
  const info = await router.describeService("workspace");
  const me = await router.whoami();
  const issued = await router.issueToken([{ service: "workspace", methods: ["*"] }], 900);
  await router.revokeToken(issued.token_id);

  // 2) 将 router 传给生成的 Client 复用连接
  const ws = new WorkspaceClient(router);

  // ① Unary：位置参数与服务端完全一致
  const stat = await ws.writeFile("/hello.txt", "hello world");

  // 单次调用扩展选项：通过 .withOptions() 链式附加
  const stat2 = await ws
    .withOptions({
      timeoutMs: 5_000,
      idempotencyKey: "write-hello-v1",
      instance: "workspace-node-1",
      metadata: { "x-user-id": "u_42" },
    })
    .writeFile("/hello.txt", "hello world");

  // ② Server Stream
  for await (const line of ws.withOptions({ idleTimeoutMs: 3_000 }).tailFile("/hello.txt", 5)) {
    process.stdout.write(line);
  }

  // ③ Client Stream：普通参数在前，流在最后
  async function* fileChunks() {
    yield new TextEncoder().encode("part1-");
    yield new TextEncoder().encode("part2");
  }
  const uploaded = await ws.uploadFile("/blob.bin", fileChunks());

  // ④ Duplex
  await using term = await ws.openTerminal("zsh");
  await term.send({ data: "ls" });
  for await (const out of term) {
    console.log(out.output);
    await term.send({ data: "exit" });
  }
}
```

---

### 3. Go 完整样例

```go
package main

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/t4wefan/meshcall/meshcall-go"
	"example.com/demo/generated/workspaceclient"
)

// ============================================================================
// 1. 依赖注入 (DI) 与元数据逃生舱
// ============================================================================

type Database struct{}

func (db *Database) Save(path, content string) int { return len(content) }

// 类型级依赖提供者
var dbDep = meshcall.Dependency(func(ctx context.Context) (*Database, error) {
	return &Database{}, nil
})

// 元数据逃生舱：按需提取网关透传的 Header
type CallerID string

var callerIDDep = meshcall.Meta[CallerID]("x-user-id")

// ============================================================================
// 2. 数据模型与服务端定义 (反射自动提取方法与参数，无需手工注册表)
// ============================================================================

type FileStat struct {
	Path string `json:"path"`
	Size int    `json:"size"`
}

type TerminalIn struct {
	Data string `json:"data"`
}

type TerminalOut struct {
	Output string `json:"output"`
}

type Workspace struct {
	meshcall.ServiceMeta `name:"workspace" description:"远程工作区服务。"`
}

func NewWorkspace() *Workspace {
	return &Workspace{}
}

// ① Unary：已注册为 Dependency 的类型（*Database, CallerID）自动注入，不暴露给 Client
func (w *Workspace) WriteFile(
	ctx context.Context,
	path string,
	content string,
	db *Database,
	callerID CallerID,
) (FileStat, error) {
	size := db.Save(path, content)
	return FileStat{Path: path, Size: size}, nil
}

// ② Server Stream：返回 iter.Seq2[T, error]
func (w *Workspace) TailFile(ctx context.Context, path string, lines int) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for i := range lines {
			if !yield(fmt.Sprintf("%s: line %d\n", path, i), nil) {
				return
			}
		}
	}
}

// ③ Client Stream：普通参数在前，chunks 流在最后
func (w *Workspace) UploadFile(
	ctx context.Context,
	path string,
	chunks iter.Seq2[[]byte, error],
) (FileStat, error) {
	total := 0
	for chunk, err := range chunks {
		if err != nil {
			return FileStat{}, err
		}
		total += len(chunk)
	}
	return FileStat{Path: path, Size: total}, nil
}

// ④ Duplex：普通参数在前，DuplexChannel 在最后
func (w *Workspace) OpenTerminal(
	ctx context.Context,
	shell string,
	ch *meshcall.DuplexChannel[TerminalIn, TerminalOut],
) error {
	_ = ch.Send(ctx, TerminalOut{Output: "started " + shell + "\n"})
	for msg, err := range ch.Recv(ctx) {
		if err != nil {
			return err
		}
		if msg.Data == "exit" {
			return nil
		}
		_ = ch.Send(ctx, TerminalOut{Output: "echo: " + msg.Data})
	}
	return nil
}

// ============================================================================
// 3. 服务端启动：极简模式 vs 传入 Router 对象展开配置
// ============================================================================

func startMinimal() error {
	// 极简一行启动
	return meshcall.Run(NewWorkspace(), meshcall.WithDependencies(dbDep, callerIDDep))
}

func startExpanded() error {
	// 构造 Router 对象：既可传给服务端启动，也可用于客户端连接与调用内置服务
	router := meshcall.NewRouter(
		meshcall.WithRouterURLs("ws://127.0.0.1:8765", "ws://127.0.0.1:8766"),
		meshcall.WithAuth(meshcall.BearerToken("mk_service_secret")),
		meshcall.WithPing(20*time.Second, 10*time.Second),
		meshcall.WithReconnect(meshcall.ReconnectPolicy{
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     5 * time.Second,
			Multiplier:   2.0,
			Jitter:       0.2,
		}),
		meshcall.WithMaxFrameSize(64<<20), // 64 MiB
		// 若本地自动拉起内嵌 Router，以下参数控制内嵌 Router 行为：
		meshcall.WithSpawnIfLocal(true),
		meshcall.WithHTTPGateway("127.0.0.1:8080", 30*time.Second, "https://app.example.com"),
		meshcall.WithAuthFile("/etc/meshcall/auth.json"),
		meshcall.WithRouterLimits(16<<20, 1024),
	)

	return meshcall.Run(
		NewWorkspace(),
		meshcall.WithDependencies(dbDep, callerIDDep),
		meshcall.WithRouter(router),
		// --- Server 实例级配置 ---
		meshcall.WithInstanceID("workspace-node-1"),
		meshcall.WithMaxInflight(128),
		meshcall.WithDrainTimeout(10*time.Second),
		meshcall.WithStrict(true),
		meshcall.WithAccessLog(true),
	)
}

// ============================================================================
// 4. 客户端连接、复用 Router 连接与调用内置服务
// ============================================================================

func runClient(ctx context.Context) error {
	// 方式 A：单服务极简连接
	// ws, err := workspaceclient.Connect(ctx)

	// 方式 B：显式连接 Router —— 调用内置服务 + 多 Client 复用同一条连接
	router, err := meshcall.ConnectRouter(
		ctx,
		meshcall.WithRouterURLs("ws://127.0.0.1:8765", "ws://127.0.0.1:8766"),
		meshcall.WithAuth(meshcall.BearerToken("mk_client_token")),
		meshcall.WithPeerID("go-caller-1"),
		meshcall.WithReconnect(meshcall.ReconnectPolicy{
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     5 * time.Second,
			Multiplier:   2.0,
			Jitter:       0.2,
		}),
		meshcall.WithRetry(meshcall.RetryPolicy{
			MaxAttempts:  3,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     2 * time.Second,
			Multiplier:   2.0,
		}),
		meshcall.WithPing(20*time.Second, 10*time.Second),
		meshcall.WithDeadlineGrace(2*time.Second),
	)
	if err != nil {
		return err
	}
	defer router.Close()

	// 1) 调用 Router 内置服务 (Registry & AuthService)
	services, err := router.ListServices(ctx)
	info, err := router.DescribeService(ctx, "workspace")
	me, err := router.Whoami(ctx)
	issued, err := router.IssueToken(ctx, []meshcall.RouterScope{{Service: "workspace", Methods: []string{"*"}}}, 900)
	revoked, err := router.RevokeToken(ctx, issued.TokenID)
	_, _, _, _ = services, info, me, revoked

	// 2) 将 router 传给生成的 Client 复用连接
	ws := workspaceclient.New(router)

	// ① Unary：参数与服务端 (ctx, path, content) 完全一致
	stat, err := ws.WriteFile(ctx, "/hello.txt", "hello world")
	_ = stat

	// 单次调用扩展选项：通过 .WithOptions() 链式附加
	stat, err = ws.WithOptions(meshcall.CallOptions{
		Timeout:        5 * time.Second,
		IdempotencyKey: "write-hello-v1",
		Instance:       "workspace-node-1",
		Metadata:       map[string]any{"x-user-id": "u_42"},
	}).WriteFile(ctx, "/hello.txt", "hello world")

	// ② Server Stream：直接 for range
	for line, err := range ws.WithOptions(meshcall.CallOptions{
		IdleTimeout: 3 * time.Second,
	}).TailFile(ctx, "/hello.txt", 5) {
		if err != nil {
			return err
		}
		fmt.Print(line)
	}

	// ③ Client Stream：普通参数在前，流在最后
	chunks := meshcall.SliceSeq([][]byte{[]byte("part1-"), []byte("part2")})
	uploaded, err := ws.UploadFile(ctx, "/blob.bin", chunks)
	_ = uploaded

	// ④ Duplex
	term, err := ws.OpenTerminal(ctx, "zsh")
	if err != nil {
		return err
	}
	defer term.Close()

	_ = term.Send(ctx, TerminalIn{Data: "ls"})
	for out, err := range term.Recv(ctx) {
		if err != nil {
			return err
		}
		fmt.Println(out.Output)
		_ = term.Send(ctx, TerminalIn{Data: "exit"})
	}
	return nil
}
```

---

## 三、 两套标准开发流程

### 1. 用户使用 MeshCall 开发服务的流程（4 步闭环）
1. **写服务（Write Service as Plain Functions）**：直接定义业务类与方法（Unary / Server Stream / Client Stream / Duplex），数据库、鉴权、或网关 Header（`meta("x-user-id")`）通过 DI 声明在参数中。
2. **一键运行（Zero-Config Run）**：`meshcall.run(Workspace())` 一行启动；本地未起 Router 时自动拉起内嵌 `meshcall-router`，生产环境通过 `MESHCALL_ROUTER_URL` 或传入 `Router(...)` 对象切换。
3. **生成强类型客户端（Generate Client）**：运行 `meshcall generate` 导出契约并生成目标语言客户端（DI 参数自动剥离，业务参数签名 1:1 对齐，不暴露合成的 `<Service><Method>Request` 包装结构体）。
4. **像本地函数一样调用（Connect & Call）**：单服务直接 `WorkspaceClient.connect()`；多服务或需调用 Router 内置服务（`list_services` / `describe_service` / `whoami` / `issue_token` / `revoke_token`）时通过 `Router.connect()` 建立共享连接并传入 `WorkspaceClient(router)`；单次调用超时、幂等键、指定实例或透传 Header 通过 `.with_options(...)` 链式附加。

### 2. MeshCall 库本身的开发与演进流程
1. **契约与协议先行**：涉及协议或契约结构变更时，先更新 `docs/protocol.md` 与 `meshcall-router/internal/contract`。
2. **三端服务端运行时与契约导出同步实现**：在 `meshcall-py`、`meshcall-ts`、`meshcall-go` 同步落地装饰器/反射解析、DI 预编译执行计划与契约导出。
3. **统一代码生成器（Go CLI）**：修改 `meshcall-router/internal/codegen`（Python / TS / Go 生成器），更新 `testdata/*` 黄金文件并执行 `scripts/regenerate.sh`。
4. **一致性与跨语言互操作验证**：通过 `meshcall-router/conformance` 与 `meshcall-py/tests/test_multilang_interop.py` 验证三端互调，跑通 `scripts/check.sh`。

---

## 四、 分阶段实现计划 (Implementation Roadmap)

### Phase 1: 统一的 `Router` 对象与极简启动/连接入口
- [ ] **Python (`meshcall-py`)**：实现统一的 `Router` 类（兼具服务端/客户端配置、多路复用连接管理与内置服务 `list_services` / `describe_service` / `whoami` / `issue_token` / `revoke_token` 调用）、`meshcall.run(*services, router=..., ...)`、`await meshcall.serve(*services, router=..., ...)`（支持本地地址未监听时自动拉起内嵌 Router、bind 竞争回退连接、SIGINT/SIGTERM 优雅排空）。
- [ ] **TypeScript (`meshcall-ts`)**：实现统一的 `Router` 类（含 `Router.connect`、客户端连接复用及内置 `Registry`/`AuthService` 方法）、`run(services, opts)`、`serve(services, opts)`。
- [ ] **Go (`meshcall-go`)**：实现统一的 `Router` 类型（`NewRouter`、`ConnectRouter`、客户端连接复用及内置 `Registry`/`AuthService` 方法）、`meshcall.Run(...)`、`meshcall.Serve(...)`。
- [ ] **Experimental 边界整理**：将 `subscription` 与 `Topic` 标记为实验特性（Experimental），从核心推荐路径与默认主推导出中降级。

### Phase 2: 服务端签名统一与样板消除
- [ ] **Client Stream / Duplex 参数顺序统一**：三端统一支持“普通业务参数在前，流（`chunks`）或双向通道（`channel`）在最后”。
- [ ] **TypeScript 装饰器减负**：提供 `@mutates` / `@query` / `@stream` 简洁装饰器与内联 `input: { ... }` / `output: ...` 定义，免除重复手写参数 TS 类型标注与 `.meta({ id })`。
- [ ] **Go 结构体反射服务定义**：支持直接传入结构体实例（如 `NewWorkspace()`）通过反射提取方法、参数与返回值，支持 `iter.Seq2[T, error]` 服务端流与客户端流，免除手工组装 `Method{}` / `Param{}` 注册表。

### Phase 3: 依赖注入 (DI) 与 `meta` 逃生舱
- [ ] **Python DI**：实现 `@dependency`、`_provider()`、`meta("...")`、`DependencyScope`（请求级缓存 + `yield` 生命周期清理 + 注册期静态循环依赖检测），并在契约导出时自动剔除 DI 参数。
- [ ] **TypeScript DI**：实现 `dependency(...)`、`meta("...")` 与方法装饰器 `inject: [...]` 注入计划及契约剥离。
- [ ] **Go DI**：实现 `meshcall.Dependency(...)`、`meshcall.Meta[T]("...")`、`meshcall.WithDependencies(...)` 类型注入与契约剥离。

### Phase 4: 客户端 `.with_options` 与 Codegen 全面对齐
- [ ] **三端 Client Runtime**：新增 `.with_options(...)`（Python）、`.withOptions(...)`（TS）、`.WithOptions(...)`（Go）链式调用选项；支持 `GeneratedClient(router)` 复用 `Router` 连接与 `GeneratedClient.connect(...)` 极简连接；TS Server Stream 支持直接 `for await`，Client/Duplex 支持 `await using`；Go Stream 支持返回 `iter.Seq2[T, error]` 直接 `for range`。
- [ ] **Go CLI Codegen (`meshcall-router/internal/codegen`)**：
  - 生成 `GeneratedClient.connect(...)` / `Connect(ctx, ...)` 静态入口与接收 `Router` 的构造函数。
  - 三端（含 Go）统一按 `parameters` 生成与服务端 1:1 镜像的纯业务参数签名（不暴露合成的 `<Service><Method>Request` 结构体，移除尾随 `timeout`、`metadata` 等杂项参数；Client Stream / Duplex 普通参数在前、流在后）。
  - 支持 `--naming preserve|idiomatic` 跨语言惯用命名转换。
  - 更新 `internal/codegen/testdata/*` 黄金测试文件。

### Phase 5: Demo、文档与 Agent Skill 刷新
- [ ] 使用新接口重写 `demo/*` 与 `best-practice/*`，执行 `scripts/regenerate.sh`。
- [ ] 更新 `README.md`、`README.zh-CN.md`、`docs/guide.md`、`docs/architecture.md`、`skills/meshcall/SKILL.md` 及 `CHANGELOG.md`。
- [ ] 全量通过 `scripts/check.sh`（`go`、`py`、`ts`、`demos`、`drift`、`conformance`）。

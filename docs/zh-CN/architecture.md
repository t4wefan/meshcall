# 运行时架构设计

[English](../architecture.md) | **简体中文**

## 1. 分层架构概览

```text
Python/Pydantic 服务定义 ---- 契约提取 (export) ----+
                                                    |
TypeScript Schema 服务定义 --- 契约导出 -------------+--> 跨语言可移植契约 (.meshcall.json)
                                                    |       |
Go 结构体标签服务定义 ---- WriteContract -------------+       +--> 生成 Python uv 客户端包
                                                            |
                                                            +--> 生成 TypeScript Yarn 客户端包
                                                            |
                                                            +--> 生成 Go 客户端文件

Python RPC 事件循环 <---- 线程安全桥接 ----> 业务工作线程事件循环 (Service Worker Loop)
        |
        v
逻辑帧连接与按调用公平调度器 (Fair Scheduler)
        |
        +---- WebSocket Direct 直连驱动
        |
        +---- WebSocket Router 路由驱动 ---- 服务实例注册表 (Registry)
```

- **契约层与网络传输解耦**：默认调用形状为一元调用（`unary`），通过 `@query` / `@mutates` / `@stream`（TypeScript 为 `query` / `mutates` / `stream`，Go 为 `HandleUnary` / `HandleServerStream` / `HandleClientStream`）声明调用形状与语义（`read_only`、`idempotent`、`destructive`），这些选项作为 `annotations`、`description` 与 `deprecated` 写入契约。**函数参数即请求体（Parameters are the request）**：多个函数参数会被自动打包为一个合成的请求模型用于校验与传输，模型类型的参数只是其中一个字段。客户端流的入站迭代器排在所有普通业务参数之后。服务名自动规范化为包含一个版本段（如 `chat` → `chat.v0`）。订阅（`@method.subscription` / `method.subscription` / `HandleSubscription`）与 `Topic` 仍保留实现，但在 v0.3 中是**实验特性**：不在推荐路径中，API 可能变化。
- **双向流（duplex）是受限的最后手段**：必须显式声明（`@method.duplex(reason=…)` / `@method.duplex({ reason })` / `HandleDuplex`），Handler 拿到一个通道（`RpcDuplex` / `DuplexChannel`）迭代输入、`send` 输出，不返回值；该调用从不重试、不去重、不能通过 HTTP 调用。详见[协议规范：双向流调用](protocol.md#双向流调用duplex-calls)。
- **依赖注入参数自动剥离**：`@dependency` / `dependency()`、`Annotated[T, dependency(provider)]`、`meta("key")`、`ctx: RpcContext` 与 `logger: RpcLogger` 都属于服务端注入参数，在提取请求契约时会自动剔除，不会出现在生成的客户端参数列表中。注入在每个调用内解析：提供者最多执行一次并按提供者缓存；生成器提供者通过退出栈管理，调用结束时（成功、异常或取消）执行 `yield` 之后的清理逻辑。详见下文「依赖注入作用域」。
- **逻辑帧与底层模式解耦**：运行时核心仅处理逻辑帧（Frame），无需关心底层是 Direct 直连还是经由 Router 转发；连接握手、角色声明、TCP 与本地传输（Unix 套接字或 Windows 命名管道：Python `unix_path=` / `pipe=`，TypeScript `unixPath` / `pipe`）绑定及服务注册均由 Driver 层封装。

### 1.1 Router 对象与 run / serve / connect 生命周期

统一的 `Router` 对象（Python / TypeScript 的 `Router`，Go 的 `NewRouter` / `ConnectRouter`）承担三种角色：

1. **服务端配置**：传给 `run` / `serve`（Go `Run` / `Serve`），设置端点、凭据、重连与重试策略，以及本地内嵌 Router 的启动参数。
2. **共享客户端连接**：`Router.connect(...)` 建立一条底层 WebSocket，多个生成客户端复用它；三种运行时也都支持通过 `with_options` / `withOptions` / `WithOptions` 设置调用默认值。
3. **内置服务客户端**：`list_services` / `describe_service` / `whoami` / `issue_token` / `revoke_token`（TypeScript 为 camelCase，Go 为 `ListServices` / …）直接调用 Router 的 Registry 与 AuthService。

`run` 是一行启动入口：它从 `MESHCALL_ROUTER_URL`（默认 `ws://127.0.0.1:8765`）解析端点，当目标是本地地址且无人监听时拉起内置 `meshcall-router`，注册传入的所有服务，并在 SIGINT / SIGTERM 时排空后退出。`serve` 是其下的异步构件：返回 `RunningServer`（`.url`、`.instance_id`、`.stop(drain=True)`），测试与演示用它把 Router 起在临时端口（`ws://127.0.0.1:0`），并可显式停止或排空。若本地拉起时在端口绑定竞争中失败，会回退为连接胜出的进程；`spawn_if_local=False` 关闭自动拉起。Python 生成的客户端提供 `MyClient.connect(router_or_url)`；TypeScript 的 `MyClient.connect(router)` 与 Go 的 `NewMyClient(router)` 包装一个已有的 `Router`，因此复用其连接而不是再开一条。

单次调用控制参数刻意不进入生成的**业务**参数列表。`with_options` / `withOptions` / `WithOptions` 附加 `timeout`、`idle_timeout` / `timeoutMs`、`metadata`、`idempotency_key` / `idempotencyKey` 与 `instance`；生成的 TypeScript 与 Go 方法也接受一个可选的尾部 options / `opts ...` 参数用于同样目的。定向服务的生成客户端仍把 `instance` 作为必填参数（Python keyword-only、TypeScript 必填选项、Go 位置参数），由编译器强制。

### 1.2 依赖注入作用域

依赖注入在服务定义时编译、在每个调用中解析。Python 在 `@service` 注册阶段为每个提供者构建 `DependencyPlan`（含嵌套提供者与循环依赖检测），支持 `@dependency` 函数、默认参数写法（`user: str = current_user`）与 `Annotated[T, dependency(provider)]`。TypeScript 以 `dependency(provider, { inject })`、`meta("key")` 和每个调用新建的 `DependencyScope` 对应；Go 通过 `WithDependencies(...)` 注册 `Dependency(...)` 与 `Meta[T](...)`，反射 Handler 的参数类型命中注册表时即注入。

每个调用拥有独立作用域：提供者在**每次调用中最多执行一次**，结果按提供者缓存在该作用域内，因此依赖同一提供者的多个参数共享同一个实例。生成器 / 异步生成器提供者通过退出栈进入：yield 出的值被注入，`yield` 之后（或 `finally` 中）的代码在调用结束时执行——无论成功、异常还是取消。这正是事务型依赖能够“成功提交、失败回滚”而无需 Handler 手工 `commit()` 的原因（见 `best-practice/src/best_practice/service.py`）。元数据在普通依赖之前解析，缺少必需 Header 时会在获取任何资源之前以 `invalid_argument` 失败。注入参数会从契约中剔除，生成的客户端只看到业务参数。

### 1.3 请求绑定与生成签名

每个方法契约都保存一份请求 JSON Schema。TypeScript 的方法装饰器可以用三种绑定模式声明它；Python 与 Go 则从 Handler 签名推导出等价的请求 Schema：

- `request_object`：显式 `request` Schema（或 `@method` 的默认形式）。Handler 收到一个解析后的请求对象。
- `positional`：`input: { a: z.string(), b: z.int() }`（无参数时为 `input: {}`）。这个具名对象成为请求 Schema，Handler 按位置收到 `a, b`；字段名与顺序被保留。
- `single_schema`：`input: SomeSchema`。该 Schema 就是整个请求，Handler 将它作为一个值接收，不拆散其内部字段。

Python 按签名顺序提取非注入参数，单个模型类型的参数保持完整；Go 反射服务使用 `ctx` 之后、剔除已注册依赖类型之后的导出方法参数，指针与 `omitempty` 字段视为可选。代码生成器读取请求 Schema 的属性顺序与参数名，在 Python、TypeScript 与 Go 中 1:1 镜像，因此生成方法接收的正是服务端的业务参数列表。客户端流方法把入站迭代器追加在最后；单次调用选项通过 `with_options`（或生成 TypeScript / Go 方法的可选尾部 options 参数）附加，不作为业务参数。

---

## 2. RPC 事件循环与业务工作线程隔离（Python）

每个 Python `RpcServer` 实例都拥有一个运行在独立后台线程中的**业务工作事件循环（Service Worker Loop）**：

- **RPC 事件循环（主循环）**：负责 WebSocket 收发帧、调用状态机、超时定时器、取消传播、流控信用窗口（Credit）以及最终终止帧（Terminal Frame）仲裁。
- **Service Worker 事件循环（独立线程）**：负责 Pydantic 请求校验、执行用户业务 Handler、响应校验以及异步生成器流迭代。

两个事件循环之间通过线程安全的 Future 桥接。**这样设计的核心收益是**：即便用户的某个业务 Handler 不慎执行了同步阻塞操作或繁重的 CPU 计算，也只会延缓当前业务工作线程上的任务，**绝不会卡死底层的 RPC 事件循环**——心跳保活（Ping/Pong）、其他并发调用的超时取消及连接管理依然毫秒级精准响应。

其他运行时：

- **TypeScript**：运行在 Node.js 单线程事件循环上，Handler 不做工作线程隔离，应保持异步非阻塞。TypeScript 客户端支持一元、服务端流、客户端流、（实验性的）订阅与 duplex 调用；客户端、直连服务端与 Router 服务连接都支持 TCP 与本地传输。
- **Go**（`meshcall-go`）：端点同样写作 `ws://`、`unix:///path` 与 `pipe://NAME`；每个 Handler 运行在自己的 goroutine 中，带一个随调用结束而结束的 `context.Context`。

三种运行时使用同一个 Go Router；其进程由轻量的 Python/TypeScript 启动器拉起（Go 没有启动器）。Router 的注册表、账号鉴权、范围 Token RPC 与故障边界见 [Router 架构](router.md)。

---

## 3. 访问日志（Access Log）仲裁

访问日志由 RPC 循环在**唯一的终止帧仲裁点**输出。无论流式调用正常结束还是与取消信号发生竞态，每个调用都保证且仅输出一条包含 `service.method`、`status`、`duration_ms` 与 `call_id` 的访问日志：
- 成功调用按配置的日志级别（默认 `INFO`）记录；
- 调用方原因导致的失败（如 `invalid_argument`、`cancelled`）按 `WARNING` 记录；
- 服务端内部错误（`internal`、`protocol_error`）按 `ERROR` 记录。

未知方法与重复的 `call_id` 在 `call.open` 被拒绝时记录，因为它们从未创建调用对象。

---

## 4. 调用所有权与基于信用窗口（Credit）的流控

### 调用所有权
客户端为每次调用生成全局唯一的 UUID `call_id`。Direct 服务端按连接维护调用表；Router 则维护全局路由表，将客户端连接与选定的单个服务实例绑定，并在收到终止帧或连接断开时精确回收路由与并发计数。Router 在转发流数据时无需解码业务 Payload（仅 `sticky` 粘性路由策略会读取声明的路由键字段）。

### 双向独立信用窗口（Flow Control）
客户端到服务端（`client`）、服务端到客户端（`server`）两个流方向各自维护独立的条目信用额度（Item Credit）：
1. 接收端初始授予 **16 个条目**的窗口额度；
2. 每当业务代码从流迭代器消费 1 个条目，接收端自动向发送端补充 1 个信用额度（`stream.window`）；
3. 发送端在额度耗尽时会暂停生产等待授信（允许预取最多 1 个条目以提前感知迭代器结束）。

**Router 也执行信用规则**：它在转发窗口与条目时统计每个调用的信用，生产方超出信用时使该调用失败（`protocol_error`，`reason: "flow_control_violation"`），而不是缓冲多出的数据。因此 Router 的每连接发送队列只需按字节限制（`--outbox-size`，默认 256 MiB）：并发流的总量取决于接收方自己授予的信用，而非固定的帧数。详见[协议规范：流量控制](protocol.md#流方向与流量控制flow-control)。

### 公平帧调度（Fair Scheduling）
同一条连接上的发送调度器优先发送控制帧（`call.cancel`、`stream.window`、`ping`/`pong`），其余业务数据帧按 `call_id` 进行 **Round-Robin 公平轮转调度**，防止单个大吞吐流独占 WebSocket 连接而饿死其他并发调用。

---

## 5. 故障边界与恢复策略

- **异常脱敏**：未捕获的业务异常会被统一转换为脱敏的 `internal` 错误返回给客户端，完整堆栈仅记录在服务端本地日志中。
- **双向契约校验与零敏感值泄露**：
  - 请求或客户端流条目校验失败 → 返回 `invalid_argument`（`source: "server"`），附带字段级 `details.violations`（包含 `path`、`code`、`message`），**绝不在错误中回显被拒绝的原始输入值**。
  - 服务端自身的返回值或输出流条目违反契约 → 属于服务端自身 Bug，返回 `internal`（`reason: "invalid_response"` 或 `"invalid_output_item"`），详细违规信息仅打印在服务端日志中。
  - 客户端收到违反本地契约的响应或流条目 → 客户端立即抛出 `ProtocolError`（`source: "client"`，`reason: "invalid_response"` / `"invalid_output_item"`），第一时间拦截契约版本不一致。
- **类型化错误**：客户端为每个协议错误码提供一个子类（`InvalidArgumentError`、`DeadlineExceededError`、`UnavailableError` 等；`cancelled` 对应 `CallCancelledError`），由 `MeshCallError.from_code` / `fromCode` 构造；未知（业务）错误码保持为普通 `MeshCallError`。`error.source` 与 `error.reason` 读取对应的 `details` 字段。
- **三层相对超时（Relative Timeouts）**：超时以相对毫秒数 `timeout_ms` 在线上传输，无需机器间时钟同步。Server（`timeout_ms`）、Router（`timeout_ms + 500ms`）和 Client（`timeout_ms + max(1s, 10%)`）依次在各自单调时钟上独立兜底。`idle_timeout_ms` 只由消费方执行。
- **容量与熔断**：`max_inflight` 限制每个实例的并发调用数，服务端自身也会执行（`resource_exhausted`，`reason: "overloaded"`），Router 绕开满载实例；Router 的实例级熔断器在连续超时或 `internal`/`unavailable` 后排除实例，再放行一个探测调用。
- **重试**：内置的 `RetryPolicy`（位于客户端拦截器链最外层）只在错误可重试、错误码在策略集合中，且方法按契约是幂等的或调用带有 `idempotency_key` 时重发失败的一元调用；各次尝试共享调用的超时，最终错误报告 `details.attempts`。流从不重试；（实验性的）订阅改为从最后一个 cursor 重新打开（见[订阅](protocol.md#订阅subscriptions)）。
- **优雅排空**：`run` / `serve` 在 SIGINT / SIGTERM 或退出上下文时自动排空；更底层的形式是 `stop(drain=True)` / `close({ drain: true })` / Go `server.Drain(ctx)`，它们先停止接收新调用（向 Router 发送 `server.drain`，或关闭监听器），在排空超时内等待进行中的调用结束后再停止，进行中的调用不会失败。
- **断线重连策略**：连接断开会立即终止该连接上的活跃调用。随后客户端在下次发起调用时按指数退避与随机抖动（`ReconnectPolicy`）自动重连，最多等待该调用的超时；Router 模式服务端则在后台以同样退避持续重连并重新注册。调用从不自动重放；（实验性的）订阅从最后一个 cursor 重新打开。客户端与 Router 模式服务端都接受多个 Router 端点：客户端故障转移到下一个端点，服务端在每个 Router 上各保持一个注册。
- **帧错误**：仍能对应到某个调用的畸形帧只使该调用以 `protocol_error` 失败；无法归属的帧会关闭连接。超大帧使发送它的调用以 `resource_exhausted` 失败。

---

## 6. 契约质量静态检查（Contract Warnings & Strict Mode）

MeshCall 将契约规范分为两类：
1. **硬性错误（导入/定义期直接报错 `ContractError` / 抛出 `Error`）**：服务名格式非法、参数缺失类型注解、声明流形状与函数签名不匹配、同时声明 `read_only` 与 `destructive`，以及 duplex 规则（必须显式声明且 `reason` 非空、没有返回值、不能标注 `idempotent` 或 `read_only`）。
2. **质量告警（Advisory Warnings，`W001`–`W006`）**：带有稳定编号与直接修复提示（`hint`），在开启 **`MESHCALL_STRICT=1`** 时升级为致命错误：

| 编号 | 检查项说明 | 修复或显式静默方式 |
| --- | --- | --- |
| `W001` | 服务或方法缺少描述文档（Python docstring / TS `description`） | 补充描述，或显式传 `description=""` |
| `W002` | 方法名看似写操作（如 `create_*`、`update_*`、`delete_*`、`send_*`）但未声明任何读写注解 | 显式添加 `idempotent=True`、`destructive=True` 或 `read_only=True` |
| `W003` | 注解与方法名语义矛盾（如 `delete_*` 标了 `read_only=True`，或 `get_*` 标了 `destructive=True`） | 修正方法名或修正注解 |
| `W005` | 标记了 `deprecated` 但未说明替代方案 | 填写替代说明，如 `deprecated="请改用 v2 接口"` |
| `W006` | 在 `targeted=True` 的定向服务上配置了 `sticky` 负载均衡（定向服务永远不会走负载均衡） | 移除 `sticky` 策略 |

告警出现的位置：TypeScript 在编译期（不带 `description` 的方法构造器重载被标为 `@deprecated`）；Python 在导入期（`warnings.warn(MeshCallWarning)`）；三种运行时在服务启动时各输出一行汇总日志（Go 在 `NewServer` 中，`meshcall.LintService` 列出结果）。

严格模式：`MESHCALL_STRICT=1`（三种运行时通用）、TypeScript `MeshCallServer({ strictContracts: true })` 与 Go `ServerOptions{Strict: true}` 会把告警变为错误。也可通过 `MESHCALL_IGNORE_WARNINGS=W001,W002` 按编号全局忽略特定告警。`lint_contract(contract)` / `lintService(definition)` 无副作用地返回检查结果，供工具使用。

---

## 7. 拦截器（Interceptors）

Python、TypeScript 与 Go 在客户端和服务端均提供结构一致的洋葱模型拦截器（Go 为 `ServerOptions.Interceptors` / `ClientOptions.Interceptors`），列表中的第一个拦截器位于最外层：

- **服务端拦截器**：`(ctx, request, call_next) -> result`，在请求通过 Schema 校验后、进入业务 Handler 前执行。对于流式调用，`call_next()` 会在整个流发送完毕后才返回。抛出 `MeshCallError` 可短路调用；`ctx.set_trailer()` 添加 Trailers；`ctx.metadata` 携带调用方发来的元数据。
- **客户端拦截器**：`(info, call_next) -> result`，其中 `info` 包含服务名、方法名、调用形状以及可修改的 `options`（如统一注入 `metadata` 或默认 `timeout`），并可通过再次调用 `call_next(info)` 实现自定义重试。

拦截器看不到单个流条目；对 duplex 调用，它们只观察调用的开始与结束。

---

## 8. 可观测性（Observability）

OpenTelemetry 以每种语言一对可选拦截器的形式提供：Python `meshcall.otel.client_interceptor()` / `server_interceptor()`（安装 `meshcall[otel]`），TypeScript 从 `meshcall/otel` 导入 `otelClientInterceptor()` / `otelServerInterceptor()`（`@opentelemetry/api` 是可选 peer dependency；Node 需要 context manager，OpenTelemetry Node SDK 会注册它）。

Trace context 作为调用元数据（`traceparent` / `tracestate`）传递，因此链路可以跨语言、穿过 Router 延续——Router 原样转发元数据，自身不依赖 OpenTelemetry。每个调用生成一个名为 `service/method` 的 span（`rpc.system = "meshcall"`、`rpc.service`、`rpc.method`、`rpc.meshcall.error_code`），并按服务、方法、错误码与调用侧计入 `meshcall.calls` 与 `meshcall.call.duration`。服务端 span 覆盖整个调用；客户端 span 覆盖一元或客户端流调用，对流只覆盖打开阶段。

Router 在独立的监听地址上提供 Prometheus 指标（`--metrics-listen 127.0.0.1:9464`，从不在公开端口上），见 [Router 指标](router.md#7-指标metrics)。

---

## 9. 协议一致性测试套件（Conformance Suite）

`meshcall-router/conformance` 是验证一个语言运行时“是否完全符合 MeshCall 规范”的可执行标准测试集。它在进程内启动真实的 Go Router，拉起被测语言的 Fixture 进程，并通过脚本化的原始协议帧交互验证：服务注册、一元调用、错误透传、双向流信用窗口（含额度耗尽时的单条目预取）、服务端超时与空闲超时、取消传播、Metadata 与 Trailers、心跳保活、Router 重启后自动重注册以及优雅排空（Drain）。

Fixture 约定见 `meshcall-router/conformance/FIXTURE.md`；参考 Fixture 为 `meshcall-py/conformance/fixture.py`、`meshcall-ts/conformance/fixture.ts` 与 `meshcall-go/conformance/fixture`。新的语言运行时只要通过 `meshcall-conformance --fixture "<其 fixture 命令>"` 即视为符合规范。`scripts/check.sh conformance` 会让三个 Fixture 分别在 TCP 与本地传输（`--transport tcp` / `local`）上运行，CI 同样如此。客户端侧一致性测试（由测试套件扮演 Router 驱动脚本化的客户端 Fixture）是计划中的第二阶段。

运行命令：

```bash
scripts/check.sh conformance
```

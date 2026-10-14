# 运行时架构设计

[English](../architecture.md) | **简体中文**

## 1. 分层架构概览

```text
Python/Pydantic 服务定义 ---- 契约提取 (export) ----+
                                                    |
TypeScript Schema 服务定义 --- 契约导出 -------------+--> 跨语言可移植契约 (.meshcall.json)
                                                            |
                                                            +--> 生成 Python uv 客户端包
                                                            |
                                                            +--> 生成 TypeScript Yarn 客户端包

Python RPC 事件循环 <---- 线程安全桥接 ----> 业务工作线程事件循环 (Service Worker Loop)
        |
        v
逻辑帧连接与按调用公平调度器 (Fair Scheduler)
        |
        +---- WebSocket Direct 直连驱动
        |
        +---- WebSocket Router 路由驱动 ---- 服务实例注册表 (Registry)
```

- **契约层与网络传输解耦**：默认调用形状为一元调用（`unary`），通过显式装饰器声明服务端流（`server_stream`）或客户端流（`client_stream`）。**函数参数即请求体（Parameters are the request）**：多个函数参数会被自动打包为一个合成的请求模型用于校验与传输。服务名自动规范化为包含一个版本段（如 `chat` → `chat.v0`）。
- **依赖注入参数自动剥离**：名为 `logger`（类型 `RpcLogger`）和 `ctx` / `context`（类型 `RpcContext`）的参数属于服务端运行时依赖注入，在提取请求契约时会自动剔除，不会暴露在生成的客户端参数列表中。
- **逻辑帧与底层模式解耦**：运行时核心仅处理逻辑帧（Frame），无需关心底层是 Direct 直连还是经由 Router 转发；连接握手、角色声明、TCP/Unix 套接字绑定及服务注册均由 Driver 层封装。

---

## 2. RPC 事件循环与业务工作线程隔离（Python）

每个 Python `RpcServer` 实例都拥有一个运行在独立后台线程中的**业务工作事件循环（Service Worker Loop）**：

- **RPC 事件循环（主循环）**：负责 WebSocket 收发帧、调用状态机、超时定时器、取消传播、流控信用窗口（Credit）以及最终终止帧（Terminal Frame）仲裁。
- **Service Worker 事件循环（独立线程）**：负责 Pydantic 请求校验、执行用户业务 Handler、响应校验以及异步生成器流迭代。

两个事件循环之间通过线程安全的 Future 桥接。**这样设计的核心收益是**：即便用户的某个业务 Handler 不慎执行了同步阻塞操作或繁重的 CPU 计算，也只会延缓当前业务工作线程上的任务，**绝不会卡死底层的 RPC 事件循环**——心跳保活（Ping/Pong）、其他并发调用的超时取消及连接管理依然毫秒级精准响应。

（注：TypeScript 运行时目前运行在 Node.js 单线程事件循环上，因此 TypeScript Handler 应保持异步非阻塞。）

---

## 3. 访问日志（Access Log）仲裁

访问日志由 RPC 循环在**唯一的终止帧仲裁点**输出。无论流式调用正常结束还是与取消信号发生竞态，每个调用都保证且仅输出一条包含 `service.method`、`status`、`duration_ms` 与 `call_id` 的访问日志：
- 成功调用按配置的日志级别（默认 `INFO`）记录；
- 客户端原因导致的失败（如 `invalid_argument`、`cancelled`）按 `WARNING` 记录；
- 服务端内部错误（`internal`、`protocol_error`）按 `ERROR` 记录。

---

## 4. 调用所有权与基于信用窗口（Credit）的流控

### 调用所有权
客户端为每次调用生成全局唯一的 UUID `call_id`。Direct 服务端按连接维护调用表；Router 则维护全局路由表，将客户端连接与选定的单个服务实例绑定，并在收到终止帧或连接断开时精确回收路由与并发计数。Router 在转发流数据时无需解码业务 Payload（仅 `sticky` 粘性路由策略会读取声明的路由键字段）。

### 双向独立信用窗口（Flow Control）
客户端到服务端（`client`）、服务端到客户端（`server`）两个流方向各自维护独立的条目信用额度（Item Credit）：
1. 接收端初始授予 **16 个条目**的窗口额度；
2. 每当业务代码从流迭代器消费 1 个条目，接收端自动向发送端补充 1 个信用额度（`stream.window`）；
3. 发送端在额度耗尽时会暂停生产等待授信（允许预取最多 1 个条目以提前感知迭代器结束）。

### 公平帧调度（Fair Scheduling）
同一条连接上的发送调度器优先发送控制帧（`call.cancel`、`stream.window`、`ping`/`pong`），其余业务数据帧按 `call_id` 进行 **Round-Robin 公平轮转调度**，防止单个大吞吐流独占 WebSocket 连接而饿死其他并发调用。

---

## 5. 故障边界与恢复策略

- **异常脱敏**：未捕获的业务异常会被统一转换为脱敏的 `internal` 错误返回给客户端，完整堆栈仅记录在服务端本地日志中。
- **双向契约校验与零敏感值泄露**：
  - 请求或客户端流条目校验失败 → 返回 `invalid_argument`（`source: "server"`），附带字段级 `details.violations`（包含 `path`、`code`、`message`），**绝不在错误中回显被拒绝的原始输入值**。
  - 服务端自身的返回值或输出流条目违反契约 → 属于服务端自身 Bug，返回 `internal`（`reason: "invalid_response"` 或 `"invalid_output_item"`），详细违规信息仅打印在服务端日志中。
  - 客户端收到违反本地契约的响应或流条目 → 客户端立即抛出 `ProtocolError`（`source: "client"`，`reason: "invalid_response"` / `"invalid_output_item"`），第一时间拦截契约版本不一致。
- **三层相对超时（Relative Timeouts）**：超时以相对毫秒数 `timeout_ms` 在线上传输，无需机器间时钟同步。Server（`timeout_ms`）、Router（`timeout_ms + 500ms`）和 Client（`timeout_ms + max(1s, 10%)`）依次在各自单调时钟上独立兜底。
- **断线重连策略**：连接断开会立即终止该连接上的活跃调用。随后客户端在下次发起调用时按指数退避与随机抖动（`ReconnectPolicy`）自动重连；Router 模式服务端则在后台持续重连并重新注册。

---

## 6. 契约质量静态检查（Contract Warnings & Strict Mode）

MeshCall 将契约规范分为两类：
1. **硬性错误（导入/定义期直接报错 `ContractError`）**：服务名格式非法、参数缺失类型注解、声明流形状与函数签名不匹配、同时声明 `read_only` 与 `destructive`。
2. **质量告警（Advisory Warnings，`W001`–`W006`）**：带有稳定编号与直接修复提示（`hint`），在开启 **`MESHCALL_STRICT=1`** 时升级为致命错误：

| 编号 | 检查项说明 | 修复或显式静默方式 |
| --- | --- | --- |
| `W001` | 服务或方法缺少描述文档（Python docstring / TS `description`） | 补充描述，或显式传 `description=""` |
| `W002` | 方法名看似写操作（如 `create_*`、`update_*`、`delete_*`、`send_*`）但未声明任何读写注解 | 显式添加 `idempotent=True`、`destructive=True` 或 `read_only=True` |
| `W003` | 注解与方法名语义矛盾（如 `delete_*` 标了 `read_only=True`，或 `get_*` 标了 `destructive=True`） | 修正方法名或修正注解 |
| `W005` | 标记了 `deprecated` 但未说明替代方案 | 填写替代说明，如 `deprecated="请改用 v2 接口"` |
| `W006` | 在 `targeted=True` 的定向服务上配置了 `sticky` 负载均衡（定向服务永远不会走负载均衡） | 移除 `sticky` 策略 |

可通过环境变量 `MESHCALL_STRICT=1`（或在 TS 中传 `strictContracts: true`）开启严格模式；也可通过 `MESHCALL_IGNORE_WARNINGS=W001,W002` 按编号全局忽略特定告警。

---

## 7. 拦截器（Interceptors）

Python 与 TypeScript 在客户端和服务端均提供结构一致的洋葱模型拦截器，列表中的第一个拦截器位于最外层：

- **服务端拦截器**：`(ctx, request, call_next) -> result`，在请求通过 Schema 校验后、进入业务 Handler 前执行。对于流式调用，`call_next()` 会在整个流发送完毕后才返回。
- **客户端拦截器**：`(info, call_next) -> result`，其中 `info` 包含服务名、方法名、调用模式以及可修改的 `options`（如统一注入 `metadata` 或默认 `timeout`），并可通过再次调用 `call_next(info)` 实现自定义重试。

---

## 8. 协议一致性测试套件（Conformance Suite）

`meshcall-router/conformance` 是验证一个语言运行时“是否完全符合 MeshCall 规范”的可执行标准测试集。它在进程内启动真实的 Go Router，拉起被测语言的 Fixture 进程，并通过脚本化的原始协议帧交互验证：服务注册、一元调用、错误透传、双向流信用窗口（含额度耗尽时的单条目预取）、服务端超时与空闲超时、取消传播、Metadata 与 Trailers、心跳保活、Router 重启后自动重注册以及优雅排空（Drain）。

运行命令：

```bash
scripts/check.sh conformance
```

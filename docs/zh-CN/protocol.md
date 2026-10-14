# MeshCall 逻辑协议规范 v1 (`meshcall/1`)

[English](../protocol.md) | **简体中文**

MeshCall 在有序、可靠的字节流/消息连接上传输 JSON 逻辑帧。WebSocket（TCP 或 Unix Domain Socket）是首个物理传输层实现，但本规范定义的所有状态机与帧规则均独立于底层传输协议。

---

## 1. 连接握手与服务注册

连接建立后，发起方发送的第一帧必须是 `hello`，声明其角色为 `client` 或 `server`，协议版本为 `meshcall/1`。接收方校验通过后回复 `hello.ack`。

在 Router 模式下，`server` 角色在收到 `hello.ack` 后紧接着发送一帧 `server.register`，上报其不可变的服务注册信息与可移植契约。同一个服务的不同实例不必注册完全相同的方法集合：Router 按具体方法在注册了该方法的实例间路由，仅当同名方法的流形状（`stream`）、负载均衡策略（`balance`）或服务的 `targeted` 标志不一致时才拒绝注册。

---

## 2. 前向兼容规则

在 `meshcall/1` 协议内：
- 解码器**必须忽略任何帧上的未知字段（Unknown Fields）**；
- 解码器**必须拒绝未知的帧类型（Unknown Frame Kinds）**。

这一规则保证了未来在小版本迭代中为现有帧新增可选字段时无需升级协议主版本；同时 Router 在转发调用帧时保持原样透传，使其不认识的新字段也能无损到达对端。

---

## 3. 逻辑帧一览（Frames）

每一帧都是一个包含字符串字段 `kind` 的 JSON 对象。`call_id` 是由客户端生成的全局唯一字符串，所有时间字段均为整数毫秒（`ms`）。

| 帧类型 (`kind`) | 核心字段 | 说明 |
| --- | --- | --- |
| `hello` | `protocol`, `role`, `peer_id`, `instance_id?`, `capabilities?` | 连接发起方发送的第一帧 |
| `hello.ack` | `protocol`, `connection_id` | 连接接收方的握手确认回复 |
| `server.register` | `instance_id`, `services[{name, methods[], targeted?, contract?}]`, `max_inflight?` | 仅用于 Router 模式；每条服务端连接发送一次 |
| `server.register.ack` | `instance_id` | Router 确认服务注册成功 |
| `server.drain` | — | 服务实例 → Router：从所有负载均衡池摘除，但继续处理已路由的存量调用 |
| `server.drain.ack` | `instance_id?` | Router → 服务实例：确认已摘除 |
| `call.open` | `call_id`, `service`, `method`, `payload`, `timeout_ms?`, `idle_timeout_ms?`, `metadata?`, `idempotency_key?`, `instance_id?` | 客户端发起一次 RPC 调用 |
| `call.result` | `call_id`, `payload?`, `metadata?` | **终止帧（Terminal）**：调用成功结束并携带返回值与响应 Trailers |
| `call.error` | `call_id`, `error{code,message,retryable?,details?}`, `metadata?` | **终止帧（Terminal）**：调用失败并携带结构化错误与响应 Trailers |
| `call.cancel` | `call_id`, `reason?` | 请求取消该调用（属于通知请求，本身不是终止帧） |
| `stream.item` | `call_id`, `direction`, `sequence`, `payload` | 流式数据帧（`direction` 为 `"client"` 或 `"server"`，`sequence` 从 0 递增） |
| `stream.end` | `call_id`, `direction` | 半关闭（Half-Close）指定方向的发送流 |
| `stream.window` | `call_id`, `direction`, `credit` | 授予指定方向额外发送 `credit`（`> 0`）个流条目的信用额度 |
| `ping` | `nonce` | 协议层心跳探测帧 |
| `pong` | `nonce` | 原样回显 `nonce` 的心跳响应帧 |

### 元数据（Metadata 与 Trailers）
- `call.open` 上的 `metadata` 携带调用方传入的请求头（如 Trace ID、租户标识）；
- `call.result` 与 `call.error` 上的 `metadata` 携带服务端返回的响应尾标（Trailers）；
- Key 与 Value 均为字符串，区分大小写；以 `meshcall-` 开头的 Key 保留给运行时内部使用。

---

## 4. 服务命名规范（Service Names）

服务名由点号（`.`）分隔的多个标识符段组成，其中**必须包含且仅包含一个版本段**（格式为 `v<major>` 或 `v<major>.<minor>.<patch>[-<prerelease>]`）。若用户在代码中省略了版本段，运行时会在注册和调用前自动追加 `.v0`（即 `chat` 等价于 `chat.v0`）。

合法示例：`chat.v1`、`demo.v1.Greeting`、`chat.v1.2.0`、`chat.v2.0.0-beta1`。路由按完整规范化名称精确匹配。

---

## 5. 四种调用形状（Call Shapes）

| 形状 (`stream`) | 客户端发送流条目 (`client`) | 服务端发送流条目 (`server`) | `call.result` 是否携带最终返回值 |
| --- | --- | --- | --- |
| `unary`（一元） | 否 | 否 | 是 |
| `server_stream`（服务端流） | 否 | 是 | 否（`call.result` 仅表示流正常结束） |
| `client_stream`（客户端流） | 是 | 否 | 是 |
| `duplex`（双向流，实验性） | 是 | 是 | 可选 |

---

## 6. 三层超时机制（Timeouts）

- **`timeout_ms`**：整个调用的总时间预算（相对时间，各节点使用本地单调时钟从收到 `call.open` 起独立计时，无需机器间时钟同步）。
- **`idle_timeout_ms`**：流式调用中相邻两个 `stream.item` 之间的最大允许静默间隔，仅由该流方向的消费端执行检查。

为了在故障时优先向调用方返回信息量最丰富的错误，`timeout_ms` 由三层依次递增触发：

| 执行层级 | 触发时刻 | 触发动作 |
| --- | --- | --- |
| **1. Server（服务端）** | `timeout_ms` | 取消业务 Handler 协程，发送 `call.error`（`code: "deadline_exceeded"`, `source: "server"`） |
| **2. Router（路由器）** | `timeout_ms + 宽限期`（默认 `+ 500ms`） | 向客户端发送 `deadline_exceeded`（`source: "router"`），向卡死的服务实例发送 `call.cancel` 并释放路由 |
| **3. Client（客户端）** | `timeout_ms + 宽限期`（默认 `+ max(1s, 10%)`） | 在本地将调用判定为 `deadline_exceeded`（`source: "client"`），并尽力发送 `call.cancel` |

---

## 7. 心跳保活（Liveness）

为了在所有传输层与运行环境（含容器、NAT 网关）下表现一致，MeshCall 使用协议层 `ping`/`pong` 帧保活：
- **主动连接方（客户端，或连接到 Router 的服务实例）**：在连接空闲满 `ping_interval`（默认 15 秒）时发送 `ping`；若在 `ping_timeout`（默认 10 秒）内未收到匹配 `nonce` 的 `pong`，则主动断开连接并将连接上的活跃调用标记为可重试的 `unavailable`。
- **被动监听方（Router 或 Direct 服务端）**：不主动发 `ping`，但会关闭超过 `idle_timeout`（默认 45 秒）未收到任何帧的静默连接。

---

## 8. 标准错误码与校验违规详情（Errors & Violations）

| 错误码 (`code`) | 含义 | 常见来源 (`details.source`) |
| --- | --- | --- |
| `cancelled` | 调用被客户端或对端取消 | `client`, `server`, `router` |
| `deadline_exceeded` | `timeout_ms` 或 `idle_timeout_ms` 超时 | `server`, `router`, `client` |
| `invalid_argument` | 请求参数或客户端流条目未通过契约校验，或定向服务缺少 `instance_id` | `server`, `router` |
| `method_not_found` | 未知的服务名或方法名 | `server`, `router` |
| `permission_denied` | 已认证但无权访问目标服务/方法 | `router` |
| `unauthenticated` | 未提供凭据或凭据无效 | `router` |
| `resource_exhausted` | 达到容量上限（如 `max_inflight` 满载 `overloaded` 或单帧超大），通常可重试 | `router`, `server`, `client` |
| `unavailable` | 无可用实例（`no_instances`）、实例断线（`instance_lost`）、全员熔断（`circuit_open`）或目标实例正在排空（`instance_draining`） | `router`, `client` |
| `protocol_error` | 帧格式违反协议，或客户端收到不符合本地契约的响应（`invalid_response`） | 任意节点 |
| `internal` | 服务端内部未预期异常，或服务端返回值未通过自身契约校验 | `server` |

### 结构化校验错误（`details.violations`）
- 当客户端请求或输入流条目校验失败时，服务端返回 `invalid_argument`（`details.reason` 为 `"request"` 或 `"input_item"`），并在 `details.violations` 中提供 `[{path, code, message}]` 列表（例如 `path: "profile.tags.0"`）。**为防止日志或错误信息泄露敏感数据，任何校验错误都绝不包含被拒绝的原始输入值。**
- 当服务端自身的返回值违反契约时，服务端返回 `internal`（`details.reason = "invalid_response"`），违规详情仅记录在服务端日志中。
- 当客户端收到无法通过本地契约 Schema 校验的响应时，客户端在本地抛出 `protocol_error`（`details.source = "client"`, `details.reason = "invalid_response"`），防止版本偏移的脏数据流入业务代码。

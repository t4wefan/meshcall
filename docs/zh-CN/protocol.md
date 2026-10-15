# MeshCall 逻辑协议规范 v1 (`meshcall/1`)

[English](../protocol.md) | **简体中文**

MeshCall 在有序、可靠的连接上传输 JSON 逻辑帧。WebSocket 是首个物理传输层实现，但本规范定义的所有状态机与帧规则均独立于底层传输协议。

WebSocket 可以运行在 TCP 上，也可以运行在平台的本地传输上：Linux、macOS 与 FreeBSD 上是 Unix Domain Socket，Windows 上是命名管道（`\\.\pipe\NAME`）。每个平台只有一种本地传输，另一种会被拒绝并给出提示。WebSocket 升级、帧格式与 HTTP 调用在所有传输上完全一致。工具中的端点写法分别为 `ws://host:port`、`unix:///path` 与 `pipe://NAME`。

本文是英文规范的中文摘要，各节末尾链接到英文原文的完整描述；两者不一致时以英文版为准。

---

## 连接握手与服务注册

连接建立后，发起方发送的第一帧必须是 `hello`，声明其角色为 `client` 或 `server`，协议版本为 `meshcall/1`。接收方必须在接受调用前拒绝不支持的版本，校验通过后回复 `hello.ack`。

在 Router 模式下，`server` 角色在收到 `hello.ack` 后紧接着发送一帧 `server.register`，上报其不可变的服务注册信息与可移植契约。同一个服务的不同实例不必注册完全相同的方法集合：Router 按具体方法在注册了该方法的实例间路由，仅当同名方法的流形状（`stream`）、负载均衡策略（`balance`）或服务的 `targeted` 标志不一致时才拒绝注册。

---

## 前向兼容规则

在 `meshcall/1` 协议内：
- 解码器**必须忽略任何帧上的未知字段（Unknown Fields）**；
- 解码器**必须拒绝未知的帧类型（Unknown Frame Kinds）**。

小版本迭代可以为现有帧新增可选字段而无需升级协议版本；新增帧类型或改变已有字段语义则需要新的协议版本。Router 在转发调用帧时保持原样透传，使其不认识的新字段也能无损到达对端。

---

## 逻辑帧一览（Frames）

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

`services[]` 中每项为 `{name, methods[{name, stream, balance{kind, key?}}], contract?}`。`contract` 是服务的可移植契约条目（与 `*.meshcall.json` 文档中的对象相同：版本、描述、带 Schema 的方法、参数、注解；duplex 方法另含 `duplex_reason`，订阅方法另含 `subscription`）。它对路由不透明，Router 为每个服务保留首个注册的契约，并通过 `meshcall.router.v1.Registry` 服务（`list`、`describe {service}`）对外提供。`stream` 取值为 `unary`、`server_stream`、`client_stream`、`duplex`；`balance.kind` 取值为 `round_robin`、`least_inflight`、`random`、`sticky`、`disabled`。`capabilities` 保留给后续小版本协商可选特性，对端忽略不认识的条目。

### 帧大小（Frame size）

单帧编码后的大小默认上限为 64 MiB，这也是 Router 接受的最大值（`--max-frame-size`；各运行时为 `max_frame_size` / `maxFrameSize` / `MaxFrameSize`）。一个负载（包括单个流条目）必须放进一帧，更大的数据请用流发送。收到超大帧的一方会关闭连接（WebSocket 关闭码 1009）。Router 为每个对端排队的帧最多 `--outbox-size` 字节（默认 256 MiB，且不小于最大帧大小）；对端停止读取且欠它的数据超过该值时会被断开。

### 元数据（Metadata 与 Trailers）
- `call.open` 上的 `metadata` 携带调用方传入的请求头（如 Trace Context、关联 ID、租户标识）；
- `call.result` 与 `call.error` 上的 `metadata` 携带服务端返回的响应尾标（Trailers）；
- Key 与 Value 均为字符串，Key 区分大小写且不能为空；以 `meshcall-` 开头的 Key 保留给运行时内部使用；Router 原样转发元数据。

---

## 服务命名规范（Service Names）

服务名由点号（`.`）分隔的多个标识符段组成，除版本外每段以字母开头，只含字母、数字与下划线。其中**必须包含且仅包含一个版本段**（格式为 `v<major>` 或 `v<major>.<minor>.<patch>[-<prerelease>]`）。若省略了版本段，运行时会在注册和调用前自动追加 `.v0`（即 `chat` 等价于 `chat.v0`）。

合法示例：`chat.v1`、`demo.v1.Greeting`、`chat.v1.2.0`、`chat.v2.0.0-beta1`。路由按完整规范化名称精确匹配，`chat.v1` 与 `chat.v1.2.0` 是不同的服务。`meshcall.router.` 前缀保留给 Router 内置服务。

---

## 容量与健康（Capacity and health）

- **`max_inflight`**：`server.register` 上声明实例在所有服务上同时接受的调用数。Router 不会选中已满载的实例，所有实例都满载时返回可重试的 `resource_exhausted`（`reason: "overloaded"`）；实例自身也执行该上限，因此直连模式行为一致。
- **单连接调用上限**：Router 限制单个连接同时进行的调用数（`--max-calls-per-connection`，默认 1000，`0` 表示不限；加入进行中幂等调用的请求也计数）。超出的 `call.open` 以可重试的 `resource_exhausted`（`reason: "overloaded"`）失败，连接保持打开。
- **熔断器**：Router 为每个实例维护熔断器。命中 Router 超时、或实例返回 `internal` / `unavailable` 计为失败，业务错误不计。连续失败达到阈值（默认 5）后实例被排除一段冷却时间（默认 10 秒），之后只放行一个探测调用：成功则关闭熔断，失败则重新打开。所有实例都被排除时返回可重试的 `unavailable`（`reason: "circuit_open"`）。熔断状态仅在内存中，实例重连即重置。

详见英文版 [Capacity and health](../protocol.md#capacity-and-health)。

---

## 排空（Draining）

即将停止的服务实例发送 `server.drain`。Router 立即把它从所有服务池摘除（不再向它路由新的 `call.open`），回复 `server.drain.ack`，并继续双向转发已路由调用的帧直到它们结束。实例随后在自身排空超时内等待进行中调用结束，再关闭连接；在此之前其 `instance_id` 保持占用。重复发送 `server.drain` 无害；客户端发送它属于协议违规。直连服务端的排空方式是关闭监听器、保留已有连接并同样等待。

### Router 停机（Router shutdown）

Router 收到停止请求（SIGTERM 或 SIGINT；第二次信号跳过等待）时先排空：

1. `/readyz` 返回 503。新连接以 HTTP 503 与 `Retry-After` 拒绝；已有连接上的新 `call.open` 以可重试的 `unavailable`（`reason: "router_draining"`）失败。这类调用尚未到达任何实例，换到别处重试总是安全的。
2. 没有进行中调用的连接（作为客户端没有调用、作为实例没有被路由的调用）立即以 WebSocket 状态码 1001（going away）关闭，其余连接在最后一个调用结束时关闭。客户端随之重连，配置了多个 Router 时会切换到其他 Router；服务实例同样重连并注册到仍接受它的 Router。
3. 超过 `--shutdown-timeout`（默认 10 秒）后仍在进行的调用被结束：客户端收到可重试的 `unavailable`（`reason: "router_shutdown"`），实例收到原因为 `router_shutdown` 的 `call.cancel`，剩余连接以 1001 关闭。

不经排空而停止的 Router（其所有者关闭了 stdin，或嵌入程序调用了 `Close`）会立即执行第 3 步。详见英文版 [Router shutdown](../protocol.md#router-shutdown)。

---

## 调用与调用形状（Calls）

客户端生成全局唯一的 `call_id` 并发送 `call.open`。Router 模式下，Router 收到该帧时选定一个已注册实例，该映射在调用的整个生命周期内不变，此后该调用的所有流条目、窗口、结果、错误与取消帧都沿同一映射转发。

| 形状 (`stream`) | 客户端发送流条目 | 服务端发送流条目 | 最终返回值 |
| --- | --- | --- | --- |
| `unary`（一元） | 否 | 否 | 是 |
| `server_stream`（服务端流） | 否 | 是 | 否（`call.result` 仅表示流正常结束） |
| `client_stream`（客户端流） | 是 | 否 | 是 |
| `duplex`（双向流，受限形状） | 是 | 是 | 否（`call.result` 不带负载） |

`duplex` 是受支持但受限制的形状，规则见下文[双向流调用](#双向流调用duplex-calls)。订阅（subscription）在线路上就是 `server_stream`，见[订阅](#订阅subscriptions)。

---

## 定向调用（Targeted calls）

每个服务实例都有一个 **instance id**：由运维者指定（`instance_id=` / `instanceId`），或由 SDK 在启动时生成；服务自身（`ctx.instance_id`、`server.instance_id`）、Router 的 Registry 与 `meshcall router inspect` 都能看到它。同一实例向它注册的每个 Router 使用相同的 id；id 在实例自身重连时保持不变，并且在一个 Router 内必须唯一，重复的注册会被拒绝。

`call.open` 可以携带 `instance_id` 直接点名某个实例，Router 跳过负载均衡：

- 该实例未注册该服务 → `unavailable`，`details.reason = "instance_unknown"`（不可重试）；
- 实例正在排空 → `unavailable`，`reason: "instance_draining"`（可重试）；
- 实例已达 `max_inflight` → `resource_exhausted`，`reason: "overloaded"`；
- 否则即使该实例熔断器处于打开状态也照常路由：调用方是按名字要它的。

以 `targeted: true` 注册的服务要求每次调用都指定实例（各实例持有不同状态：会话、已加载的模型、Actor）。缺少 `instance_id` 的调用不经路由即返回 `invalid_argument`（`reason: "instance_required"`）。同一服务的所有实例必须在 `targeted` 上一致；`sticky` 负载均衡对定向服务无意义，会被忽略。直连服务端按自身 id 执行同样规则。幂等去重的键包含目标实例：同一个 Key 发往两个实例是两次调用。

详见英文版 [Targeted calls](../protocol.md#targeted-calls)。

---

## 超时（Timeouts）

- **`timeout_ms`**：整个调用的总时间预算（相对时间，各节点使用本地单调时钟从处理 `call.open` 起独立计时，无需机器间时钟同步）。缺省表示不限。
- **`idle_timeout_ms`**：流式调用中相邻两个 `stream.item` 之间的最大允许间隔，仅由该流方向的消费端检查（客户端检查 `server` 方向，服务端检查 `client` 方向），一元调用忽略。超时后消费端以 `deadline_exceeded`（`details.reason = "idle"`）结束调用并发送 `call.cancel`。Router 不执行空闲超时。

为了在故障时优先向调用方返回信息量最丰富的错误，`timeout_ms` 由三层依次递增触发：

| 执行层级 | 触发时刻 | 触发动作 |
| --- | --- | --- |
| **1. Server（服务端）** | `timeout_ms` | 取消业务 Handler，发送 `call.error`（`code: "deadline_exceeded"`） |
| **2. Router（路由器）** | `timeout_ms + 宽限期`（默认 `+ 500ms`） | 向客户端发送 `deadline_exceeded`（`details.source = "router"`），向卡死的服务实例发送原因为 `deadline_exceeded` 的 `call.cancel` 并释放路由 |
| **3. Client（客户端）** | `timeout_ms + 宽限期`（默认 `+ max(1s, 10%)`） | 在本地将调用判定为 `deadline_exceeded`（`details.source = "client"`），并尽力发送 `call.cancel` |

服务端层是权威的；Router 层防止实例不断开连接却不再响应；客户端层防止 Router 或直连服务端不再响应，其预算还覆盖建立连接与握手的时间。`deadline_exceeded` 永远不是 `retryable`，能否重试取决于方法本身。

---

## 幂等键（Idempotency keys）

`idempotency_key`（1–128 字节，由调用方选择）标识一次调用的*意图*，使重试变得安全，服务端会把它交给 Handler。

- **一元调用**：Router 按 `(service, method, idempotency_key, instance_id)` 去重（非定向调用的 `instance_id` 为空）。同一 Key 的前一次调用仍在进行时，第二个 `call.open` 会加入它并收到同一个终止帧；完成后在短时间窗口（默认 60 秒）内重放该终止帧。若处理实例在应答前断开，所有加入的调用方收到同一个可重试的 `unavailable`，下一次携带该 Key 的调用优先选择其他实例。
- **服务端流与客户端流**：Key 会传给 Handler，但从不去重。
- **双向流**：无法安全重放，因此携带 `idempotency_key` 的 duplex `call.open` 会被 Router（路由前）与直连服务端拒绝，返回 `invalid_argument`，`details.reason = "idempotency_key_on_duplex"`。

去重表只是缓存：Router 重启会丢失它，最坏情况是对遵守 Key 的 Handler 多执行一次。客户端运行时自带的重试策略只在错误为 `retryable`、错误码在重试集合中，且方法在契约中声明为幂等或调用携带 `idempotency_key` 时才重发失败的一元调用；各次尝试共享同一个 `timeout_ms`。

---

## 流方向与流量控制（Flow control）

`client` 方向是客户端产生、服务端消费的数据；`server` 方向反之。两个方向的 `sequence` 相互独立，从 0 开始逐一递增。`stream.end` 只半关闭指定方向，不关闭另一方向，也不结束调用。

每个流方向使用条目信用（item credit）：`stream.window` 授予生产方在该方向再发送 `credit` 个条目的许可，没有信用时生产方不得发送下一个条目。运行时可以在等待信用时预取一个条目，以便无需额外授权即可发现迭代器已结束。

信用按调用、按方向计算。**Router 会执行这一规则**：它统计每个转发的 `stream.window` 授予的信用与据此转发的条目，超出信用的条目视为生产方违规——Router 不转发它，向客户端返回 `protocol_error`（`details.source = "router"`，`details.reason = "flow_control_violation"`），并向实例发送原因为 `flow_control_violation` 的 `call.cancel`。只有这一个调用失败，不会关闭任何连接。消费方收到超出信用的条目时同样以 `protocol_error` 结束调用。

因此 Router 为某个对端缓冲的内容都是该对端自己请求的：它授予信用范围内的条目，加上它发起或接受的每个调用有限数量的控制帧与终止帧。Router 按字节（而非帧数）限制每个对端的缓冲区（`--outbox-size`）；对端停止读取且其请求的数据超过预算时作为慢节点被断开。可能停顿的消费方应让一个连接上所有调用的未用信用乘以条目大小保持在该预算内。条目信用不限制字节数，未来的协议版本可能增加按字节的流信用而不改变调用语义。

详见英文版 [Flow control](../protocol.md#flow-control)。

---

## 终止帧（Terminal frames）

一个调用恰好由一个 `call.result` 或 `call.error` 终止。`call.cancel` 是终止请求，本身不是终止确认。运行时接受的第一个终止帧生效，之后的帧被忽略。

Router 把客户端的 `call.cancel` 视为其对该路由兴趣的结束：转发取消帧、释放路由、丢弃该 `call_id` 之后的帧；实例仍会发送自己的终止帧，由 Router 丢弃。客户端发出取消后即可在本地以 `cancelled` 结束调用。连接断开会终止该连接上所有活跃调用；版本 1 不恢复流，也不在实例间迁移流。

---

## 双向流调用（Duplex calls）

duplex 调用同时在两个方向上流式传输：客户端仍在发送输入条目时，服务端就可以发送输出条目。它没有专属的帧：每个方向各自拥有序号、信用与半关闭，客户端的 `idle_timeout_ms` 由各方向的消费方分别执行。调用以**不带负载**的 `call.result` 或 `call.error` 结束，没有最终返回值；Trailers 照常随终止帧返回。

duplex 是最后的选择：它关闭了其他形状所保有的保证，方法必须在契约中显式声明理由（`duplex_reason`，必填）才能使用。duplex 方法：

- 从不重试、从不去重，不接受 `idempotency_key`（否则返回 `invalid_argument` / `idempotency_key_on_duplex`），也不能被标注为 `idempotent` 或 `read_only`；
- 不能通过 HTTP 调用（`405`，`details.reason = "duplex_over_http"`）；
- 整个生命周期固定在一个实例上：只在 `call.open` 时负载均衡一次，排空会等待它结束（受排空超时限制）；
- 对超时、熔断器与拦截器而言只是一次调用：它们观察开始与结束，而不是单个条目；
- 可能自我死锁：客户端只发送不读取输出时，服务端输出信用耗尽、停止读取输入，客户端输入信用也随之耗尽。请在发送的同时并发读取输出（或每个方向交替一条）。运行时不检测这种情况。

只要两个方向并非真正交错，就优先使用"服务端流 + 一元调用"或客户端流。详见英文版 [Duplex calls](../protocol.md#duplex-calls)。

---

## 订阅（Subscriptions）

订阅是一种预期长期保持打开（事件、通知、实时数据）并在中断后恢复的服务端流。它不给线路协议增加任何东西：调用、信用、取消与错误都与服务端流相同，方法的 `stream` 就是 `server_stream`。它增加的是一个客户端据以行动的契约条目：

```json
"subscription": {"cursor": "seq", "resume": "since"}
```

- `cursor` 指定输出条目中标识位置的属性（序号、事件 ID、时间戳）；`resume` 指定一个可选请求参数，要求服务端从该位置*之后*继续。两者要么都给出，要么都不给（`{}`）：没有 cursor 的订阅只能从"现在"重新开始。工具与 Registry 将其显示为订阅。

客户端运行时对订阅调用的处理与其他流不同：

- 没有默认的 `timeout_ms` 与 `idle_timeout_ms`；
- 遇到**可重试**失败（`unavailable`、`resource_exhausted` 或连接丢失）时，按重连退避重新打开调用；若声明了 cursor 且已收到过条目，则把 `resume` 设为最后一个条目的 cursor。对调用方而言条目始终来自同一个迭代器；
- 不可重试的错误、调用方取消或服务端结束流都会结束订阅。

每次重新打开都是普通的 `call.open`：可能到达不同实例，拦截器能看到它，由服务端负责遵守 `resume`。服务端无法做到时（历史已不覆盖该 cursor）应以不可重试错误结束调用，而不是悄悄跳过事件。Python 与 TypeScript 运行时的 `Topic[T]` 正是这样做的：落后太多的订阅者以可重试的 `resource_exhausted`（`reason: "slow_subscriber"`）被断开，其客户端随后从 cursor 处重新订阅；早于历史缓冲的 cursor 以不可重试的 `invalid_argument`（`reason: "cursor_expired"`）被拒绝。通过 HTTP 时订阅就是普通服务端流，调用方需自行携带 cursor 恢复。

详见英文版 [Subscriptions](../protocol.md#subscriptions)。

---

## 心跳保活（Liveness）

为了在所有传输层与运行环境（含浏览器）下表现一致，MeshCall 使用协议层 `ping`/`pong` 帧保活，而非传输层机制：
- **主动连接方（客户端，或连接到 Router 的服务实例）**：在连接空闲满 `ping_interval`（默认 15 秒）时发送 `ping`；若在 `ping_timeout`（默认 10 秒）内未收到匹配 `nonce` 的 `pong`，则断开连接，连接上的活跃调用以可重试的 `unavailable` 失败。
- **被动接受方（Router 或直连服务端）**：不主动发 `ping`，但会关闭超过 `idle_timeout`（默认 45 秒，必须大于 `ping_interval + ping_timeout`）未收到任何帧的连接。

每个节点都必须以相同 `nonce` 的 `pong` 回应 `ping`，且优先于数据帧发送；没有对应 `ping` 的 `pong` 被忽略。

---

## 标准错误码与校验违规详情（Errors）

协议错误使用稳定的机器可读错误码。默认不发送内部异常详情与堆栈。业务错误可以使用带命名空间的错误码，但仍须使用统一的错误信封。

| 错误码 (`code`) | 含义 | 常见来源 |
| --- | --- | --- |
| `cancelled` | 调用被调用方或对端取消 | client, server |
| `deadline_exceeded` | `timeout_ms` 或 `idle_timeout_ms` 超时 | server, Router, client |
| `invalid_argument` | 请求参数或输入流条目未通过校验 | server |
| `method_not_found` | 未知的服务名或方法名 | server, Router |
| `permission_denied` | 该身份（包括匿名用户）无权进行此调用；无论目标是否存在，结果都相同 | Router |
| `unauthenticated` | 提供了凭据但凭据无效；在建立连接时拒绝 | Router |
| `resource_exhausted` | 达到容量上限；通常可重试 | Router, 传输层 |
| `unavailable` | 没有可用实例，或连接丢失；通常可重试 | Router, client |
| `protocol_error` | 帧违反本规范 | 任意节点 |
| `internal` | 服务端未进一步说明的意外失败，包括未通过服务自身校验的响应或输出条目 | server |

`details` 是可选对象，运行时使用 `source`（`"client"`、`"router"`、`"server"`）、`reason` 与 `instance_id`。Router 合成的错误携带 `source: "router"` 与以下 `reason` 之一：

| `reason` | 所属错误码 | 含义 |
| --- | --- | --- |
| `no_instances` | `unavailable` | 该服务没有任何已注册实例 |
| `instance_lost` | `unavailable` | 处理中的实例断开（`instance_id` 指明是哪个） |
| `overloaded` | `resource_exhausted` | 达到容量上限：Router 自身的上限，或所有实例都已达 `max_inflight` |
| `circuit_open` | `unavailable` | 所有实例都被熔断器排除 |
| `cancelled` | `cancelled` | 加入的幂等调用被其首个调用方取消 |
| `instance_unknown` / `instance_draining` | `unavailable` | 定向调用指定的实例未注册 / 正在离开 |
| `instance_required` | `invalid_argument` | 定向服务的调用缺少 `instance_id` |
| `idempotency_key_on_duplex` | `invalid_argument` | duplex 调用携带了幂等键 |
| `flow_control_violation` | `protocol_error` | 流条目超出消费方授予的信用（见[流量控制](#流方向与流量控制flow-control)） |
| `router_draining` / `router_shutdown` | `unavailable` | Router 正在停止（见 [Router 停机](#router-停机router-shutdown)） |

直连服务端使用同样的 reason，但 `source: "server"`；服务端执行自身 `max_inflight` 时返回 `resource_exhausted`（`source: "server"`，`reason: "overloaded"`）。服务端的 `Topic` 还会使用 `slow_subscriber` 与 `cursor_expired`（见[订阅](#订阅subscriptions)）。客户端重试策略在进行了多次尝试时向最终错误加入 `attempts`。

### 结构化校验错误（`details.violations`）

服务端会依据契约校验请求、每个输入流条目、响应与每个输出流条目。返回的错误取决于责任方：

- 请求或输入条目无效 → `invalid_argument`，`details.source = "server"`，`details.reason` 为 `"request"` 或 `"input_item"`，并在 `details.violations` 中提供 `[{path, code, message}]` 列表（`path` 为点分字段路径，如 `"profile.tags.0"`，根为 `""`；`code` 为简短校验关键字，如 `type`、`missing`、`int_parsing`）。信封中的 `message` 固定为 `Invalid request` / `Invalid input stream item`。
- 响应或输出条目无效 → `internal`，`details.reason = "invalid_response"` / `"invalid_output_item"`。这是服务的 bug，违规详情只记录在服务端日志中，不发送。
- 持有契约的客户端也会校验收到的内容。未通过*客户端* Schema 的响应或输出条目在本地报告为 `protocol_error`（`details.source = "client"`，`details.reason = "invalid_response"` / `"invalid_output_item"`，附 `violations`）：说明双方契约不一致（版本偏移），重试无法解决。

**校验错误的任何字段都绝不包含被拒绝的原始输入值。** 运行时为每个错误码提供类型化子类（`InvalidArgumentError`、`DeadlineExceededError` 等），调用方无需比较字符串。

---

## HTTP 调用（HTTP calls）

Router 在同一监听地址上也接受普通 HTTP 请求形式的调用，任何 HTTP 客户端无需 MeshCall 运行时即可调用网格。每个请求就是一次调用，Router 把它翻译成上述帧，因此权限检查、负载均衡、超时、熔断、去重与定向调用的行为与 WebSocket 调用方完全一致。HTTP **完整支持一元调用与服务端流，有限支持客户端流，不支持 duplex**。路径不以 `/v1/` 开头的请求仍是 WebSocket 升级（`/healthz` 与 `/readyz` 除外）。该映射在 `/v1` 下保持稳定，破坏性变更需要新的路径前缀。

- **请求**：`POST /v1/call/{service}/{method}`。请求头 `Authorization`（`Basic …` 账号、`Bearer mk_…` API Key 或 `Bearer mt_…` 临时 Token；不带则为 `anonymous` 用户）、`MeshCall-Timeout-Ms`（不超过 `--http-max-timeout`）、`MeshCall-Idle-Timeout-Ms`、`Idempotency-Key`、`MeshCall-Instance`（定向调用）、`MeshCall-Meta-<key>`（元数据，key 转为小写）。未给出超时时，一元与客户端流调用使用 `--http-default-timeout`（默认 30 秒），服务端流没有默认超时。
- **请求体**：一元与服务端流为 JSON 请求负载（空请求体即 `{}`，不检查 Content-Type）；客户端流为 `Content-Type: application/x-ndjson`，第一行是请求负载，其后每个非空行是一个输入条目，Router 只在实例授予信用后才读取下一行；duplex 返回 `405` 与 `method_not_found`（`details.reason = "duplex_over_http"`）。客户端断开请求即发送 `call.cancel`。
- **响应**：一元调用为 `200` 与 JSON 结果，每个 Trailer 是一个 `MeshCall-Trailer-<key>` 响应头；服务端流为 `200` 与 `application/x-ndjson`，每行一个信封 `{"item": …}`，最后一行是 `{"end": {"trailers": …}}`，或在出错时为 `{"error": …, "trailers": …}`，缺少结束行说明响应被截断。
- **错误状态码**：首个条目之前的错误以 `{"error": {...}}` 为响应体，`invalid_argument` 400、`unauthenticated` 401、`permission_denied` 403、`method_not_found` 404（`duplex_over_http` 为 405）、`resource_exhausted` 429、`cancelled` 499、`internal` / `protocol_error` / 业务错误码 500、`unavailable` 503、`deadline_exceeded` 504。超过 `--http-max-inflight` 的并发 HTTP 调用收到 `429`（`overloaded`）与 `Retry-After`。
- **发现与健康**：`GET /v1/services`（Registry `list`）、`GET /v1/services/{service}`（`describe`）与 `GET /v1/openapi.json`（OpenAPI 3.1）需要 Registry 权限，且只显示调用方有权调用的内容；`GET /healthz` 与 `GET /readyz`（停机时 `503`）无需凭据。
- **浏览器**：跨域调用只对 `--http-cors-origin` 列出的源开放；从不使用 Cookie。浏览器无法在 WebSocket 上设置 `Authorization` 头，因此 Web 客户端使用 HTTP：TypeScript 包的 `meshcall/web` 入口（`MeshCallHttpClient`）基于 `fetch` 实现该映射，生成的 TypeScript 客户端可直接使用它。
- **部署**：反向代理不应缓冲客户端流的请求体与服务端流的响应（nginx：`proxy_request_buffering off`、`proxy_buffering off`；Router 在流式响应上发送 `X-Accel-Buffering: no`）。

完整映射详见英文版 [HTTP calls](../protocol.md#http-calls)。

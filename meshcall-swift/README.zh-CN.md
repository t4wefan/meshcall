# MeshCall for Swift

[English](./README.md) | **简体中文**

`meshcall-swift` 是 [MeshCall](../README.zh-CN.md) 的 Swift 客户端运行时：iOS 和
macOS 应用通过 MeshCall Router，在一条 WebSocket 上调用用 Python、TypeScript 或 Go
写的服务。调用方式、错误、超时、流控、重连规则、隧道和端到端加密都与其他运行时一致
（见 [`docs/protocol.md`](../docs/zh-CN/protocol.md)）。它是一个 Swift 6 包，除
Foundation 和 CryptoKit 外没有依赖（iOS 16+、macOS 13+）。

这个运行时**只有客户端**：Swift 暂时不能注册服务。

## 安装

用本地路径添加（本仓库的检出目录或 submodule）：

```swift
// Package.swift
dependencies: [.package(name: "MeshCall", path: "../meshcall/meshcall-swift")],
targets: [.target(name: "App", dependencies: [.product(name: "MeshCall", package: "MeshCall")])]
```

## 生成客户端

无论服务用哪种语言实现，都可以从它的契约生成类型化客户端：

```bash
meshcall generate service.meshcall.json --language swift --output Sources/App/Generated
uv run meshcall generate my_app.service:Chat --language swift --output Sources/App/Generated
```

结果是一个文件（`ChatClient.swift`）：每种载荷对应一个 `Codable`、`Sendable`、
`Hashable` 结构体，再加一个 `async` 客户端，方法参数与处理函数的参数一致。不要手改，
需要时重新生成。

```swift
import MeshCall

let mesh = try Mesh(Router("wss://router.example"), options: MeshOptions(credentials: .bearer("mk_…")))
let chat = ChatClient(mesh)

let reply = try await chat.send(session: "s1", text: "hello")           // 一元调用
for try await event in try await chat.watch(session: "s1") {           // 服务端流
    render(event)
}
let total = try await chat.upload(name: "a.txt", items: chunks)         // 客户端流（任意 AsyncSequence）

let talk = try await chat.converse(session: "s1")                      // 双向流
try await talk.send(Line(text: "hi"))
for try await line in talk { print(line.text) }
```

单次调用的选项通过 `withOptions` 设置，方法签名保持和处理函数一致：
`chat.withOptions(CallOptions(timeout: .seconds(5), metadata: ["tenant": "t1"]))`。
不用生成的客户端时，`mesh.unary(service, method, request, as:)`、`serverStream`、
`clientStream` 和 `duplex` 接受任意 `Codable` 类型。

流都是 `AsyncSequence`。额度跟着消费走：Router 只允许服务端发送客户端已经读走的数量
（再加 16 条），读得慢的客户端会让服务端放慢，而不是在内存里堆积。提前跳出
`for try await` 循环、取消任务或释放流对象，都会取消这次调用。

## 隧道和端到端加密

签发方（你的服务端，或 Effi Cloud 的 `POST /api/instances/{id}/tunnel`）返回 Router
地址、隧道凭据，端到端隧道还会带上实例的公钥。`TunnelGrant` 可以直接解码这个返回：

```swift
let grant = try JSONDecoder().decode(TunnelGrant.self, from: body)
// {router_url, credential, instance_id, tunnel_id, e2e_key}
let mesh = try Mesh(tunnel: grant)
let sessions = SessionsClient(mesh)
```

隧道连接上只能看到这个实例在授权范围内的服务，实例也是隐含的。带 `e2e_key` 时，每个调用都
朝固定的公钥加密（`Noise_NK_25519_AESGCM_SHA256`；配了客户端身份时用 IK，见下文）：Router 负责路由和检查范围，但看不到
载荷、元数据和错误信息。客户端从不从 Router 获取公钥；握手失败时返回
`unauthenticated` / `e2e_handshake_failed`，绝不退回明文。端到端凭据（`mtun_e2e.…`）
没有配公钥，或者普通凭据配了公钥，都会在连接前直接拒绝。

### 客户端认证

默认情况下客户端没有自己的密钥（NK），能不能连接只由 Router 决定。客户端有了自己的 X25519
密钥后，也会向实例证明自己的身份，实例就可以拒绝它不认识的客户端
（`Noise_IK_25519_AESGCM_SHA256`）。密钥生成一次并妥善保存（例如放在钥匙串里）；离开设备的
只有它的公钥，而且只出现在握手里：

```swift
let key = E2EKeyPair.generate()          // 保存 key.privateKey
let mesh = try Mesh(tunnel: grant, identity: E2EIdentity(privateKey: key.privateKey, instanceID: grant.instanceID))
```

要和一个还不认识这个密钥的实例配对，再加上实例主人在带外给出的一次性配对信息（例如实例设备上
显示的二维码，绝不经过 Router）：配对 id 和 32 字节的 PSK。这时第一次握手用
`Noise_IKpsk2_25519_AESGCM_SHA256`，并在第一个调用之前确认（实例可能要等人确认，所以确认最多
等 120 秒）；实例接受之后，这个 `Mesh` 之后的每次握手（包括重连）都用 IK。已经配对过的客户端
新建 `Mesh` 时，传不带 `pairing` 的身份即可。

```swift
var grant = try JSONDecoder().decode(TunnelGrant.self, from: body)
grant.e2eKey = qr.instanceKey            // 用二维码里固定的公钥，不用签发方给的副本
let mesh = try Mesh(
    tunnel: grant,
    identity: E2EIdentity(
        privateKey: key.privateKey, instanceID: qr.instanceID,
        pairing: E2EPairing(id: qr.pairingID, psk: qr.psk)))
```

如示例所示，自己固定了实例公钥和 id 的应用不需要签发方给的副本：把 `grant.e2eKey` 和身份的
`instanceID` 换成自己的值即可（签发方仍会在 `tunnels.open` 时固定公钥，公钥不是实例自己的，
实例会拒绝）。客户端绝不从 IK 或 IKpsk2 退回 NK。密钥不被认识、配对已用过或 PSK 不对，都返回
`unauthenticated` / `e2e_handshake_failed`（几种情况的回答完全一样，实例不会透露它认识哪些
密钥）；身份没有配 `e2eKey`，或者密钥、PSK 格式不对、`instanceID` 为空、配对 id 不在 1–128
字节之间，都会在连接前直接拒绝。`E2EIdentity` 和 `E2EPairing` 的 `description` 不包含密钥。

## 错误

所有失败都是 `MeshCallError`，带固定的 `code`、`message` 和 `details`（`source`、
`reason`、`violations` 等）。按错误码判断：

```swift
do {
    try await chat.send(session: "s1", text: "hi")
} catch let error as MeshCallError where error.code == .unavailable && error.retryable {
    // 连接断了：可以重试
} catch let error as MeshCallError where error.reason == "tunnel_revoked" {
    // 找签发方要新的授权
}
```

响应无法解码成本客户端的类型时，返回 `protocol_error`，`reason: "invalid_response"`
（两端契约版本不一致）。只检查 `Codable` 能检查的：类型、必填字段和 null。TypeScript
客户端会按完整的 JSON Schema 校验收到的数据，这里不同：`minimum`、`pattern`、枚举取值等
约束不检查（枚举按开放集合解码，未知的值会保留）。被吊销或拒绝的凭据会被记住：之后的调用
立即返回同样的错误，不会再重连。

## 连接、重连和应用生命周期

`Mesh` 在第一次调用时建立 WebSocket。连接断开时，进行中的调用以可重试的 `unavailable`
失败；下一次调用按指数退避重连（`MeshOptions.reconnect`），最多等到这次调用自己的
`timeout`。幂等的一元调用，或带 `idempotencyKey` 的一元调用，会按内置的重试策略重试。
流不会续传：断线后重新 `watch`。

保活 ping 每 15 秒一次。被系统挂起的应用回到前台时，连接可能已经没人应答；在回到前台时调用
`checkConnection()`，这样失效的连接会立即断开（流失败，watcher 重新订阅），而不用等保活超时：

```swift
.onChange(of: scenePhase) { _, phase in
    if phase == .active { Task { await mesh.checkConnection() } }
}
```

## 原样转发 JSON

`JSONValue` 表示任意 JSON 值。需要把事件原样转发（例如转给 WebView 里的渲染包）的应用可以
不解码直接接收：
`mesh.serverStream("aa.v3.Sessions", "watch", ["id": "s1"], of: JSONValue.self)`，然后
`try event.jsonData()`。

## 选项

`MeshOptions`：`credentials`（API Key、临时令牌和隧道凭据用 `.bearer`，账号用 `.basic`）、
`e2eKey`、`e2eIdentity`、`maxFrameSize`（64 MiB）、`handshakeTimeout`（10 秒）、`deadlineGrace`
（`max(1 秒, 10%)`）、`pingInterval` / `pingTimeout`（15 秒 / 10 秒）、`reconnect`、
`retry`、`interceptors`（每次尝试都会经过；一元调用的 trailer 在 `info.trailers` 里）和默认的
`callOptions`。传多个 Router 地址可以故障切换：`Router(urls: ["wss://a", "wss://b"])`。

凭据放在 `hello` 帧里发送，这是协议为无法在升级请求上设置 `Authorization` 的对端规定的方式。

## 暂未支持

注册服务、HTTP 映射（`https://` Router）、Unix socket 和命名管道、OpenTelemetry，以及 Linux
（CryptoKit 和 `URLSessionWebSocketTask` 是 Apple 的框架）。

## 测试

```bash
DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer swift test
scripts/check.sh swift   # 另外还有：iOS 构建、生成代码的类型检查、demo/ts2swift
```

测试运行在共享的客户端环境 `meshcall-conformance env` 上（见
[`conformance/CLIENTS.md`](../meshcall-router/conformance/CLIENTS.md)）：真实的 Router、
一个能让连接停住的代理和一个一致性 fixture。测试覆盖一致性套件的全部客户端场景，每个测试都标有
`// conformance-scenario: <id>`：所有调用形态、额度、超时、取消、元数据、帧大小、隧道、
端到端加密、吊销、客户端认证和配对、Router 重启和回到前台时的检查。NK、IK 和 IKpsk2 握手用与
其他运行时相同的测试向量验证（cacophony 向量和 `conformance/testdata/e2e_handshakes.json`）。

默认用 Go 的 fixture（没有设置 `MESHCALL_FIXTURE_BINARY` 时自动 `go build`）；用
`MESHCALL_FIXTURE` 可以让同一套测试跑在其他运行时的服务端上：

```bash
MESHCALL_FIXTURE="node meshcall-ts/dist/conformance/fixture.js" swift test
MESHCALL_FIXTURE="uv run --project meshcall-py --locked python meshcall-py/conformance/fixture.py" swift test
```

（命令在仓库根目录下执行。）

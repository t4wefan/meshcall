# MeshCall JVM 运行时（Kotlin、Java、Android）

[English](README.md) | **简体中文**

`meshcall-jvm` 是 [MeshCall](../README.zh-CN.md) 的 JVM 客户端运行时：经 MeshCall
Router 调用 Python、TypeScript 或 Go 编写的服务，通过 WebSocket 讲 `meshcall/1`，
行为与其他运行时一致：帧、错误码、`details` 键、超时、流控、重连和存活检测都相同。
它用 Kotlin 编写，基于协程和 `Flow`；Java 调用方使用 `CompletableFuture` 和回调。
可在 Android（minSdk 24）和任意 JVM 上运行（Java 11 字节码，测试在 JDK 17 上跑）。

这一版只提供**客户端**，不能注册服务。

| | |
| --- | --- |
| 调用 | unary、server stream、client stream、duplex；按实例定向调用（`instance`） |
| Router 功能 | 隧道（`mtun_…`）、端到端加密隧道（Noise NK，固定实例公钥）、令牌、Registry、AuthService、TunnelService |
| 传输 | `ws://`、`wss://`（OkHttp），可给多个 Router 做故障切换 |
| 代码生成 | `meshcall generate … --language kotlin`：kotlinx.serialization 数据类，suspend / `Flow` 方法，以及给 Java 用的 `…Async` 方法 |
| 依赖 | kotlinx-coroutines、kotlinx-serialization-json、OkHttp、BouncyCastle（只用 X25519） |

## 安装

坐标是 `io.github.t4wefan:meshcall`。发布之前，从本仓库以 included build 的方式构建：

```kotlin
// 应用的 settings.gradle.kts
includeBuild("path/to/meshcall/meshcall-jvm")
// build.gradle.kts
dependencies { implementation("io.github.t4wefan:meshcall:0.3.0") }
plugins { kotlin("plugin.serialization") } // 生成的客户端使用 kotlinx.serialization
```

## 生成的客户端

可以从任意服务的契约生成类型化客户端：Python 的 `module:Class`，或者 TypeScript、Go
服务导出的 `*.meshcall.json`。

```bash
uv run meshcall generate my_app.service:Chat --language kotlin \
  --package-name com.example.chat --output app/src/main/kotlin/com/example/chat
meshcall generate chat.meshcall.json --language kotlin --package-name com.example.chat --output Chat.kt
```

生成结果是单个文件 `<Name>Client.kt`（`demo.v1.Chat` → `ChatClient`）：每个载荷类型是一个 `@Serializable` 类，
客户端类的方法和服务端的业务参数一一对应：

```kotlin
val mesh = Mesh(Router("wss://router.example.com"), MeshOptions(token = "mk_…"))
val chat = ChatClient(mesh).withOptions(CallOptions.DEFAULT.withTimeoutMs(10_000))

val reply = chat.send(text = "hi")                         // unary：suspend fun
chat.watch(room = "general").collect { println(it) }        // server stream：Flow
val total = chat.upload(name = "a.txt", items = chunks)     // client stream：传入 Flow
chat.talk().use { call ->                                   // duplex
    launch { call.output.collect { println(it) } }          // 一边发一边读
    call.send(Line("hello")); call.closeSend(); call.await()
}
mesh.close()
```

| JSON Schema | Kotlin |
| --- | --- |
| `integer` / `number` / `string` / `boolean` | `Long` / `Double` / `String` / `Boolean` |
| 数组、带 `additionalProperties` 的对象 | `List<T>`、`Map<String, T>` |
| 带属性的对象 | `data class`；内联对象生成嵌套类（`Request.Diff`） |
| 字符串 `enum` | `String` 的 `typealias`，外加列出已知取值的 `…Values` 对象（新版本服务端可能增加取值） |
| 非必填，或允许 `null` | `T? = null`；不传的字段由服务端默认值生效。带 schema 默认值的参数以它作为 Kotlin 默认值 |
| 多种类型的联合 | `JsonElement` |

**定向（targeted）服务**的方法第一个参数是 `instance: String`，由编译器保证不会漏传。
标注为幂等的方法会为重试策略设置 `idempotent`。不使用代码生成时，`Mesh` 直接收发
`JsonElement`（`mesh.unary(service, method, request)`），`io.github.t4wefan.meshcall`
包里的扩展函数提供带序列化器的版本。

### Java

每个方法都有一个给 Java 用的 `…Async` 版本：unary 和 client stream 返回
`CompletableFuture`（取消 future 即取消调用）；server stream 把条目交给
`StreamObserver`，返回 `CallHandle`；duplex 返回 `JavaDuplexCall`，它的 `send` /
`closeSend` 也返回 future。

```java
try (Mesh mesh = new Mesh(new Router("ws://127.0.0.1:8765"))) {
    ChatClient chat = new ChatClient(mesh);
    Reply reply = chat.sendAsync("hi").get();
    CallHandle handle = chat.watchAsync("general", new StreamObserver<Event>() {
        public void onNext(Event event) { … }
        public void onCompleted(Map<String, String> trailers) { … }
        public void onError(Throwable error) { … }
    });
}
```

为什么选 future 加回调：`CompletableFuture` 从 JDK 8、Android API 24 起都有，回调不
需要引入任何响应式库。流控也照常生效：`onNext` 返回之后才发放下一个条目额度，观察者
处理得慢，生产方就会跟着变慢。拦截器只能用 Kotlin 写，因为它是 suspend 函数。

## 隧道与端到端加密

隧道凭据只能访问一个服务实例。端到端隧道（凭据以 `mtun_e2e.` 开头）会把客户端和该
实例之间的载荷、元数据和错误信息全部加密，Router 只能看到密文：

```kotlin
val mesh = Mesh(
    Router("wss://router.example.com"),
    MeshOptions(tunnel = credential, e2eKey = instancePublicKey), // 两者都来自签发方
)
val sessions = SessionsClient(mesh)                // instance_id 由 Router 补上
```

客户端始终朝固定（pinned）的公钥发起 `Noise_NK_25519_AESGCM_SHA256` 握手，从不使用
Router 提供的公钥。公钥不对时，调用以不可重试的 `UnauthenticatedException`
（`reason = "e2e_handshake_failed"`）失败，绝不会退回明文。端到端凭据没配 `e2eKey`，
或者普通凭据配了 `e2eKey`，在构造 `Mesh` 时就会被拒绝。实例重启丢失会话后
（`e2e_session_unknown`），客户端会在新连接上重新握手，并把 unary 调用重试一次。
隧道被吊销后，进行中的调用以 `UnauthenticatedException`（`reason = "tunnel_revoked"`）
失败，之后的调用也会立即以同样的错误失败。

X25519 用的是 BouncyCastle 的纯 Java RFC 7748 实现，直接调用，不注册 JCA Provider，
因此不会和 Android 自带的那份冲突。JCA 的 `XDH` 要到 Android API 33 才有。AES-256-GCM、
SHA-256 和 HMAC 来自 JCA，所有 Android 版本都可用。实现已经用一致性测试工具同一份
公开的 Noise 测试向量验证过。

## 调用、错误与超时

- `CallOptions`（`withTimeoutMs`、`withIdleTimeoutMs`、`withMetadata` /
  `withMetadataEntry`、`withIdempotencyKey`、`withIdempotent`、`withInstance`）可以
  按次传入，也可以通过 `Mesh` 或生成客户端的 `withOptions` 统一设置。以 `meshcall-`
  开头的元数据键会在本地被拒绝（`reserved_metadata`）。
- 每个协议错误码都有对应的异常：`InvalidArgumentException`（带 `violations`）、
  `DeadlineExceededException`、`UnavailableException`、`CallCancelledException` 等；
  应用自定义的错误码则是 `MeshCallException`。`source`、`reason`、`instanceId`、
  `attempts` 读取 `details` 里对应的键。
- `timeoutMs` 涵盖建立连接和调用本身。服务端的截止时间过后，客户端再等
  `max(1 s, 10 %)`，然后在本地判定失败（`source = "client"`）并发送 `call.cancel`。
  `idleTimeoutMs` 限制相邻两个输出条目之间的间隔（`reason = "idle"`）。
- 取消发起调用的协程，或者提前结束对 `Flow` 的收集，都会发送 `call.cancel`。server
  stream 是冷 `Flow`：每收集一次就打开一次调用，所以 `watch(id).retry { it is
  UnavailableException }` 可以在断线后重新 watch。`collectWithTrailers { … }` 返回服务端
  的 trailers。
- 重试策略只重发失败的 **unary** 调用，条件是：错误可重试，错误码在策略的集合里
  （`unavailable`、`resource_exhausted`），并且方法是幂等的或调用带有幂等键。流式调用
  从不重试。
- 超过 `maxFrameSize`（64 MiB）的帧不会发出，只有这一次调用失败（`resource_exhausted`，
  `frame_too_large`）。OkHttp 单条消息最多发 16 MiB，所以在 JVM 上这才是实际的发送上限；
  更大的数据请用流传输。

## 连接、重连与 Android 生命周期

一个 `Mesh` 持有一条多路复用的 WebSocket，在第一次调用（或 `connect()`）时建立。客户端
每 15 秒 ping 一次，pong 迟到 10 秒就断开连接。连接断开时，其上的调用以可重试的
`unavailable` 失败；下一次调用会用带抖动的指数退避重连（`ReconnectPolicy`），最多等到
这次调用自己的超时。调用从不自动重放。给了多个 Router 地址时，客户端从上次成功的那个
开始依次尝试。Router 以 4401 关闭连接（凭据已吊销）或拒绝凭据时，错误会立即上报，
不再重试。

`close()` 只是断开连接，`Mesh` 仍然可以继续用，下一次调用会重新连接。App 切到后台再
回到前台，只需要下面这些：

```kotlin
class SessionsViewModel(private val mesh: Mesh) : ViewModel() {
    private var watching: Job? = null

    fun onForeground() {                     // ProcessLifecycleOwner ON_START
        watching = viewModelScope.launch {
            sessions.watch(id)
                .retryWhen { e, _ -> e is UnavailableException && e.retryable }
                .collect { event -> cache.apply(event) }   // 先收快照，再按 id 整条替换
        }
    }

    fun onBackground() {                     // ON_STOP
        watching?.cancel()                   // 为这个流发送 call.cancel
        mesh.close()                         // 释放 socket；之后不会重放任何调用
    }
}
```

回到前台后再次收集，客户端会重新连接（端到端隧道还会重新握手），服务端会发送新的快照。
如果系统冻结了进程却没有关闭 socket，存活检测会在 `pingIntervalMs + pingTimeoutMs`
之内发现。在后台时调用 `close()` 可以免去这段等待。

## 与其他运行时的差别

- 只有客户端，没有 `mount` / `serve`。
- 只支持 WebSocket：使用 `http(s)://`（HTTP 调用映射）、`unix://`、`pipe://` 的 Router
  地址会被拒绝，并附提示。
- 发送的帧受 OkHttp 限制，最大 16 MiB（见上文）。
- 响应和条目通过解码成生成的类来校验，与 Python 客户端借助 Pydantic 校验的方式相同；
  不匹配时抛出 `ProtocolErrorException`，`reason` 为 `"invalid_response"` 或
  `"invalid_output_item"`。

## 开发

```bash
cd meshcall-jvm
./gradlew build      # 编译、单元测试、集成测试、Kotlin 代码生成的 golden 文件
./gradlew :examples:run   # showcase 示例的 Kotlin 客户端（读取 MESHCALL_ROUTER_URL）
```

集成测试运行在共享的客户端一致性环境中：Gradle 用 `go build` 构建 `meshcall-conformance`
和 Go 一致性测试 fixture，测试再启动 `meshcall-conformance env --fixture …`，由它运行
Router 和 fixture，并接受 `restart_router` / `pause_router` / `restart_fixture` 等命令。
每个场景都标有 `// conformance-scenario: <id>`，`meshcall-router` 中的
`go test ./conformance/...` 会检查每个客户端场景都被覆盖。把 `MESHCALL_FIXTURE` 设为一条
命令即可换用其他 fixture（例如 Python 版），`MESHCALL_CONFORMANCE_BINARY` 可指定预先构建的
二进制。没有 Go 时会跳过。`examples` 模块包含生成的
showcase 客户端（由 `scripts/regenerate.sh kotlin` 生成，CI 做漂移检查），以及一个
Kotlin 调用方和一个 Java 调用方。`:meshcall` 的测试还会编译
`meshcall-router/internal/codegen/testdata` 下的所有 Kotlin golden 文件。

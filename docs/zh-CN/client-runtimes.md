# 新增一个客户端运行时

客户端运行时让另一种语言写的应用能*调用*网格上的服务（第一个是给 iOS 和 macOS 用的
`meshcall-swift/`）。它由三部分组成：运行时本身、`meshcall generate` CLI 里的一个目标、
以及一套客户端一致性测试。本页列出每部分要做什么，以 JVM（Kotlin 优先，Java 也能调用）为例。
[English](../client-runtimes.md)

## 1. 运行时

实现 [`protocol.md`](protocol.md) 中客户端这一半；对照阅读 TypeScript 客户端
（`meshcall-ts/src/client.ts`）或 Swift 客户端（`meshcall-swift/Sources/MeshCall/ClientCore.swift`，一个 actor）。

- **传输。** 每个 Router 端点一条 WebSocket，不用子协议，JSON 文本帧，上限 64 MiB
  （`frame_too_large`）。凭据放在 `hello.authorization`（`Bearer …` 或 `Basic …`），不放 HTTP 头。
  关闭码 4401/4403/4429/4503/1008 按 `meshcall-swift/Sources/MeshCall/Errors.swift` 映射成错误。
- **调用形态。** 单次、服务端流、客户端流、双向流。流的初始额度是 16，在*使用方*取走一项时才发
  `stream.window`，不在到达时发；调用方的 `idle_timeout` 由调用方自己执行。
  一个调用的控制帧不能抢在它自己的 `call.open` 前面发出。
- **超时与取消。** 发送 `timeout_ms`，本地多等 `max(1s, 10%)` 再放弃；取消时发 `call.cancel`。
- **重试。** 只对幂等方法或带幂等键的调用重试；最终错误带 `details.attempts`。
- **重连与存活。** ping、带退避的重连，以及给会挂起应用的平台用的「立即检查连接」
  （Swift 的 `checkConnection(timeout:)`；Android 上在 `onResume` 调用）。
- **隧道与端到端加密。** `TunnelGrant`（`router_url`、`credential`、`instance_id`、`tunnel_id`、
  `e2e_key`）打开一个固定到某个实例的 Mesh。`mtun_e2e.` 凭据需要
  `Noise_NK_25519_AESGCM_SHA256`，prologue 为 `meshcall-e2e/1|<凭据的 sha256 十六进制>`；
  先用 `meshcall-router/conformance/testdata/noise_nk_25519_aesgcm_sha256.json` 验证握手。
  收到 `e2e_session_unknown` 时重置连接，单次调用重试一次。
- **错误**沿用共同的错误码和 `details` 键（`source`、`reason`、`instance_id`、`attempts`、`violations`）。

JVM 上：WebSocket 用 OkHttp（或 Ktor）；调用 API 用协程（单次调用是 `suspend`，服务端流是
`Flow`，双向流是一对 channel），再为 Java 提供阻塞或 `CompletableFuture` 包装；X25519 和
AES-GCM 用 JDK（11+），较旧的 Android 用 Tink。

## 2. 代码生成目标

目标在 `meshcall-router/internal/codegen/`，一个目标一个文件（`swift.go`、`golang.go`）。共享部分：

- `targets.go`：`registerTarget(Target{Language, Extension, Golden, FileName, Render})`。
  注册后 `internal/cli/generate.go` 里的 `--language <名字>` 就能用，无需其他改动。
- `clientmodel.go`：`BuildClientModel(service, naming, clientName)` 把契约变成与语言无关的
  `ClientModel`：具名类型（`TypeDecl`：结构、枚举或别名，可嵌套）、字段（`Field`：线上键名、
  成员名、可选或必须为 null、默认值）和方法（`ClientMethod`：线上名、`Shape`、展开的 `Params`
  或整个请求类型、响应、输入、输出类型、是否幂等）。目标只负责把这个模型写成对应语言。
- `naming.go`：`camelCase(value, fallback, upper, initialisms)`，每个目标传入自己的缩写规则
  （Swift 是 `ID`、`URL` …；Kotlin 风格是 `Id`、`Url`），关键字也由目标自己转义。

所以 Kotlin 目标就是一个 `kotlin.go`：

1. `init()` 注册 `{Language: "kotlin", Extension: ".kt", Golden: "client.kt"}`；
   `FileName` 返回 `<Client>.kt`，`Render` 用 `TargetOptions.PackageName` 写 `package` 行。
2. 一个 `Naming`：Kotlin 的缩写规则、关键字转义（反引号），以及 `ReservedParams`：生成的方法
   自己要用的名字（`options`、`instance`、`rpc` …），同名的请求字段会被改名。
3. 从 `ClientModel` 渲染：每个结构一个 `@Serializable data class`（线上键名用 `@SerialName`），
   每个枚举一个 `enum class`，每个别名一个 `typealias`，再加一个客户端类，每个方法一个
   `suspend fun` 或 `Flow` 函数。
4. Golden 文件：先建空的 `testdata/*/client.kt`，运行 `go test ./internal/codegen -update`，
   读一遍后提交。测试会遍历 `Targets()`，本身不用改。
5. 在 CI 里用运行时编译这些 golden（`scripts/check.sh swift` 会用 `swiftc -typecheck`
   检查每个 `client.swift`）。

## 3. 一致性测试

按 [`conformance/CLIENTS.md`](../../meshcall-router/conformance/CLIENTS.md) 做：新增
`conformance/clients_<runtime>.go`，注册运行时及其测试目录；在测试套件里启动
`meshcall-conformance env`；每个场景对应一个测试，标上 `// conformance-scenario: <id>`。
之后 `go test ./conformance` 会一直失败，直到每个场景都已覆盖或注明理由跳过。
`MESHCALL_FIXTURE` 能让测试也跑在 TypeScript 或 Python 的 fixture 上。

## 4. 接线

- `scripts/check.sh`：加一个以运行时命名的部分，构建一致性二进制和 Go fixture，运行该运行时的
  测试并编译 golden；把它加进 `all` 和用法说明。
- `.github/workflows/ci.yml`：加一个运行该部分的 job（Swift 的在 `macos-latest` 上；JVM 的可以
  在 Linux 上配 `setup-java`）。
- `scripts/regenerate.sh`：加一个 demo 目标（Swift 的是 `ts2swift`），并在 `demo/` 里加一个通过
  生成的客户端调用 TypeScript 服务的 demo。
- 文档：`docs/router.md` 里的 `--language` 列表（中英文两份）、根目录 README、
  `skills/meshcall/SKILL.md`（之后运行 `scripts/regenerate.sh skill`），以及 `AGENTS.md`
  里的分支 domain。

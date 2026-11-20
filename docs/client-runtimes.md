# Adding a client runtime

A client runtime lets apps in another language *call* services on a mesh
(`meshcall-swift/` for iOS and macOS is the first). It is three pieces: the
runtime, a target in the `meshcall generate` CLI, and a run of the client
conformance tests. This page lists what each needs, with the JVM (Kotlin first,
callable from Java) as the example. [中文](zh-CN/client-runtimes.md)

## 1. The runtime

Implement the client half of [`protocol.md`](protocol.md); read the
TypeScript client (`meshcall-ts/src/client.ts`) or the Swift one
(`meshcall-swift/Sources/MeshCall/ClientCore.swift`, one actor) next to it.

- **Transport.** One WebSocket per Router endpoint, subprotocol-free, text
  frames of JSON, 64 MiB limit (`frame_too_large`). Credentials go in
  `hello.authorization` (`Bearer …` or `Basic …`), not in HTTP headers.
  Map close codes 4401/4403/4429/4503/1008 to errors as
  `meshcall-swift/Sources/MeshCall/Errors.swift` does.
- **Call shapes.** Unary, server stream, client stream, duplex. Streams start
  with 16 credits and send `stream.window` as the *consumer* takes items, never
  on arrival; the caller's `idle_timeout` is enforced on the caller's side.
  Control frames of a call must not overtake that call's `call.open`.
- **Deadlines and cancellation.** Send `timeout_ms`, wait `max(1s, 10%)`
  longer before giving up locally; cancelling sends `call.cancel`.
- **Retries.** Only for idempotent methods or calls with an idempotency key;
  the final error carries `details.attempts`.
- **Reconnect and liveness.** Ping, reconnect with backoff, and an explicit
  "check the connection now" for platforms that suspend apps (Swift's
  `checkConnection(timeout:)`; Android: `onResume`).
- **Tunnels and end-to-end encryption.** `TunnelGrant` (`router_url`,
  `credential`, `instance_id`, `tunnel_id`, `e2e_key`) opens a mesh pinned to
  one instance. `mtun_e2e.` credentials need
  `Noise_NK_25519_AESGCM_SHA256` with prologue
  `meshcall-e2e/1|<sha256 hex of the credential>`; check your handshake
  against `meshcall-router/conformance/testdata/noise_nk_25519_aesgcm_sha256.json`
  first. On `e2e_session_unknown`, reset the connection and retry a unary call
  once. A client with a key of its own (`E2EIdentity`: its private key, the
  instance id, optionally a pairing id and PSK) uses
  `Noise_IK_25519_AESGCM_SHA256`, or `Noise_IKpsk2_25519_AESGCM_SHA256` plus
  the `{session, confirm}` call while pairing, with the prologue
  `meshcall-e2e/1|<sha256 hex>|<instance_id>`; after a confirmed pairing every
  later handshake is IK, and it never falls back to NK. Check it against the
  IK and IKpsk2 vectors next to the NK one and against
  `testdata/e2e_handshakes.json` (requests, answers, session ids, the
  confirmation), and cover the `e2e.ik` / `e2e.pairing…` client scenarios.
- **Errors** keep the shared codes and `details` keys (`source`, `reason`,
  `instance_id`, `attempts`, `violations`).

For the JVM: OkHttp's WebSocket (or Ktor), coroutines for the call API
(`suspend` unary, `Flow` for server streams, a channel pair for duplex) with
blocking/`CompletableFuture` wrappers for Java, and X25519 + AES-GCM from the
JDK (11+) or Tink on older Android.

## 2. The codegen target

Targets live in `meshcall-router/internal/codegen/`, one file each
(`swift.go`, `golang.go`). The shared parts:

- `targets.go`: `registerTarget(Target{Language, Extension, Golden, FileName,
  Render})`. Registration makes `--language <name>` work in
  `internal/cli/generate.go` with no further change.
- `clientmodel.go`: `BuildClientModel(service, naming, clientName)` turns a
  contract into a language-neutral `ClientModel`: named types (`TypeDecl`:
  struct, enum or alias, with nested types), fields (`Field`: wire key,
  member name, optional / required-null, default), and methods
  (`ClientMethod`: wire name, `Shape`, flattened `Params` or a whole request
  type, response / input / output types, idempotency). A target only spells
  this model.
- `naming.go`: `camelCase(value, fallback, upper, initialisms)`, so each target
  passes its own initialisms (`ID`, `URL`, … in Swift; Kotlin style is `Id`,
  `Url`) and escapes its own keywords.

A Kotlin target is therefore `kotlin.go`:

1. `init()` registers `{Language: "kotlin", Extension: ".kt", Golden: "client.kt"}`;
   `FileName` gives `<Client>.kt`, `Render` uses `TargetOptions.PackageName`
   for the `package` line.
2. A `Naming` with Kotlin initialisms, a keyword escape (backticks), and
   `ReservedParams` for the names generated methods use themselves (`options`,
   `instance`, `rpc`, …), so a request field of that name is renamed.
3. A renderer from `ClientModel`: `@Serializable data class` per struct (with
   `@SerialName` for wire keys), `enum class` per enum, `typealias` per alias,
   and a client class with one `suspend fun` / `Flow` function per method.
4. Goldens: create empty `testdata/*/client.kt`, run
   `go test ./internal/codegen -update`, read them, then commit them. The test
   loops over `Targets()`, so it needs no change.
5. Compile the goldens in CI against the runtime (`scripts/check.sh swift`
   type-checks every `client.swift` with `swiftc -typecheck`).

## 3. Conformance

Follow [`conformance/CLIENTS.md`](../meshcall-router/conformance/CLIENTS.md):
add `conformance/clients_<runtime>.go` registering the runtime and the
directory of its tests, start `meshcall-conformance env` from the test suite,
and mark one test per scenario with `// conformance-scenario: <id>`.
`go test ./conformance` then fails until every scenario is covered or skipped
with a reason. `MESHCALL_FIXTURE` runs the tests against the TypeScript or
Python fixture too.

## 4. Wiring

- `scripts/check.sh`: a part named after the runtime that builds the
  conformance binary and the Go fixture, runs the runtime's tests, and compiles
  the goldens; add it to `all` and to the usage line.
- `.github/workflows/ci.yml`: a job running that part (Swift's runs on
  `macos-latest`; a JVM job can run on Linux with `setup-java`).
- `scripts/regenerate.sh`: a demo target (Swift's is `ts2swift`), and a demo
  in `demo/` that calls a TypeScript service through the generated client.
- Docs: the `--language` list in `docs/router.md` (both languages), the root
  READMEs, `skills/meshcall/SKILL.md` (then `scripts/regenerate.sh skill`),
  and a branch domain in `AGENTS.md`.

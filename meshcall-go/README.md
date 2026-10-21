# MeshCall for Go

`github.com/t4wefan/meshcall/meshcall-go` is the Go runtime for
[MeshCall](../README.md): typed services and clients that speak `meshcall/1`
through the MeshCall Router or directly, interoperating with the Python and
TypeScript runtimes. It passes the same conformance suite
(`meshcall-router/conformance`) over TCP, Unix sockets and Windows named
pipes, and depends only on `github.com/coder/websocket` (plus go-winio on
Windows).

```bash
go get github.com/t4wefan/meshcall/meshcall-go
```

A release is pinned with `go get github.com/t4wefan/meshcall/meshcall-go@vX.Y.Z`
once a `meshcall-go/vX.Y.Z` tag exists: Go tags for a module in a
subdirectory carry the directory as a prefix.

## A service

Request, response and item types are structs; their JSON Schema (the
contract other languages generate clients from) comes from the struct
fields: `json` tags name properties, a field is required unless it is a
pointer or `omitempty`, and `doc:"…"`, `enum:"a,b"`, `default:"<json>"` tags
add a description, an enumeration and a default. The server validates
requests against that schema before the handler runs; clients validate what
they receive.

The recommended service is a struct embedding `meshcall.ServiceMeta`: the
runtime reflects its exported methods, derives the contract from their
parameters and return types, and injects dependencies, so there is no
registration boilerplate.

```go
type Database struct{}

func (db *Database) Save(path, content string) int { return len(content) }

type CallerID string

var (
	dbDep = meshcall.Dependency(func(ctx context.Context) (*Database, error) {
		return &Database{}, nil
	})
	callerDep = meshcall.Meta[CallerID]("x-user-id", meshcall.WithMetaRequired())
)

type FileStat struct {
	Path string `json:"path"`
	Size int    `json:"size"`
}

type Workspace struct {
	meshcall.ServiceMeta `name:"workspace.v1" description:"Remote workspace."`
}

// Write a file and return its new stat.
func (w *Workspace) WriteFile(ctx context.Context, path, content string, db *Database, caller CallerID) (FileStat, error) {
	return FileStat{Path: path, Size: db.Save(path, content)}, nil
}

// Stream a file's lines.
func (w *Workspace) TailFile(ctx context.Context, path string, lines int) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) { /* … */ }
}
```

The first parameter after the receiver must be `context.Context` or
`*meshcall.Context`; the Go method name becomes the RPC name in snake_case.
Ordinary parameters become request properties in declaration order, keeping
the Go parameter names on the wire, and pointer parameters are optional.
The return shape picks the method kind: `(Resp, error)` or `Resp` is unary,
`iter.Seq2[T, error]` is a server stream, a trailing `iter.Seq2[T, error]`
parameter is a client stream, and a trailing
`*meshcall.DuplexChannel[In, Out]` parameter (returning `error`) is duplex.
The doc comment becomes the method description; a line `@idempotent`,
`@readonly` or `@destructive` in it sets the annotation, and
`WithMethodOptions` attaches options programmatically. The `ServiceMeta`
tags also accept `targeted:"true"`,
`balance:"round_robin|least_inflight|random|disabled|sticky:<field>"` and
`module:"…"`; a `name` without a version segment gets `.v0`.

Any parameter whose type is registered with `WithDependencies` is injected
per call and stripped from the contract. `Dependency` providers may take
`context.Context` or `*meshcall.Context` plus other dependencies, and return
`T`, `(T, error)` or `(T, cleanup, error)` where `cleanup` is `func()` or
`func() error`; each type is built once per call and its cleanup runs when
the call ends. `Meta[T]("header")` injects a
metadata header — define a distinct type per header, such as `type CallerID
string`. A missing `WithMetaRequired()` header fails with `invalid_argument`
before any other dependency is acquired; `WithMetaDefault(v)` supplies a
fallback. Circular dependencies fail at registration.

Mount the service on a `*meshcall.Mesh` and start it with `mesh.Run(...)` (which blocks until SIGINT or SIGTERM and then drains in-flight calls) or `mesh.Serve(ctx, ...)`:

```go
mesh := meshcall.NewMesh(meshcall.NewRouter("ws://127.0.0.1:8765"))
mesh.Mount(
	&Workspace{},
	meshcall.WithDependencies(dbDep, callerDep),
)
err := mesh.Run(meshcall.WithInstanceID("workspace-1"))
```

Or keep a background handle with `mesh.StartServing`; `Close` drains in-flight calls
(bounded by `WithDrainTimeout`, default 10s):

```go
mesh := meshcall.NewMesh(meshcall.NewRouter("ws://127.0.0.1:8765"))
mesh.Mount(&Workspace{}, meshcall.WithDependencies(dbDep, callerDep))

running, err := mesh.StartServing(ctx,
	meshcall.WithInstanceID("workspace-1"),
	meshcall.WithStrict(true),
)
defer running.Close()
```

In tests or self-contained demos, use `StartLocalRouter` (or `NewLocalRouter` + `Start`) to manage a local `meshcall-router` subprocess explicitly:

```go
router, err := meshcall.StartLocalRouter(ctx, meshcall.WithPort(0))
defer router.Stop()

mesh := meshcall.NewMesh(router)
mesh.Mount(&Workspace{}, meshcall.WithDependencies(dbDep, callerDep))
```

`mesh.Mount` accepts `*Service`, `[]*Service`, `ServiceMeta` structs (or pointers) and `WithDependencies(...)`. `meshcall.ReflectService` (and panicking `MustReflectService`) produce the same `*Service` from a struct when you need to pass it to `NewServer` or `WriteContract` directly. `NewRouter()` with no URL reads `MESHCALL_ROUTER_URL`, else `ws://127.0.0.1:8765` (or use `NewRouterWithOptions(WithUnixSocket(...))` / `WithPipe(...)`). `WithDirectListen("127.0.0.1:9000")` serves clients directly without a Router, and `WithInstanceID`, `WithMaxInflight`, `WithAccessLog`, `WithServerInterceptors`, `WithServerLogger` and `WithStrict` configure the server instance.

Services can also be built programmatically with `NewService` when methods
are registered dynamically:

```go
type SendRequest struct {
	SessionID string `json:"session_id"`
	Text      string `json:"text" doc:"what the user typed"`
}

type Message struct {
	Text string `json:"text"`
}

chat := meshcall.NewService("chat.v1.Chat", meshcall.Describe("Chat sessions."))

meshcall.HandleUnary(chat, "send", func(ctx *meshcall.Context, req SendRequest) (Message, error) {
	return Message{Text: "echo " + req.Text}, nil
}, meshcall.Doc("Append a message and return the reply."), meshcall.Idempotent())

meshcall.HandleServerStream(chat, "history", func(ctx *meshcall.Context, req SendRequest, send func(Message) error) error {
	return send(Message{Text: "…"})
}, meshcall.Doc("Replay the session."), meshcall.ReadOnly())

mesh := meshcall.NewMesh(meshcall.NewRouter("ws://127.0.0.1:8765"))
mesh.Mount(chat)
err := mesh.Serve(ctx)
```

Also `HandleClientStream` (reads a `*meshcall.Input[Item]`) and `HandleDuplex` (the
shape of last resort; it needs a reason). Return a `*meshcall.Error` to fail
a call with a code (`&meshcall.Error{Code: "chat.v1.quota_exceeded", …}`);
any other error becomes a sanitised `internal`, and a panic is recovered and
logged. `ctx` is a `context.Context` that ends with the call's deadline or
cancellation, and carries metadata, the idempotency key, trailers
(`ctx.SetTrailer`) and a call-scoped `slog` logger. Contract warnings
(W001–W006) are logged at start and fail startup with `WithStrict(true)`,
`Strict: true` or `MESHCALL_STRICT=1`. `meshcall.WriteContract(path, chat)`
writes the contract document for `meshcall generate`.

## A client

Generate a typed client directly from one or more service sources — written in
Python, TypeScript or Go — or from a `.meshcall.json` contract file. `meshcall
export` and `meshcall generate` spin up an in-process inspect-only Router,
register the discovered services, and extract their portable contract IR. For
Go, `meshcall generate` writes a single `client.go`:

```bash
meshcall generate ./internal/workspace:Workspace --language go --package-name workspaceclient --output internal/workspaceclient
meshcall generate ./internal/workspace --language python --output workspace_client.py
meshcall export ./internal/workspace --output workspace.meshcall.json
```

The file holds every payload type and a typed client whose methods mirror
the contract's business parameters one for one — injected dependencies and
metadata never appear — plus `opts ...meshcall.CallOption`. Construct it
from a `*meshcall.Mesh` or a `*meshcall.Client` (both are
`meshcall.Caller`); `WithOptions` returns a copy that applies default call
options to every call:

```go
mesh, err := meshcall.ConnectMesh(ctx,
	meshcall.NewRouter("ws://127.0.0.1:8765"),
	meshcall.WithToken("mk_…"),
)
defer mesh.Close()

workspace := workspaceclient.NewWorkspaceClient(mesh).WithOptions(meshcall.CallOptions{
	Metadata: map[string]string{"x-user-id": "u_42"},
	Timeout:  5 * time.Second,
})
stat, err := workspace.WriteFile(ctx, "/notes.txt", "hello")
```

A `Mesh` multiplexes every generated client over one connection and is a
`meshcall.Caller`, so pass it wherever a client is expected. It also exposes
the Router's built-in services via `mesh.Registry()` (`List` and `Describe` on
`meshcall.router.v1.Registry`) and `mesh.Auth()` (`Whoami`, `IssueToken` and
`RevokeToken` on `meshcall.router.v1.AuthService`). `NewMesh(router, opts...)`
builds a lazy Mesh from a `*Router` or `*LocalRouter` and `MeshOption`s
(`WithToken`, `WithCredentials`, `WithPing`, `WithReconnect`,
`WithRetry`, `WithMaxFrameSize`, …); it does no I/O until the first call.
`ConnectMesh` connects eagerly.

Without generation, the typed helpers are `meshcall.Unary`,
`ServerStream`, `ClientStream` and `Duplex` (plus
`ServerStreamSeq`, `ClientStreamSeq`, `SliceSeq` and `SliceItems` for
iterators); they take a `meshcall.Caller` (`*meshcall.Mesh` or `*meshcall.Client`).
Clients connect lazily, reconnect with backoff, fail over between several
endpoints, retry unary calls that are idempotent (or carry `WithIdempotencyKey`)
on retryable errors, validate what they receive against their own types, and
take call options `WithTimeout`, `WithIdleTimeout`, `WithMetadata`,
`WithInstance` (required for targeted services) and `IdempotentCall`. Generated
clients for targeted services take the instance ID as their second parameter and
add `WithInstance` for you; generated unary methods declared idempotent retry by
default. Streams are read with `Recv()` (`io.EOF` at the end) or
`for item, err := range stream.All()`.

## HTTP transport

When `NewRouter` is given an `http://` or `https://` URL, `NewMesh` automatically selects the HTTP client transport (`net/http`) underneath; generated clients and `mesh.Registry()` / `mesh.Auth()` work over it identically to a WebSocket Router:

```go
mesh := meshcall.NewMesh(meshcall.NewRouter("https://router.example"), meshcall.WithToken("mk_…"))
defer mesh.Close()
chat := chatclient.NewChatClient(mesh)
reply, err := chat.Greet(ctx, "Alice")
services, err := mesh.Registry().List(ctx)
```

Duplex calls are WebSocket-only (`method_not_found`, reason `duplex_over_http`), and `mesh.Mount` panics if called on an HTTP-backed `Mesh`. Unary retries, server/client streams and the built-in calls work as over WebSocket.

## Credentials, interceptors and draining

`WithToken("…")`, `WithCredentials(BearerToken(…))` and `WithCredentials(BasicAuth(…))`
set the Router credential on `NewMesh` and `ConnectMesh`;
`ClientOptions.Authorization` and `ServerOptions.Authorization` do the same
for hand-built clients and servers. The value is sent with the WebSocket
upgrade or HTTP request (see [Router authentication](../docs/router.md)).

`WithClientInterceptors` and `WithServerInterceptors` attach interceptors to
a `Mesh` and to `Serve`/`StartServing`/`Run` respectively;
`ClientOptions.Interceptors` (`[]meshcall.ClientInterceptor`) and
`ServerOptions.Interceptors` (`[]meshcall.ServerInterceptor`) do the same on
hand-built values. They wrap every call, the first one outermost; call
metadata carries what they add, such as a W3C `traceparent`.

`RunningServer.Close` (and `mesh.Drain(ctx)`, or `mesh.Run` on SIGINT/SIGTERM)
drains first: in Router mode the instance leaves every pool, in direct mode the
listener closes, and it waits for the calls in flight to finish or
`WithDrainTimeout` to end. With a hand-built server, call `server.Drain(ctx)`
for a rolling restart and `server.Close()` afterwards.

## Documentation

- [MeshCall overview](../README.md)
- [Protocol specification](../docs/protocol.md)
- [Router, authentication and management](../docs/router.md)
- [Runtime architecture](../docs/architecture.md)

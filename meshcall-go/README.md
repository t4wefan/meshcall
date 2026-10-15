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

## A service

Request, response and item types are structs; their JSON Schema (the
contract other languages generate clients from) comes from the struct
fields: `json` tags name properties, a field is required unless it is a
pointer or `omitempty`, and `doc:"…"`, `enum:"a,b"`, `default:"<json>"` tags
add a description, an enumeration and a default.

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

server, err := meshcall.NewServer(meshcall.ServerOptions{Services: []*meshcall.Service{chat}})
// Through a Router (ws://, unix:///path, pipe://NAME; several for failover):
err = server.ConnectRouter(ctx, "ws://127.0.0.1:8765")
// …or directly: server.Listen("127.0.0.1:9000")
```

Also `HandleClientStream` (reads a `*meshcall.Input[Item]`),
`HandleSubscription` (a server stream clients resume from a cursor; see
[Subscriptions](../docs/protocol.md#subscriptions)) and `HandleDuplex` (the
shape of last resort; it needs a reason). Return a `*meshcall.Error` to fail
a call with a code (`&meshcall.Error{Code: "chat.v1.quota_exceeded", …}`);
any other error becomes a sanitised `internal`, and a panic is recovered and
logged. `ctx` is a `context.Context` that ends with the call's deadline or
cancellation, and carries metadata, the idempotency key, trailers
(`ctx.SetTrailer`) and a call-scoped `slog` logger. Contract warnings
(W001–W006) are logged at start and fail `NewServer` with `Strict: true` or
`MESHCALL_STRICT=1`. `meshcall.WriteContract(path, chat)` writes the
contract document for `meshcall generate`.

## A client

Generate a typed client from any service's contract — written in Python,
TypeScript or Go:

```bash
meshcall generate chat.meshcall.json --language go --package-name chatclient --output internal/chatclient
```

```go
rpc, err := meshcall.NewClient(meshcall.ClientOptions{}, "ws://127.0.0.1:8765")
chat := chatclient.NewChatClient(rpc)
reply, err := chat.Send(ctx, chatclient.ChatSendRequest{SessionID: "s1", Text: "hi"}, meshcall.WithTimeout(5*time.Second))
```

Without generation, the typed helpers are `meshcall.Unary`,
`ServerStream`, `ClientStream`, `Duplex` and `Subscribe`. Clients connect
lazily, reconnect with backoff, fail over between several endpoints, retry
unary calls that are idempotent (or carry `WithIdempotencyKey`) on
retryable errors, validate what they receive against their own types, and
take call options `WithTimeout`, `WithIdleTimeout`, `WithMetadata` and
`WithInstance` (required for targeted services). Streams are read with
`Recv()` (`io.EOF` at the end) or `for item, err := range stream.All()`.

# MeshCall best practice

This is a small, complete application with real process boundaries and three
languages:

- `src/best_practice/service.py` implements an in-memory, LLM-shaped Python
  service with `@service`, `@query`, `@mutates`, `@stream`, `@dependency` and `meta`;
- `src/best_practice/server.py` starts it through `Mesh(router).mount(LlmService()).run()`;
- `src/best_practice/demo.py` runs the whole example: it starts a `LocalRouter`
  and mounts the service on a `Mesh`, then runs the TypeScript CLI and the Go
  client against it;
- `ts-client/` is a generated TypeScript client package plus an interactive
  CLI;
- `go-client/` is a generated Go client plus a small program that exercises
  unary, client-streaming and server-streaming calls.

The service exposes `new_session`, `list_sessions`, `count_tokens`, a
client-streaming `assemble_prompt`, and the server-streaming `stream_chat`. The
response is deterministic fake data, so the application needs no model
credentials and keeps all state in the Python server process.

## What it demonstrates

**Explicit topology and peer runtime.** `server.py` connects to `Router()` when
`MESHCALL_ROUTER_URL` is set or starts a `LocalRouter(port=8765)` for local
development, mounts `LlmService()` on `Mesh(router)`, and runs `mesh.run()`,
draining in-flight calls on `SIGINT` / `SIGTERM`.

**Method decorators.** `@query` for pure reads (`list_sessions`,
`count_tokens`), `@mutates` for `new_session` and the client-streaming
`assemble_prompt`, and `@stream` for `stream_chat`. Generated clients take the
same business parameters, in the same order.

**A transactional dependency.** Every call gets a `SessionUnit` from the
`@dependency` provider `transaction()`. Handlers stage writes on it; the
provider commits them when the call succeeds and rolls them back when it
raises or is cancelled. A `stream_chat` the caller cancels halfway leaves no
half-written turn. Injected parameters never appear in the contract or in
generated clients.

**Metadata as an escape hatch.** `caller_id = meta("x-caller-id")` reads a
value that arrives as call metadata, not as a business field. The TypeScript
and Go clients set it with `withOptions({ metadata })` /
`WithOptions(CallOptions{Metadata})`.

**One `Mesh`, several clients.** The TypeScript CLI connects with
`await Mesh.connect(new Router())` and passes `mesh` to
`new LlmServiceClient(mesh)`; the Go client creates
`meshcall.NewMesh(meshcall.NewRouter())` and passes it to
`llmclient.NewLlmServiceClient(mesh)`. Both share one connection and call
`withOptions` / `WithOptions` for per-call settings.

## Run it

Everything at once (builds the clients, starts the service and an embedded
Router, runs the TypeScript CLI once and the Go client, then stops):

```bash
cd best-practice
uv run demo
```

## Run it by hand

In the first terminal:

```bash
cd best-practice
uv run server
```

The server registers with the Router from `MESHCALL_ROUTER_URL` (default
`ws://127.0.0.1:8765`, started locally when missing). Access logging can be
adjusted without editing the demo:

```bash
MESHCALL_LOG_LEVEL=DEBUG MESHCALL_LOG_COLOR=1 uv run server
MESHCALL_ACCESS_LOG=0 uv run server
```

In a second terminal, run the TypeScript CLI:

```bash
cd best-practice/ts-client
yarn install --frozen-lockfile
yarn build
yarn cli
```

The CLI creates one session and then waits at `you> `. Type a prompt and watch
the fake assistant response arrive chunk by chunk. The small command set is:

```text
/new [TITLE]       create and switch to a session
/sessions          list in-memory sessions
/tokens TEXT       count simple lexical tokens
/help              show commands
/quit              exit
```

For a non-interactive smoke test, pass a prompt directly:

```bash
node dist/cli.js "Hello MeshCall"
```

Both entry points read `MESHCALL_ROUTER_URL`; point them at a remote Router when
needed. The Go client does the same:

```bash
go -C go-client run . "Hello MeshCall"
```

## Regenerate the clients

The checked-in clients are generated from the Python service; do not edit
`ts-client/src/client.ts`, `ts-client/src/models.ts` or
`go-client/llmclient/client.go` by hand. `src/cli.ts`, `src/index.ts` and
`go-client/main.go` are the handwritten application layer.

```bash
cd best-practice
uv run --locked --reinstall-package meshcall meshcall generate \
  best_practice.service:LlmService \
  --language typescript \
  --runtime-version file:../../meshcall-ts \
  --package-name meshcall-best-practice-client \
  --output ts-client
uv run --locked --reinstall-package meshcall meshcall generate \
  best_practice.service:LlmService \
  --language go --package-name llmclient --output go-client/llmclient
```

`scripts/regenerate.sh best-practice` does both and is checked by CI.

# MeshCall best practice

This application currently uses Direct connections. Its service, streaming,
logging, and CLI structure remain useful references; its connection setup is
pending migration. Use [the Router demo](../demo/router/README.md) and
[Router deployment](../docs/router.md) for new applications.

This is a small, complete application with a real process boundary:

- `src/best_practice/service.py` implements an in-memory Python LLM-shaped
  service;
- `src/best_practice/server.py` is the Python RPC server entry point;
- `ts-client/` is a generated TypeScript client package plus an interactive
  CLI.

The service exposes `new_session`, `list_sessions`, `count_tokens`, a
client-streaming `assemble_prompt`, and the server-streaming `stream_chat`.
The response is deterministic fake data, so the application needs no model
credentials and keeps all state in the Python server process.

## Start the server

In the first terminal:

```bash
cd best-practice
uv run server
```

The server listens on `ws://127.0.0.1:8765` by default. Set
`MESHCALL_BEST_PRACTICE_PORT` to use another port.

The Python server writes one access log after every completed call. Each entry
contains the service method, status, elapsed milliseconds, and `call_id`. The
service methods also demonstrate an injected `logger: RpcLogger` parameter for
application logs. Startup logging can be adjusted without editing the demo:

```bash
MESHCALL_LOG_LEVEL=DEBUG MESHCALL_LOG_COLOR=1 uv run server
MESHCALL_ACCESS_LOG=0 uv run server
```

The library equivalent is `RpcServer(..., access_log=False)`; `log_level` and
`colorize` are also available as server options.

## Start the interactive CLI

In a second terminal:

```bash
cd best-practice/ts-client
yarn install --frozen-lockfile
yarn build
MESHCALL_SERVER_URL=ws://127.0.0.1:8765 yarn cli
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
MESHCALL_SERVER_URL=ws://127.0.0.1:8765 node dist/cli.js "Hello MeshCall"
```

The CLI calls the generated client. `assemble_prompt` exercises client-to-
server streaming, while `stream_chat` exercises server-to-client streaming.
The fake server waits briefly between output chunks with `asyncio.sleep`, so
the streaming behavior is visible without blocking the RPC loop.

## Regenerate the TypeScript package

The checked-in TypeScript client is generated from the Python service:

```bash
cd best-practice
uv run --locked --reinstall-package meshcall meshcall generate \
  best_practice.service:LlmService \
  --language typescript \
  --runtime-version file:../../meshcall-ts \
  --package-name meshcall-best-practice-client \
  --output ts-client
```

Do not edit `ts-client/src/client.ts` or `ts-client/src/models.ts` manually;
`ts-client/src/cli.ts` is the intentionally handwritten application layer.

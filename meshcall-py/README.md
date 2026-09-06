# meshcall-py

The Python runtime and code-generation package for the MeshCall monorepo.
Its import name is `meshcall`.

```bash
go -C ../meshcall-router build -o bin/meshcall-router ./cmd/meshcall-router
uv sync
uv run pytest -q
uv build
```

The monorepo-level runnable examples are in `../demo`. See the repository root
README for the complete Python/TypeScript workflow.

`WebSocketRouter` launches the standalone Go executable. It supports
`binary_path`, `auth_file`, readiness timeouts, `pid`, `is_running`,
`wait_closed()`, and async context management. A prebuilt binary can also be
selected with `MESHCALL_ROUTER_BINARY`; running it requires no Go compiler.

Pass `RouterCredentials(username=..., password=...)` or
`RouterCredentials(token=...)` as `auth` to a WebSocket client or Router server
driver. `RouterAuthClient` provides `whoami()`, `issue_token(scopes=..., ...)`,
and `revoke_token(...)`. Use `RouterScope(service=..., methods=[...])` to limit
a token, or `register_service=True` for temporary service instances.
See [Router authentication and management](../docs/router.md).

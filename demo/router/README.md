# MeshCall Router demo

This is a complete uv project. From this directory, run the short-lived
Go Router, Python service, and generated-client flow with:

```bash
go -C ../../meshcall-router build -o bin/meshcall-router ./cmd/meshcall-router
uv run demo
```

For the deployment-shaped deployment version, use three terminals:

```bash
uv run python -m router_demo.router
uv run python -m router_demo.server
uv run python -m router_demo.client
```

The Python Router entrypoint owns a Go child process. All routing runs in Go.
For accounts and scoped tokens, see [Router docs](../../docs/router.md).

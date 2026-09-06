# MeshCall Router demo

This is the recommended starting point for MeshCall's connection model and a
complete uv project. From this directory, run the short-lived
Go Router, Python service, and generated-client flow with:

```bash
go -C ../../meshcall-router build -o bin/meshcall-router ./cmd/meshcall-router
uv run demo
```

This local demo uses anonymous loopback connections. Real applications should
configure accounts and service scopes as shown in [Router docs](../../docs/router.md).
The current runtime does not require authentication when binding a network interface.

For the three-process local version, run one command per terminal:

```bash
uv run python -m router_demo.router
uv run python -m router_demo.server
uv run python -m router_demo.client
```

The Python Router entrypoint owns a Go child process. All routing runs in Go.
Deployments normally run that executable independently under Docker or another
process manager. See [SDK process ownership](../../docs/router-launcher.md).

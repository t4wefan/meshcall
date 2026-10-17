# MeshCall Router demo

This is a complete uv project. From this directory, run the short-lived
standalone-Router flow with:

```bash
uv run demo
```

It starts a `WebSocketRouter` process, registers the service with
`meshcall.serve(..., router=Router(url))`, connects a client with
`Router.connect`, prints the registry (`list_services`) and calls the service
through the generated client.

For the deployment-shaped version, use three terminals (Router on
`ws://127.0.0.1:8765`, the default every entry point resolves):

```bash
uv run python -m router_demo.router
uv run python -m router_demo.server
uv run python -m router_demo.client
```

The Python Router entrypoint owns a Go child process. All routing runs in Go.
For accounts and scoped tokens, see [Router docs](../../docs/router.md).

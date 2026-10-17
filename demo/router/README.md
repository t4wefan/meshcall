# MeshCall Router demo

This is a complete uv project. From this directory, run the short-lived
standalone-Router flow with:

```bash
uv run demo
```

It starts a `LocalRouter` process, registers the service with
`Mesh(local_router).mount(CounterService(), instance_id=...).serve()`, connects
a client with `Mesh(Router(local_router.url))`, prints the registry
(`mesh.registry.list()`) and calls the service through `CounterServiceClient(mesh)`.

For the deployment-shaped version, use three terminals (Router on
`ws://127.0.0.1:8765`, the default every entry point resolves):

```bash
uv run python -m router_demo.router
uv run python -m router_demo.server
uv run python -m router_demo.client
```

The Python Router entrypoint owns a Go child process. All routing runs in Go.
For accounts and scoped tokens, see [Router docs](../../docs/router.md).

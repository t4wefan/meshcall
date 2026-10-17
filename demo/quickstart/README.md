# MeshCall quickstart demo

This is a complete uv project. From this directory, run the whole example
with:

```bash
uv run demo
```

The command starts a `LocalRouter` on an ephemeral port, mounts `CounterService`
on a `Mesh` (`server_mesh.serve()`), calls it through `CounterServiceClient(mesh)`,
prints the typed stream, and shuts everything down. The two-process form is:

```bash
uv run python -m quickstart_demo.server   # Mesh(router).mount(CounterService()).run()
uv run python -m quickstart_demo.client   # CounterServiceClient(mesh)
```

`server.py` uses `Router()` when `MESHCALL_ROUTER_URL` is set and starts a
`LocalRouter(port=8765)` otherwise; `client.py` connects with `Mesh(Router())`
(defaulting to `ws://127.0.0.1:8765`).

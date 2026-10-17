# MeshCall quickstart demo

This is a complete uv project. From this directory, run the whole example
with:

```bash
uv run demo
```

The command starts the service with an embedded Router (`meshcall.serve`),
calls it through the generated client, prints the typed stream, and shuts
everything down. The two-process form is:

```bash
uv run python -m quickstart_demo.server   # meshcall.run(CounterService())
uv run python -m quickstart_demo.client   # CounterServiceClient.connect()
```

Both resolve `MESHCALL_ROUTER_URL` (default `ws://127.0.0.1:8765`); the first
one starts a bundled Router if the address is local and nothing listens there.

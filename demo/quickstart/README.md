# MeshCall quickstart demo

This is a complete uv project. From this directory, run the whole example
with:

```bash
uv run demo
```

The command starts a short-lived direct WebSocket server, calls it through the
generated client, prints the typed stream, and shuts everything down. To see
the two-process form instead:

```bash
uv run python -m quickstart_demo.server
uv run python -m quickstart_demo.client
```

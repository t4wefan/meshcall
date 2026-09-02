# MeshCall Router demo

This is a complete uv project. From this directory, run the short-lived
Router, service, and generated-client flow with:

```bash
uv run demo
```

For the deployment-shaped three-process version, use three terminals:

```bash
uv run python -m router_demo.router
uv run python -m router_demo.server
uv run python -m router_demo.client
```

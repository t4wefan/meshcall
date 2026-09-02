# MeshCall demos

Every demo is an importable Python package. A demo keeps its service,
client/server entry points, and generated artifacts together so it can be
copied or extended without relying on loose files in the parent directory.

## Packages

- `quickstart/` — the smallest direct WebSocket server and generated client.
- `showcase/` — the complete recommended API example. Run it with
  `python -m demo.showcase`.
- `router/` — a router process and a service process that connects to it.

The showcase deliberately does not demonstrate duplex RPC. Duplex is still an
experimental capability and is tested separately from the recommended demos.

## Run the demos

Run the complete showcase directly from the repository root:

```bash
uv run --project meshcall-py python -m demo.showcase
```

The quickstart uses two terminals:

```bash
uv run --project meshcall-py python -m demo.quickstart.server
uv run --project meshcall-py python -m demo.quickstart.client
```

The router demo uses three terminals:

```bash
uv run --project meshcall-py python -m demo.router.router
uv run --project meshcall-py python -m demo.router.server
uv run --project meshcall-py python -m demo.router.client
```

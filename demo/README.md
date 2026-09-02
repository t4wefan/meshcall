# MeshCall demos

Every demo is an importable Python package. A demo keeps its service,
client/server entry points, and generated artifacts together so it can be
copied or extended without relying on loose files in the parent directory.
Each demo directory is also a complete uv project with its own `pyproject.toml`
and `uv.lock`; from inside any demo directory, `uv run demo` is the one-command
entry point.

## Packages

- `quickstart/` — the smallest direct WebSocket server and generated client.
- `py2ts/` — a Python service and a generated TypeScript client package.
- `showcase/` — the complete recommended API example. Run it with
  `uv run demo`.
- `router/` — a router process and a service process that connects to it.
- `ts2py/` — a TypeScript service and a generated Python uv client package.

The showcase deliberately does not demonstrate duplex RPC. Duplex is still an
experimental capability and is tested separately from the recommended demos.

## Run the demos

Run a demo from its own project directory:

```bash
cd demo/showcase
uv run demo
```

The quickstart's complete short-lived flow is also one command:

```bash
cd demo/quickstart
uv run demo
```

The quickstart's deployment-shaped two-process form uses two terminals:

```bash
cd demo/quickstart
uv run python -m quickstart_demo.server
uv run python -m quickstart_demo.client
```

The router's complete short-lived flow is also one command:

```bash
cd demo/router
uv run demo
```

The router's deployment-shaped three-process form uses three terminals:

```bash
cd demo/router
uv run python -m router_demo.router
uv run python -m router_demo.server
uv run python -m router_demo.client
```

The cross-language demos each contain both a uv project and a Yarn project.
Run them from their own directories:

```bash
cd demo/py2ts
uv run demo
```

```bash
cd demo/ts2py
uv run demo
```

# MeshCall demos

Every demo is an importable Python package. A demo keeps its service,
client/server entry points, and generated artifacts together so it can be
copied or extended without relying on loose files in the parent directory.
Each demo directory is also a complete uv project with its own `pyproject.toml`
and `uv.lock`; from inside any demo directory, `uv run demo` is the one-command
entry point.

## Start with Router

`router/` is the recommended connection example. It starts a Go Router, registers
a Python service, calls it through a generated client, and shuts down the flow.
For separate processes, account setup and scoped tokens, use
[the best-practice application](../best-practice/README.md).
Prepare the executable once, then run the demo from the monorepo root:

```bash
go -C meshcall-router build -trimpath -o bin/meshcall-router ./cmd/meshcall-router
uv run --project demo/router demo
```

The demo uses anonymous loopback connections for experimentation. Applications
should configure accounts and scopes; see [Router authentication](../docs/router.md)
and [SDK startup](../docs/router-launcher.md).

For three local processes, open a terminal in `demo/router/` for each command:

```bash
uv run python -m router_demo.router
uv run python -m router_demo.server
uv run python -m router_demo.client
```

## Service and compatibility references

These runners currently use Direct. They remain useful for studying service
contracts and interoperability, but their connection setup is pending migration
to Router:

- `quickstart/` — a small Direct server and generated client.
- `showcase/` — service APIs, expanded parameters, unary and both streaming shapes.
- `py2ts/` — a Python service and generated TypeScript client package.
- `ts2py/` — a TypeScript service and generated Python uv client package.

Run `uv run demo` from the respective directory. Cross-language demos include
both a uv project and a Yarn project. Their READMEs document regeneration and
any separate process entry points.

Duplex remains experimental and is tested separately from these examples.

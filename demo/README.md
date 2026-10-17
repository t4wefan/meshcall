# MeshCall demos

Every demo is an importable Python package. A demo keeps its service,
client/server entry points, and generated artifacts together so it can be
copied or extended without relying on loose files in the parent directory.
Each demo directory is also a complete uv project with its own `pyproject.toml`
and `uv.lock`; from inside any demo directory, `uv run demo` is the one-command
entry point.

Every demo is Router-first: services mount onto a `Mesh` connected to a `Router`
(`MESHCALL_ROUTER_URL`, default `ws://127.0.0.1:8765`) or an explicit
`LocalRouter` subprocess, and clients connect through `Mesh(Router(...))`.

## Packages

- `quickstart/` — the smallest service, one stream and a generated client.
- `py2ts/` — a Python service and a generated TypeScript client package.
- `showcase/` — the recommended API: `@query`, `@mutates`, `@stream`, metadata
  injection, `with_options`, `mesh.registry` and a generated client.
- `router/` — a standalone Router process (`LocalRouter`), a service registered
  with it, and a client that inspects `mesh.registry` and calls the service.
- `ts2py/` — a TypeScript service and a generated Python uv client package.

Duplex is a restricted shape of last resort (see
[Duplex calls](../docs/protocol.md#duplex-calls)); it is covered by the
conformance suite rather than the introductory demos.

The complete application example — dependency injection with a transactional
session store, a TypeScript CLI and a Go client — lives in
[`best-practice/`](../best-practice/README.md).

## Run the demos

Run a demo from its own project directory:

```bash
cd demo/showcase
uv run demo
```

Every demo also has a deployment-shaped multi-process form: the quickstart and
router demos expose `python -m quickstart_demo.server` / `.client` and
`python -m router_demo.router` / `.server` / `.client`; the cross-language
demos start their second language from the same `uv run demo` command.

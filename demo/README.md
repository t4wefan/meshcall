# MeshCall demos

Every demo is an importable Python package. A demo keeps its service,
client/server entry points, and generated artifacts together so it can be
copied or extended without relying on loose files in the parent directory.
Each demo directory is also a complete uv project with its own `pyproject.toml`
and `uv.lock`; from inside any demo directory, `uv run demo` is the one-command
entry point.

Every demo is Router-first: services start with `meshcall.run` / `meshcall.serve`,
which connect to `MESHCALL_ROUTER_URL` (default `ws://127.0.0.1:8765`) and start
a bundled Router when the address is local and nothing listens there. Direct
mode still exists for tests, but no demo uses it.

## Packages

- `quickstart/` — the smallest service, one stream and a generated client.
- `py2ts/` — a Python service and a generated TypeScript client package.
- `showcase/` — the recommended API: `@query`, `@mutates`, `@stream`, metadata
  injection, `with_options`, the Router registry and a generated client.
- `router/` — a standalone Router process, a service registered with it, and a
  client that inspects the registry and calls the service.
- `ts2py/` — a TypeScript service and a generated Python uv client package.

Duplex is a restricted shape of last resort (see
[Duplex calls](../docs/protocol.md#duplex-calls)); it is tested by the
conformance suite rather than the recommended demos. Subscriptions and `Topic`
are experimental in v0.3.

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

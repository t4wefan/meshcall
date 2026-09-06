# MeshCall py2ts demo

This runner uses Direct as a cross-language contract reference. New applications
should use [Router connections](../../docs/router.md#connecting-services).

This directory is a complete uv project containing a Python service and a
generated TypeScript client package. From this directory, run the entire
cross-language round trip with:

```bash
uv run demo
```

The command builds the local TypeScript runtime and generated client package,
starts the Python service, invokes it from Node, and shuts the service down.
The Python subprocess orchestration uses asyncio subprocess APIs, so the demo
does not block its event loop while it waits for Yarn or Node.

The generated TypeScript package is in `ts-client/`. To regenerate it after
changing the Python service:

```bash
uv run meshcall generate \
  py2ts_demo.service:PythonGreetingService \
  --language typescript \
  --runtime-version file:../../../meshcall-ts \
  --package-name meshcall-demo-py2ts-client \
  --output ts-client
```

The `run.mjs` runner is kept in that package alongside the generated client so
the demo remains easy to inspect.

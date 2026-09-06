# MeshCall ts2py demo

This runner uses Direct as a cross-language contract reference. New applications
should use [Router connections](../../docs/router.md#connecting-services).

This directory is a complete uv project containing a TypeScript service and a
generated Python client package. From this directory, run the entire
cross-language round trip with:

```bash
uv run demo
```

The command builds the local TypeScript runtime and service, starts the
TypeScript server, calls it from Python with the generated Pydantic client,
and shuts the server down. It demonstrates unary `greet`, server-streaming
`count`, and client-streaming `sum`. The streaming calls exchange 40 items,
exceeding the initial credit window. The Python subprocess orchestration uses asyncio
subprocess APIs and does not block the event loop.

TypeScript method contracts validate payloads at runtime. The same service can
register with the shared Go Router by configuring
`router: { endpoint: "ws://127.0.0.1:8765", instanceId: "greeting-ts-1" }` on
`MeshCallServer`; see [Router architecture](../../docs/router.md).

The portable contract is in `ts-server/service.meshcall.json`. The generated
Python uv client package is in `py-client/`. To regenerate it after changing
the TypeScript service:

```bash
cd ts-server
yarn run export-contract
cd ..
uv run meshcall generate-contract \
  ts-server/service.meshcall.json \
  --language python \
  --package-name meshcall-demo-ts2py-client \
  --output py-client
```

For this repository-local copy, keep the following override in the generated
`py-client/pyproject.toml`, and format generated imports with the Python project's
Ruff configuration before committing:

```toml
[tool.uv.sources]
meshcall = { path = "../../../meshcall-py" }
```

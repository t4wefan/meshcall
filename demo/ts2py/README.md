# MeshCall ts2py demo

This directory is a complete uv project containing a TypeScript service and a
generated Python client package. From this directory, run the entire
cross-language round trip with:

```bash
uv run demo
```

The command builds the local TypeScript runtime and service, starts the
TypeScript server, calls it from Python with the generated Pydantic client,
and shuts the server down. The Python subprocess orchestration uses asyncio
subprocess APIs and does not block the event loop.

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

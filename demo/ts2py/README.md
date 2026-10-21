# MeshCall ts2py demo

This directory is a complete uv project containing a TypeScript service and a
generated Python client package. From this directory, run the entire
cross-language round trip with:

```bash
uv run demo
```

The command builds the local TypeScript runtime and service, starts a
`LocalRouter` and serves the TypeScript service with `new Mesh(router).mount(...).startServing()`,
calls it from Python through `Mesh(Router(url))` +
`TypeScriptGreetingServiceClient(mesh)`, and shuts the server down. It
demonstrates unary `greet`, server-streaming `count`, and client-streaming
`sum`. The streaming calls exchange 40 items, exceeding the initial credit
window.

The portable contract is in `ts-server/service.meshcall.json`. The generated
Python uv client package is in `py-client/`. To regenerate it after changing
the TypeScript service:

```bash
uv run meshcall export ts-server/src/service.ts --output ts-server/service.meshcall.json
uv run meshcall generate ts-server/src/service.ts --language python --package \
  --package-name meshcall-demo-ts2py-client --output py-client
```

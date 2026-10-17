# MeshCall py2ts demo

This directory is a complete uv project containing a Python service and a
generated TypeScript client package. From this directory, run the entire
cross-language round trip with:

```bash
uv run demo
```

The command builds the local TypeScript runtime and generated client package,
starts the Python service with `meshcall.serve` (an embedded Router), invokes it
from Node through `Router.connect` + `PythonGreetingServiceClient.connect`, and
shuts the service down.

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

# MeshCall ts2swift demo

The TypeScript service of [`../ts2py`](../ts2py/ts-server) called from its
generated Swift client. From this directory (macOS, Node and yarn installed):

```bash
MESHCALL_ROUTER_BINARY=$PWD/../../meshcall-router/bin/meshcall-router swift run
```

The runner builds the local TypeScript runtime and service, starts the service
(which starts its own `LocalRouter`), then calls it through
`TypeScriptGreetingServiceClient(Mesh(Router(url)))`: unary `greet`, a 40-item
server stream `count` and the client stream `sum`, and stops the service.

`Sources/TS2SwiftDemo/TypeScriptGreetingServiceClient.swift` is generated from
the service's portable contract; never edit it by hand. Regenerate it with:

```bash
scripts/regenerate.sh ts2swift      # from the repository root
# or: meshcall generate demo/ts2py/ts-server/service.meshcall.json --language swift \
#       --output demo/ts2swift/Sources/TS2SwiftDemo
```

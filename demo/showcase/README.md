# MeshCall showcase demo

This is a complete uv project demonstrating service APIs. Its current runner
uses Direct as a compatibility reference; start with [the Router demo](../router/README.md)
for the recommended connection model. Run this service API showcase with:

```bash
uv run demo
```

It covers default unary calls with expanded parameters, request-model calls,
server streaming, client streaming, and generated Pydantic models. Duplex is
experimental and is intentionally not part of this demo.

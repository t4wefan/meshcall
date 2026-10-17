# MeshCall showcase demo

This is a complete uv project. From this directory, run the recommended API
showcase with:

```bash
uv run demo
```

It covers `@query` with expanded parameters and with a request model,
`@mutates` with `meta("x-caller-id")` injection, `@stream` server streaming,
`@mutates` client streaming, per-call options through `with_options`, the
Router's built-in `mesh.registry.list()`, and generated Pydantic models. The
service is mounted on a `Mesh` and served (`server_mesh.serve()`) against a
`LocalRouter` on an ephemeral port.

Duplex is a restricted shape of last resort and is intentionally not part of
this demo.

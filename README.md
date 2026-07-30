# MeshCall

MeshCall is an asynchronous, typed RPC framework for Python services. Pydantic
models define payload contracts, generated clients provide a static API, and a
shared logical protocol supports direct WebSocket connections and routed service
instances.

The first implementation milestone covers:

- Python 3.11+ and `asyncio`;
- Pydantic v2 service contracts;
- static Python client generation;
- unary, server-streaming, client-streaming, and duplex calls;
- WebSocket Direct and WebSocket Router drivers;
- cancellation, half-close, per-direction flow control, and fair scheduling;
- round-robin and least-inflight balancing.

Typed notifications are planned for the next milestone.

## Development

```bash
uv sync
uv run pytest
```

The wire protocol is documented in [docs/protocol.md](docs/protocol.md).


# Implementation Architecture

Router is the recommended topology for new applications. Python and TypeScript
services register with the shared Go Router, and clients connect to its endpoint
with their own credentials. Direct remains available for compatibility and
transport tests. See [Router deployment](router.md) and
[SDK process ownership](router-launcher.md).

## Layers

```text
Python/Pydantic service ---- contract extraction ---+
                                                    |
TypeScript schema service ---- contract export -----+--> portable contract
                                                            |
                                                            +--> Python uv package
                                                            |
                                                            +--> TypeScript Yarn package

Python RPC event loop <---- thread-safe bridge ----> service worker event loop
        |
        v
Logical Frame connection and fair scheduler
        |
        +---- WebSocket Router ---- service instance registry (recommended)
        |
        +---- WebSocket Direct (compatibility)
```

The contract layer has no networking dependency. Unary is the default method
shape; explicit stream decorators select the recommended server or client
streaming shapes. Ordinary service parameters are wrapped into an internal
Pydantic request model for validation and transport. A single Pydantic request
parameter remains the object-style alternative. The Runtime operates on logical
frames and doesn't know whether the connection is direct or routed. WebSocket
drivers own connection setup, role handshakes, TCP/Unix endpoints, and server
registration. Duplex remains experimental protocol/runtime plumbing and is
intentionally omitted from the recommended usage surface.

The reserved `logger: RpcLogger` method parameter is dependency injection, not
an RPC field. It is removed while extracting the request model and supplied by
the server with service, method, and `call_id` context, so generated clients do
not expose it.

## RPC and service loop isolation

Every Python `RpcServer` owns a persistent service worker loop running in a
dedicated thread. WebSocket receive/send, call state, deadlines, cancellation,
flow-control credit, and terminal-frame arbitration remain on the RPC loop.
Pydantic request validation, the user handler, response validation, and stream
iteration run on the service worker loop.

Cross-loop operations use thread-safe future submission. A synchronously
blocking handler can delay other work assigned to the current service worker,
but it cannot prevent the RPC loop from enforcing a deadline or maintaining
other connections. The TypeScript client consumes unary, server-streaming, and
client-streaming calls; TypeScript service handlers are still not worker-thread
isolated and must avoid synchronously blocking Node's event loop.

Access logging is also owned by the RPC loop. It is emitted at the single
terminal-frame arbitration point, after a result or error status is known, so a
streaming call produces exactly one access entry even if cancellation races
with normal completion. Unknown methods and duplicate call IDs are logged at
the `call.open` rejection point because they never create a call object.

The TypeScript runtime separates service definitions, JSON Schema validators,
per-connection sessions, credit/queue primitives, frame scheduling, and endpoint
lifecycle. Direct and Router connections share the same session implementation.
Unary, server-streaming, and client-streaming handlers receive a call-bound
logger and abort signal. Declared schemas validate requests, responses, and
stream items; raw legacy handlers without schemas remain unchecked.

TypeScript supports TCP and Unix sockets for clients, Direct servers, and
Router service connections. Both languages use the same Go Router; its
process is launched by thin Python/TypeScript wrappers. Its registry, account
authentication, scoped token RPC and failure boundaries are described in
[Router architecture](router.md).

## Call ownership

The client creates a UUID `call_id`. A Direct server stores calls per connection.
The Router stores a global route with the client connection and one selected
service instance. The route increments that instance's inflight count and is
removed exactly once by a terminal result, terminal error, or disconnect.

The Router forwards stream data without decoding Pydantic payloads. Sticky
balancing is the only policy that reads a declared request field.

## Flow control

Client-to-server and server-to-client directions each maintain independent item
credit. The receiver grants an initial window of 16 items and replenishes one
credit when application code consumes an item. A producer waits for credit before
sending the next item; an input pump may hold one prefetched item to discover
iterator exhaustion without another window grant.

The connection sender prioritizes cancellation, window, and Ping/Pong frames.
Other frames are scheduled round-robin by `call_id`, preventing a large stream
from monopolizing a WebSocket connection.

## Lifecycle

`RpcServer.start()` freezes service registration and obtains a generation-tagged,
exclusive Driver binding. Any startup failure rolls the Server back to `IDLE` and
releases the binding. `stop()` closes the Driver before releasing it, so a stale
task can't operate through a newly acquired generation.

## Failure boundaries

- Service exceptions become sanitized `internal` errors and are logged locally.
- Pydantic validation failures become `invalid_argument`.
- Task cancellation is propagated as `call.cancel` and `cancelled`.
- Deadline expiry cancels the service task and returns `deadline_exceeded`.
- Instance disconnect terminates its routed calls with retryable `unavailable`.
- Connection loss terminates all local active calls; no recovery is attempted.

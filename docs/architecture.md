# Implementation Architecture

## Layers

```text
Pydantic service class
        |
        v
Contract extraction ----> static Python client generation
        |
        v
Client / server call state machines
        |
        v
Logical Frame connection and fair scheduler
        |
        +---- WebSocket Direct
        |
        +---- WebSocket Router ---- service instance registry
```

The contract layer has no networking dependency. The Runtime operates on logical
frames and doesn't know whether the connection is direct or routed. WebSocket
drivers own connection setup, role handshakes, TCP/Unix endpoints, and server
registration.

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
fetching or sending the next item.

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


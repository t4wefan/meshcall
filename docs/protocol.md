# MeshCall Logical Protocol v1

MeshCall transports JSON frames over an ordered, reliable connection. WebSocket
is the first transport, but the rules in this document are transport-independent.

## Connection handshake

The first frame sent by a connection is `hello`. Its role is either `client` or
`server`. A server connection to a Router follows a successful handshake with a
`server.register` frame containing its immutable service registration.

The protocol version is `meshcall/1`. A peer must reject an unsupported version
before accepting calls.

## Compatibility rule

Within `meshcall/1`, a decoder must **ignore unknown fields** on every frame and
must **reject unknown frame kinds**. New optional fields may be added to existing
frames in a minor revision without a version change; new frame kinds or changed
semantics of existing fields require a new protocol version. A Router forwards
call frames as received, so fields it does not understand reach the other peer
unchanged.

## Frames

Every frame is one JSON object with a string `kind`. `call_id` is a globally
unique string chosen by the client. All time values are integer milliseconds.

| Kind | Fields | Notes |
| --- | --- | --- |
| `hello` | `protocol`, `role`, `peer_id`, `instance_id?`, `capabilities?` | first frame from the connecting peer |
| `hello.ack` | `protocol`, `connection_id` | reply from the accepting peer |
| `server.register` | `instance_id`, `services[]` | Router mode only; one per connection |
| `server.register.ack` | `instance_id` | |
| `server.drain` | — | instance → Router: leave every pool, keep serving routed calls |
| `server.drain.ack` | `instance_id?` | Router → instance |
| `call.open` | `call_id`, `service`, `method`, `payload`, `timeout_ms?`, `idle_timeout_ms?`, `metadata?` | |
| `call.result` | `call_id`, `payload?`, `metadata?` | terminal |
| `call.error` | `call_id`, `error{code,message,retryable?,details?}`, `metadata?` | terminal |
| `call.cancel` | `call_id`, `reason?` | request to terminate; not terminal |
| `stream.item` | `call_id`, `direction`, `sequence`, `payload` | |
| `stream.end` | `call_id`, `direction` | half-close of one direction |
| `stream.window` | `call_id`, `direction`, `credit` | `credit > 0` |
| `ping` | `nonce` | |
| `pong` | `nonce` | echoes the ping nonce |

`services[]` entries are `{name, methods[{name, stream, balance{kind, key?}}]}`.
`stream` is one of `unary`, `server_stream`, `client_stream`, `duplex`;
`balance.kind` is one of `round_robin`, `least_inflight`, `random`, `sticky`,
`disabled`.

### Metadata

`metadata` is an optional object whose keys and values are strings. On
`call.open` it carries caller-supplied request metadata (trace context,
correlation ids, tenant identifiers). On `call.result` and `call.error` it
carries server-supplied trailers. Keys are case-sensitive and must not be
empty. Keys beginning with `meshcall-` are reserved for the runtime. Metadata is
opaque to the Router, which forwards it unchanged. Runtimes expose request
metadata to handlers and interceptors and expose trailers to callers.

## Service names

A service name is a sequence of dotted segments. Every segment except the
version starts with a letter and contains only letters, digits and
underscores. Exactly one segment group is the version: `v<major>` or
`v<major>.<minor>.<patch>[-<prerelease>]` (the prerelease suffix contains no
dots). A name written without a version is the same service as the one with
`.v0` appended, and runtimes normalise it before registering or calling:
`chat` ≡ `chat.v0`. Examples: `chat.v1`, `demo.v1.Greeting`, `chat.v1.2.0`,
`chat.v2.0.0-beta1`.

Routing matches the full normalised name exactly; `chat.v1` and `chat.v1.2.0`
are different services. `meshcall.router.` is reserved for the Router's own
services.

`capabilities` is reserved for negotiating optional features (binary
encodings, compression, byte credit) in later minor revisions; peers ignore
entries they do not know.

## Draining

A service instance that is about to stop sends `server.drain`. The Router
removes it from every pool at once, so no new `call.open` is routed to it,
replies `server.drain.ack`, and keeps forwarding frames for calls already
routed to it in both directions until they end. The instance then waits for
its in-flight calls, bounded by its own drain timeout, and closes the
connection. Its `instance_id` stays reserved until then. Sending
`server.drain` twice is harmless; a client sending it is a protocol violation.

A direct server drains by closing its listener while keeping existing
connections open, then waiting the same way.

## Calls

A client creates a globally unique `call_id` and sends `call.open`. In Router
mode, the Router selects one registered instance when it receives this frame.
The mapping is immutable for the lifetime of the call. Every stream, window,
result, error, and cancellation frame for that call follows the same mapping.

Each method has one of four shapes:

| Shape | Client items | Server items | Final result |
| --- | --- | --- | --- |
| unary | no | no | yes |
| server_stream | no | yes | no |
| client_stream | yes | no | yes |
| duplex (experimental) | yes | yes | optional |

## Timeouts

`timeout_ms` is the total budget for the call, measured by each peer from the
moment it processes `call.open` on its own monotonic clock. It is relative so
that peers never need synchronised wall clocks. Absent means unlimited.

`idle_timeout_ms` bounds the gap between consecutive `stream.item` frames in a
direction, measured by the consumer of that direction from the previous item or
from `call.open` for the first item. It is only meaningful for streaming shapes
and is ignored for unary calls. Absent means unlimited.

Three layers enforce the budget independently, each slightly later than the
layer before it so that in the normal case the most informative error wins:

| Layer | Fires at | Action |
| --- | --- | --- |
| server | `timeout_ms` | cancels the handler and sends `call.error deadline_exceeded` |
| Router | `timeout_ms + grace` (default 500 ms) | sends `call.error deadline_exceeded` with `details.source = "router"` to the client, `call.cancel` with reason `deadline_exceeded` to the instance, and frees the route |
| client | `timeout_ms + grace` (default `max(1 s, 10 %)`) | fails the call locally with `deadline_exceeded` and `details.source = "client"`, and sends `call.cancel` best-effort |

The server layer is authoritative. The Router layer protects clients from a
service instance that has stopped responding without closing its connection.
The client layer protects callers from a Router or direct server that has
stopped responding. The client budget covers connecting and the handshake as
well as the call itself.

`idle_timeout_ms` is enforced by the consuming peer only: the client for
`direction = "server"` items and the server for `direction = "client"` items.
On expiry the consumer fails the call with `deadline_exceeded` and
`details.reason = "idle"` and sends `call.cancel`. The Router does not enforce
idle timeouts.

A `deadline_exceeded` error is never `retryable`; whether a retry is safe
depends on the method.

## Stream directions

`client` means data produced by the client and consumed by the server. `server`
means data produced by the server and consumed by the client. Sequences are
independent, start at zero, and increase by one in each direction.

`stream.end` half-closes only the named data direction. It does not close the
opposite direction or finish the call.

## Flow control

Every streaming direction uses item credit. A `stream.window` frame grants the
producer permission to send `credit` additional items in the frame's named
direction. A producer must not send the next item without credit. A runtime may
hold one prefetched item while waiting for credit so it can discover iterator
exhaustion without another window grant.

Item credit bounds queue length. The WebSocket driver also applies a byte limit
to encoded frames and fair scheduling across active calls. A future protocol
revision may add byte-based stream credit without changing call semantics.

## Terminal frames

Exactly one `call.result` or `call.error` terminates a call. `call.cancel` is a
request to terminate; it is not itself a terminal acknowledgement. The first
terminal frame accepted by a runtime wins, and later frames are ignored.

A Router treats `call.cancel` from the client as the end of its interest in the
route: it forwards the cancel, frees the route, and discards any later frames for
that `call_id`. The instance still sends its own terminal frame, which the
Router drops. Because the client has already given up on the call, it is also
free to fail the call locally as `cancelled` as soon as it sends the cancel.

The duplex shape is retained as an experimental protocol capability. Its public
service and client API is not part of the current recommended usage surface.

A connection loss terminates every active call on that connection. Version 1
does not resume streams or migrate them between service instances.

## Liveness

Connections are kept alive with protocol-level `ping`/`pong` frames rather than
transport-level mechanisms, so that every transport and every platform
(including browsers) behaves the same way.

The **connecting peer** (a client, or a service instance connected to a Router)
sends `ping` every `ping_interval` (default 15 s) while the connection is idle
for that long, and closes the connection if no `pong` with the matching nonce
arrives within `ping_timeout` (default 10 s). A closed connection fails every
active call on it with retryable `unavailable`.

The **accepting peer** (a Router or a direct server) does not send pings. It
closes any connection on which no frame of any kind has arrived for
`idle_timeout` (default 45 s, which must exceed `ping_interval + ping_timeout`).

Every peer must answer `ping` with `pong` carrying the same nonce, with control
priority over data frames. Pings for which no matching ping was sent are
ignored.

## Errors

Protocol errors use stable machine-readable codes. Internal exception details
and tracebacks are never sent by default. Application errors may use namespaced
codes, but must still use the common error envelope.

| Code | Meaning | Typical origin |
| --- | --- | --- |
| `cancelled` | the caller or a peer cancelled the call | client, server |
| `deadline_exceeded` | `timeout_ms` or `idle_timeout_ms` elapsed | server, Router, client |
| `invalid_argument` | request payload or stream item failed validation | server |
| `method_not_found` | unknown service or method | server, Router |
| `permission_denied` | authenticated but not authorised | Router |
| `unauthenticated` | no or invalid credentials | Router |
| `resource_exhausted` | a capacity limit was reached; usually retryable | Router, transport |
| `unavailable` | no instance, or the connection was lost; usually retryable | Router, client |
| `protocol_error` | a frame violated this document | any |
| `internal` | an unexpected failure the server does not describe further | server |

`details` is an optional object with implementation-defined keys. The runtime
uses `source` (`"client"`, `"router"`, `"server"`) and `reason` (`"idle"`) as
described above.

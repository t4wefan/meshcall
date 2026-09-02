# MeshCall Logical Protocol v1

MeshCall transports JSON frames over an ordered, reliable connection. WebSocket
is the first transport, but the rules in this document are transport-independent.

## Connection handshake

The first frame sent by a connection is `hello`. Its role is either `client` or
`server`. A server connection to a Router follows a successful handshake with a
`server.register` frame containing its immutable service registration.

The protocol version is `meshcall/1`. A peer must reject an unsupported version
before accepting calls.

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

## Stream directions

`client` means data produced by the client and consumed by the server. `server`
means data produced by the server and consumed by the client. Sequences are
independent, start at zero, and increase by one in each direction.

`stream.end` half-closes only the named data direction. It does not close the
opposite direction or finish the call.

## Flow control

Every streaming direction uses item credit. A `stream.window` frame grants the
producer permission to send `credit` additional items in the frame's named
direction. A producer must not fetch or send the next item without credit.

Item credit bounds queue length. The WebSocket driver also applies a byte limit
to encoded frames and fair scheduling across active calls. A future protocol
revision may add byte-based stream credit without changing call semantics.

## Terminal frames

Exactly one `call.result` or `call.error` terminates a call. `call.cancel` is a
request to terminate; it is not itself a terminal acknowledgement. The first
terminal frame accepted by a runtime wins, and later frames are ignored.

The duplex shape is retained as an experimental protocol capability. Its public
service and client API is not part of the current recommended usage surface.

A connection loss terminates every active call on that connection. Version 1
does not resume streams or migrate them between service instances.

## Errors

Protocol errors use stable machine-readable codes. Internal exception details
and tracebacks are never sent by default. Application errors may use namespaced
codes, but must still use the common error envelope.

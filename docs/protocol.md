# MeshCall Logical Protocol v1

**English** | [简体中文](zh-CN/protocol.md)

MeshCall transports JSON frames over an ordered, reliable connection. WebSocket
is the first transport, but the rules in this document are transport-independent.

WebSocket runs over TCP, or over the platform's local transport: a Unix socket
on Linux, macOS and FreeBSD, a named pipe (`\\.\pipe\NAME`) on Windows.
Each platform has exactly one local transport; the other is refused with a
hint. The WebSocket upgrade, frames and HTTP calls are identical on all of
them. Tools write the endpoints as `ws://host:port`, `unix:///path` and
`pipe://NAME`.

## Connection handshake

The first frame sent by a connection is `hello`. Its role is either `client` or
`server`. A server connection to a Router follows a successful handshake with a
`server.register` frame containing its immutable service registration.

The protocol version is `meshcall/1`. A peer must reject an unsupported version
before accepting calls.

Instances of one service need not register identical method sets: a Router
routes each method over the instances that registered it, and rejects only a
registration that gives an already-registered method a different stream
shape or balance policy, or disagrees on `targeted`.

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
| `hello` | `protocol`, `role`, `peer_id`, `instance_id?`, `capabilities?`, `authorization?` | first frame from the connecting peer |
| `hello.ack` | `protocol`, `connection_id` | reply from the accepting peer |
| `server.register` | `instance_id`, `services[{name, methods[], targeted?, contract?}]`, `max_inflight?` | Router mode only; one per connection |
| `server.register.ack` | `instance_id` | |
| `server.drain` | — | instance → Router: leave every pool, keep serving routed calls |
| `server.drain.ack` | `instance_id?` | Router → instance |
| `call.open` | `call_id`, `service`, `method`, `payload`, `timeout_ms?`, `idle_timeout_ms?`, `metadata?`, `idempotency_key?`, `instance_id?` | |
| `call.result` | `call_id`, `payload?`, `metadata?` | terminal |
| `call.error` | `call_id`, `error{code,message,retryable?,details?}`, `metadata?` | terminal |
| `call.cancel` | `call_id`, `reason?` | request to terminate; not terminal |
| `stream.item` | `call_id`, `direction`, `sequence`, `payload` | |
| `stream.end` | `call_id`, `direction` | half-close of one direction |
| `stream.window` | `call_id`, `direction`, `credit` | `credit > 0` |
| `ping` | `nonce` | |
| `pong` | `nonce` | echoes the ping nonce |

`services[]` entries are `{name, methods[{name, stream, balance{kind, key?}}], contract?}`.
`contract` is the service's portable contract entry (the same object a
`*.meshcall.json` document holds: version, description, methods with
schemas, parameters, annotations and, for duplex methods, `duplex_reason`). It is opaque to routing; the Router
keeps the first one registered per service and serves it through the
`meshcall.router.v1.Registry` service (`list`, `describe {service}`), which
is readable without an account in development mode and needs a call
permission like any other service otherwise.
`stream` is one of `unary`, `server_stream`, `client_stream`, `duplex`;
`balance.kind` is one of `round_robin`, `least_inflight`, `random`, `sticky`,
`disabled`.

### Frame size

A frame's encoded size is limited to 64 MiB by default, which is also the
largest limit the Router accepts (`--max-frame-size`; `max_frame_size` /
`maxFrameSize` / `MaxFrameSize` in the runtimes). A payload, including one
stream item, must fit in one frame; larger data is sent as a stream.

A sender checks every frame's encoded size before sending it and never
sends one over its limit. Instead only that call fails, with non-retryable
`resource_exhausted` (`reason: "frame_too_large"`), and the connection
stays open:

- a client fails the call locally (`source: "client"`); if the frame was an
  input stream item the call is already open, so it also sends
  `call.cancel`;
- a server whose response, output stream item or error is too large sends
  that error as the call's `call.error` instead (`source: "server"`),
  without trailers, and stops the handler.

A peer that receives a frame over its own limit closes the connection
(WebSocket close 1009), so peers that talk to each other, the Router
included, should use the same limit.
The Router queues frames for each peer up to `--outbox-size` bytes (default
256 MiB, at least the maximum frame size); a peer that stops reading
while more than that is owed to it is disconnected.

### Metadata

`metadata` is an optional object whose keys and values are strings. On
`call.open` it carries caller-supplied request metadata (trace context,
correlation ids, tenant identifiers). On `call.result` and `call.error` it
carries server-supplied trailers. Keys are case-sensitive and must not be
empty. Keys beginning with `meshcall-`, in any letter case, are reserved for
the runtime and the Router, which set them in lower case: a client refuses
such a key in caller metadata with non-retryable `invalid_argument`
(`source: "client"`, `reason: "reserved_metadata"`; an empty key is
`reason: "invalid_metadata"`), and a server refuses it as a trailer. Lookups
that ignore letter case (`meta()`) match a reserved key exactly. Metadata is
opaque to the Router, which forwards it unchanged, with one exception: keys
beginning with `meshcall-tunnel-`, in any letter case, are set only by the
Router. It removes them from every `call.open` a client sends and, on a
tunnel connection, sets `meshcall-tunnel-id` and `meshcall-tunnel-principal`
(see [Tunnels](#tunnels)). Runtimes expose request metadata to handlers and
interceptors and expose trailers to callers.

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

`capabilities` negotiates optional features; peers ignore entries they do
not know. A service instance that answers tunnel offers lists `tunnels`
(see [Tunnels](#tunnels)). Further entries (binary encodings, compression,
byte credit) are reserved for later minor revisions.

`authorization` carries credentials, in the format of the HTTP
`Authorization` header (`Basic …`, `Bearer mk_…`, `Bearer mt_…`,
`Bearer mtun_…`), for a peer that cannot set that header on the WebSocket
upgrade: a browser. Credentials never go in the URL. A connection that sent
an `Authorization` header and also `authorization` in `hello` is closed with
1008. Credentials in `hello` are refused the way the same credentials in the
header would be, with close status 4000 plus that HTTP status, so a browser
can tell a temporary refusal from a permanent one:

| Header path | `hello` path | Client error | Example |
| --- | --- | --- | --- |
| HTTP 401 | close 4401 | `unauthenticated` (not retryable) | unknown, expired or revoked credential |
| HTTP 403 | close 4403 | `permission_denied` (not retryable) | reserved for refusals of a valid identity |
| HTTP 429 | close 4429 | `resource_exhausted` (retryable) | a tunnel at its `max_connections` |
| HTTP 503 | close 4503 | `unavailable` (retryable) | the Router is busy verifying passwords |

The close reason is the Router's message (for example `tunnel connection
limit reached`).

## Capacity and health

`max_inflight` on `server.register` is the number of calls the instance
accepts concurrently across all its services. The Router never selects an
instance at its cap and answers retryable `resource_exhausted`
(`reason: "overloaded"`) when every instance is full; an instance also
enforces the cap itself, so direct-mode servers behave the same.

The Router also bounds the calls one connection may have in flight
(`--max-calls-per-connection`, default 1000, `0` for no limit), counting
calls that joined an in-flight idempotent call. A `call.open` beyond it
fails with retryable `resource_exhausted` (`reason: "overloaded"`) and the
connection stays open.

The Router keeps a per-instance circuit breaker: a call that hits the
Router's deadline, or that the instance answers with `internal` or
`unavailable`, counts as a failure; application errors do not. After a
configurable number of consecutive failures (default 5) the instance is
excluded for a cooldown (default 10 s), then exactly one probe call is
admitted; its success closes the circuit, its failure reopens it. When every
instance is excluded the Router answers retryable `unavailable`
(`reason: "circuit_open"`). Breaker state is in-memory and resets when the
instance reconnects.

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

### Router shutdown

A Router that is asked to stop (SIGTERM or SIGINT; a second signal skips the
wait) drains first:

1. `/readyz` answers 503. New connections are refused with HTTP 503 and
   `Retry-After`, and every new `call.open` on an existing connection fails
   with retryable `unavailable` (`reason: "router_draining"`). Such a call
   reached no instance, so retrying it elsewhere is always safe.
2. Each connection with nothing in flight (no call as a client, no routed
   call as an instance) is closed with WebSocket status 1001 (going away), at
   once or as soon as its last call ends. Clients reconnect, moving to
   another Router when they have several; service instances reconnect the
   same way and register wherever a Router accepts them.
3. After `--shutdown-timeout` (default 10 s) the calls still in flight end:
   the client receives retryable `unavailable` (`reason: "router_shutdown"`),
   the instance receives `call.cancel` with reason `router_shutdown`, and
   the remaining connections close with 1001.

A Router that stops without draining (its owner closed its stdin, or the
embedding program called `Close`) performs step 3 at once.

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
| duplex | yes | yes | no |

Duplex is a restricted shape; see [Duplex calls](#duplex-calls).

## Targeted calls

Every service instance has an **instance id**: chosen by the operator
(`instance_id=` / `instanceId`) or generated by the SDK at start-up, and
visible to the service itself (`ctx.instance_id`, `server.instance_id`), to
the Router's Registry and to `meshcall router inspect`. One instance uses the
same id towards every Router it registers with. Ids are stable across the
instance's own reconnects and must be unique per Router; a registration with
an id already in use is rejected.

`call.open` may carry `instance_id` to address one instance directly. The
Router then skips the balance policy and routes to exactly that instance:

- not registered for `service` → `unavailable`, `details.reason =
  "instance_unknown"` (not retryable);
- draining → `unavailable`, `reason: "instance_draining"` (retryable);
- at its `max_inflight` → `resource_exhausted`, `reason: "overloaded"`;
- otherwise the call is routed even if the instance's circuit breaker is
  open: the caller asked for this instance by name.

A service registered with `targeted: true` declares that every call to it
must name an instance (the instances hold distinct state: a session, a loaded
model, an actor). A `call.open` without `instance_id` for such a service is
answered `invalid_argument`, `reason: "instance_required"`, without being
routed. All instances of one service must agree on `targeted`; a conflicting
registration is incompatible. `sticky` balancing is meaningless for a
targeted service and ignored.

A direct server (no Router) applies the same rules against its own id: a
foreign `instance_id` is `instance_unknown`, a missing one on a targeted
service is `instance_required`.

Idempotency deduplication includes the target: the same key sent to two
instances is two calls. After `instance_lost` a targeted call has no other
instance to prefer.

## Tunnels

A **tunnel** lets a trusted issuer hand a client a credential for **one
service instance**. On a connection made with it, the Router behaves like
that instance's own Router: only that instance's services within the
tunnel's scopes exist, no `instance_id` needs naming, and nothing else is
reachable. The Router relays and enforces the scope; the instance learns who
is calling from the Router, not from caller-supplied metadata.

A tunnel is a record in one Router's memory: `tunnel_id`, a digest of its
credential, the target `instance_id`, its `scopes` (services of the instance
and the methods allowed on each, `"*"` for all), an opaque `principal`
chosen by the issuer (for example `user:alice/device:phone`, at most 512
bytes), `expires_at` and an optional `max_connections`. A Router restart
loses every tunnel; clients then ask the issuer again.

### Opening, revoking, listing

The Router's built-in `meshcall.router.v1.TunnelService` (unary methods):

| Method | Request | Result |
| --- | --- | --- |
| `open` | `instance_id`, `scopes[{service, methods[]}]`, `principal`, `ttl_seconds` (1–86400), `max_connections?`, `e2e_key?` | `credential` (`mtun_…`, returned once), `tunnel_id`, `instance_id`, `principal`, `scopes`, `expires_at_unix_ms`, `max_connections?`, `e2e_key?`, `connections`, `online` |
| `revoke` | `tunnel_id` | `revoked` |
| `list` | `{}` | `tunnels[]`: the same fields as `open` without `credential` |

The caller needs the method in its call permissions and must be an account
or API key (development mode: anyone); temporary tokens, the anonymous user
and tunnel connections cannot issue tunnels. Every scope must name a service
the instance registered and methods it serves, within the issuer's own call
permissions. `revoke` and `list` see only the caller's own tunnels.

`open` fails with `unavailable` / `instance_unknown` when the instance is
not registered, `unavailable` / `instance_draining` while it drains,
`invalid_argument` for scopes the instance does not serve, and
`permission_denied` for scopes beyond the issuer or, with
`reason: "tunnel_rejected"`, when the instance does not accept the tunnel.

### Instance consent

Before `open` returns, the Router offers the tunnel to the instance, and only
an instance that listed `tunnels` in its `hello` capabilities receives
offers (any other refuses at once, `tunnel_rejected`). The offer is an
ordinary unary call the Router opens on the instance's connection to the
reserved service `meshcall.router.v1.TunnelHost`, which runtimes answer
themselves:

- `offer` — `{tunnel_id, instance_id, principal, scopes, expires_at_unix_ms,
  resumed, e2e_key?, credential_digest?}` (the last two for an end-to-end
  tunnel); the instance answers `{"accept": true}` to accept and anything
  else (`false`, an error, no answer within 10 s) to refuse, optionally with
  a `reason` (see [End-to-end encryption](#end-to-end-encryption)). Only after
  acceptance does the tunnel become usable and `open` return.
- `closed` — `{tunnel_id, reason}`: the tunnel ended (`revoked`, `expired`,
  `rejected`, `key_changed`, `offer_timeout`, `instance_changed`, `abandoned` when the
  issuer gave up before the instance answered). The Router does not wait for
  the answer.

These calls carry `timeout_ms` and are not counted against `max_inflight`.
A runtime keeps the tunnels it accepted (id → principal, scopes) and drops
one on `closed`.

### Tunnel connections

A client connects with the credential as a Bearer token: the `Authorization`
header of the WebSocket upgrade or HTTP request, or `authorization` in
`hello` for browsers. A credential past `expires_at` is refused (HTTP 401, or
4401 after `hello`), and one at its `max_connections` with HTTP 429; HTTP
requests in progress count as connections. On a tunnel connection:

- a `call.open` resolves only to the target instance and only for methods in
  the tunnel's scopes. `instance_id` is implied: omitted, or equal to the
  target. Anything else — another instance, another service, a method
  outside the scopes, the Router's management services — is
  `permission_denied` with `reason: "tunnel_scope"` and the tunnel's own
  `instance_id`, identical whether or not it exists;
- `Registry/list` and `describe` (always allowed) and `/v1/openapi.json`
  show only that view: the scoped methods the target serves, with the target
  as the only instance. `describe` of a service outside the view is
  `method_not_found`, the same answer as for a service that does not exist;
- the connection is a `client`; it cannot register services or issue tokens
  or tunnels. `AuthService/whoami` returns `tunnel` with the record above;
- the Router sets `meshcall-tunnel-id` and `meshcall-tunnel-principal` on
  every `call.open` it routes and `instance_id` to the target, replacing
  whatever the client sent. Runtimes expose them as `ctx.tunnel`;
- idempotency deduplication is per tunnel: a tunnel's call never joins
  another principal's.

**Lifetime.** `expires_at` bounds *new* connections only: an established
connection lives until the tunnel is revoked or closed, and an expired
tunnel closes when its last connection ends. Revoking closes every
connection at once with WebSocket status 4401 and the close reason
`tunnel_revoked`; a tunnel that closes for another reason while it has
connections (the instance changed or refused a re-offer) closes them with
4401 `tunnel_closed`. Calls in flight then fail with non-retryable
`unauthenticated`, `source: "router"`, and that close reason as
`details.reason`, so a client tells revocation apart from a network loss.
A client keeps that answer: later calls on the same client fail at once with
the same error, reason included, without reconnecting (a new connection with
the credential would be refused with HTTP 401 anyway). While the instance is disconnected, calls fail with
retryable `unavailable` / `instance_offline`. When an instance registers
under the target id again, with the same identity and the `tunnels`
capability, the Router offers it each of its tunnels again with
`resumed: true`; accepting resumes the tunnel, refusing or not answering
closes it, and a registration under another identity or without the
capability closes it (`instance_changed`).

### End-to-end encryption

A tunnel opened with `e2e_key` carries its calls' contents end to end
between the client and the instance; the Router still routes, enforces the
scopes, deadlines and flow control, and sees service, method, timing, stream
structure, sizes and error codes, but not payloads, metadata or error
messages. It is opt-in per tunnel; an instance may require it (below).

**Keys and pinning.** The instance has a static X25519 key pair, configured
like its instance id (it is stable configuration, not per process). The
issuer knows the instance's public key out of band — it provisioned the
instance — and passes it to `TunnelService/open` as `e2e_key` (32 bytes,
base64url without padding; anything else is `invalid_argument` from the
Router). The Router puts it in the offer; an instance refuses an offer whose
`e2e_key` is not its own public key. The issuer gives the client the same key
with the credential. The client **never takes the key from the Router or the
instance**: it starts the handshake with the pinned key, so only the holder of
the matching private key can complete it, and a handshake that fails is a
non-retryable `unauthenticated` with `reason: "e2e_handshake_failed"` and
never falls back to plaintext. This is what defeats a man in the middle, the
Router included. The Router still decides who may connect: it sees the
credential, and the client has no static key (Noise `NK`).

**Credentials say what they are.** The credential of an end-to-end tunnel
starts with `mtun_e2e.` (a plain one never contains `.`). A runtime given an
end-to-end credential without `e2e_key`, or `e2e_key` with a plain
credential, refuses when the client is configured, before it connects, so no
payload is ever sent towards the Router unsealed by mistake.

**Requiring it.** An instance may require end-to-end tunnels for the services
of a mount (`require_e2e`); it then refuses every offer scoping one of them
that has no `e2e_key`, so a key-holding instance never serves those services
in clear through a tunnel. Requiring it without a private key is a
configuration error.

**Offer answers.** An instance that refuses an offer for an end-to-end reason
says so: `{"accept": false, "reason": "e2e_key_mismatch"}` (the pinned key is
not its own) or `"e2e_required"` (a plain offer for services that require
it). The Router names the reason in the issuer's `tunnel_rejected` message,
and closes a tunnel whose re-offer is refused with `e2e_key_mismatch` with the
reason `key_changed` (its connections close as `tunnel_closed`).

**Handshake.** Once per client connection, before its first sealed call, the
client calls `meshcall.router.v1.TunnelHost/handshake` (unary)
`{message}` → `{message}`: the two messages of
`Noise_NK_25519_AESGCM_SHA256` with empty payloads, base64url without
padding. The prologue is `meshcall-e2e/1|<credential_digest>`, where
`credential_digest` is the lowercase hex SHA-256 of the tunnel credential
(its UTF-8 bytes). The client hashes its own credential; the Router puts the
digest (never the credential) in the offer of an end-to-end tunnel as
`credential_digest`, and the instance uses the digest of the tunnel the
handshake came through. A handshake made for one tunnel therefore cannot
complete through another (`e2e_handshake_failed`): a relay holding a second
credential cannot run one client's sealed calls under another tunnel's
principal, and since a tunnel has one target, the session is bound to that
instance too. The Router routes this method only from a connection of an E2E
tunnel, to its target, with the tunnel metadata set; to any other connection
it is refused like any reserved method it may not call (`permission_denied`
with `reason: "tunnel_scope"` on a plain tunnel, `method_not_found`
elsewhere). The
session id is the first 16 bytes of the handshake hash, hex; the instance
keeps the session under that tunnel. The two keys of the Noise `Split()` seal
client → instance and instance → client. The instance keeps at most 16
sessions per tunnel (the oldest is dropped) and drops them when the tunnel
closes.

**Unknown sessions and rejected frames.** A call naming a session the
instance does not know is refused before any handler runs with
`unauthenticated`, `reason: "e2e_session_unknown"` (an instance that
reconnected has lost its sessions). On that answer, for any call shape, the
client drops the session and its connection and handshakes again before its
next call; it retries a unary call once, and a streaming call reports the
error. A frame of a known session that does not open — altered, replayed,
too old, moved to another call or method — is refused (`call.open`) or ends
its call (a later frame) with `unauthenticated`,
`reason: "e2e_frame_rejected"`; the session stays, since only the Router or
someone on the path can have changed the frame. A client receiving a frame
that does not open ends that call the same way (`source: "client"`).

**Sealed frames.** The sealed parts of a frame travel in an optional
`sealed` field: base64url without padding of an 8-byte big-endian counter
followed by the AES-256-GCM ciphertext (nonce: four zero bytes and the
counter) of a JSON object. Each direction of a session counts from 0; a
receiver accepts each counter once, within a window of the latest 1024, so
frames the Router drops or reorders never desynchronise the keys and a
replayed frame is refused.

| Frame | Left in clear | Sealed object | Clear field replaced |
| --- | --- | --- | --- |
| `call.open` | every field but `payload` and the client's `metadata`, plus `e2e_session` | `{payload, metadata}` | `payload: null` |
| `stream.item` | `kind`, `call_id`, `direction`, `sequence` | `{payload}` | `payload: null` |
| `call.result` | `kind`, `call_id` | `{payload, metadata}` | `payload: null` |
| `call.error` | `kind`, `call_id`, `error.code`, `error.retryable` | `{message, details, metadata}` | `error.message: "sealed"` |

The associated data is the UTF-8 of
`meshcall-e2e/1|<kind>|<call_id>|<service>|<method>|<direction>|<sequence>|<e2e_session>`
(empty for a field the frame lacks; `call_id` frames after `call.open` use
the call's service, method and session), so a sealed part cannot be moved to
another call, item or session. Windows, ends, cancels and keepalives carry no
content and stay clear. Router-synthesised errors are clear. The Router keeps
the metadata keys it sets (`meshcall-tunnel-*`) in clear `metadata`; a
runtime ignores `meshcall-` keys inside a sealed object.

Calls to the Router's own services (`meshcall.router.*`: `AuthService`,
`Registry`, `TunnelService`) are answered by the Router and are never sealed,
on an end-to-end connection too. On an E2E tunnel the Router refuses, with
`permission_denied` and `reason: "tunnel_e2e_required"`, every other
`call.open` that lacks `e2e_session` and `sealed`, and every HTTP `/v1/call`
(HTTP cannot carry a session). It does not deduplicate sealed calls by
idempotency key: a replayed terminal frame could not be opened by another
session. `TunnelService/list` and `AuthService/whoami` show the tunnel's
`e2e_key`.

**Rotation.** An instance configured with a new key refuses the re-offers of
tunnels that pinned its old one (`e2e_key_mismatch`), which closes them with
the reason `key_changed`; their clients' calls fail with `unauthenticated` /
`tunnel_closed` and they ask the issuer again. The first version has no
overlap window.

### Instance-bound registration

A registration credential may name the instance ids it registers under
(`instance_ids` on accounts, API keys and token scopes, see
[the Router](router.md#accounts-and-permissions)); the Router closes a
registration under any other id with 1008 and a close reason saying so. Without it, a credential allowed
to register a service could register as a tunnel's target while that
instance is offline.

## Temporary tokens

`meshcall.router.v1.AuthService` issues temporary tokens (`mt_…`): a Router
account delegates part of its permissions, optionally bound to instance ids,
to a secret it hands to a caller or a service instance. The Router keeps only
the token's SHA-256 digest, in memory; tokens are per Router process and are
invalid after a restart (an issuer hands out a new one when a holder's
reconnect is refused).

| Method | Request | Result |
| --- | --- | --- |
| `issue_token` | `ttl_seconds`, `scopes[{service, methods?, register?, instance_ids?}]` | `token` (returned once), `token_id`, `expires_at_unix_ms`, `scopes` |
| `renew_token` | `token_id`, `ttl_seconds` | `token_id`, `expires_at_unix_ms` |
| `revoke_token` | `token_id` | `revoked` |
| `list_tokens` | `{}` | `tokens[{token_id, issuer, instance_ids?, expires_at_unix_ms, expired, connections}]` |

Only an account or API key may call these (with the method in its `call`
ACL), and `renew_token`, `revoke_token` and `list_tokens` see only the
caller's own tokens. `ttl_seconds` is 1 up to the Router's maximum
(`--token-max-ttl`, 30 days by default); above it the answer is
`invalid_argument` with `details.reason: "ttl_too_long"` and
`details.max_ttl_seconds`; below 1 it is `invalid_argument` with
`details.reason: "ttl_invalid"`. Runtimes default to 900 s. The Router holds at most
`--max-tokens` outstanding tokens (4096 by default; `resource_exhausted`
beyond).

**Lifetime.** `expires_at` bounds *new* connections and HTTP requests only,
as for tunnel credentials: an expired token is refused at connect (HTTP 401,
or 4401 when sent in `hello`), but a connection authenticated before expiry
stays up — its calls and its instance's registration keep working — until it
closes or the token is revoked. `revoke_token` closes every connection on the
token at once (calls fail as for any lost connection), past expiry too, and is
the only way to cut a live one. An expired token that still holds connections
stays listed (`expired: true`) so its issuer can revoke it; one without
connections is forgotten.

**Renewal.** `renew_token` sets `expires_at` to now plus `ttl_seconds`
(bounded by the same maximum). The secret, scopes and `instance_ids` do not
change, so nothing is redelivered and nothing reconnects. A token that cannot
be renewed — expired, revoked, unknown or another issuer's: the answer is the
same, so an issuer learns nothing about other issuers' tokens — gets
`invalid_argument` with `details.reason: "token_expired"`; the issuer issues a
new token instead.

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

## Idempotency keys

`idempotency_key` (1–128 bytes, chosen by the caller) names the *intent* of a
call so that retries are safe. The server exposes it to the handler. For
**unary** calls a Router deduplicates on `(service, method, idempotency_key,
instance_id)` (the last being empty when the call is not targeted):
a second `call.open` with the same key while the first is in flight joins it
and receives the same terminal frame; after completion the terminal frame is
replayed for a short window (60 s by default). If the serving instance is
lost before answering, every joined caller receives the same retryable
`unavailable` and the next call with that key prefers a different instance.
Server and client streams carry the key to the handler but are never
deduplicated. A duplex call cannot be replayed safely at all, so a duplex
`call.open` with `idempotency_key` is refused with `invalid_argument`,
`details.reason = "idempotency_key_on_duplex"`, by the Router before routing
and by a direct server. The
deduplication table is a cache: a Router restart loses it, and the worst
case is one extra execution against a handler that honours the key.

Client runtimes ship a retry policy that re-sends a failed unary call only
when the error is `retryable`, the code is in its retry set, and either the
method is declared idempotent in the contract or the call carries an
`idempotency_key`; attempts share the call's `timeout_ms`.

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

Credit is per call and per direction; nothing limits the total a consumer
grants across the calls on one connection. A Router enforces it: it counts the
credit in every `stream.window` it forwards and the items it forwards against
it, and an item beyond that credit is the producer's violation. The Router
does not forward it; it fails the call with `protocol_error`
(`details.source = "router"`, `details.reason = "flow_control_violation"`) to
the client and sends `call.cancel` with reason `flow_control_violation` to the
instance. No connection is closed for it. A consumer that receives an item
beyond its credit fails the call with `protocol_error` likewise.

Everything a Router buffers for a peer is therefore something that peer asked
for: items within the credit it granted, and a bounded number of control and
terminal frames per call it opened or accepted (`call.open`, `stream.end`,
`call.result` or `call.error`, `call.cancel`, windows answering the items it
sent, `pong` answering its `ping`). The Router bounds each peer's buffer by
bytes, not by frame count (see [the Router](router.md)); a peer that stops
reading while what it asked for exceeds that budget is disconnected as a slow
peer. A consumer that may stall should keep its outstanding credit across all
calls on a connection, times the size of an item, within that budget. Item
credit does not bound bytes; a future protocol revision may add byte-based
stream credit without changing call semantics.

## Terminal frames

Exactly one `call.result` or `call.error` terminates a call. `call.cancel` is a
request to terminate; it is not itself a terminal acknowledgement. The first
terminal frame accepted by a runtime wins, and later frames are ignored.

A Router treats `call.cancel` from the client as the end of its interest in the
route: it forwards the cancel, frees the route, and discards any later frames for
that `call_id`. The instance still sends its own terminal frame, which the
Router drops. Because the client has already given up on the call, it is also
free to fail the call locally as `cancelled` as soon as it sends the cancel.

A connection loss terminates every active call on that connection. The protocol
does not resume streams or migrate them between service instances.

## Duplex calls

A duplex call streams in both directions at once: the server may send output
items while the client is still sending input items. It uses no frame of its
own. Each direction has its own sequence, credit and half-close exactly as
above, and the client's `idle_timeout_ms` applies to each direction by its
consumer. The call ends with `call.result` **without a payload**, or with
`call.error`; there is no final result value. Trailers ride on the terminal
frame as for every shape.

Duplex is the shape of last resort: it switches off guarantees the other
shapes keep, and a method opts into it explicitly, giving the reason in its
contract (`duplex_reason`, required). A duplex method:

- is never retried and never deduplicated, takes no `idempotency_key` (see
  [Idempotency keys](#idempotency-keys)) and cannot be annotated
  `idempotent` or `read_only`;
- is not reachable over HTTP (`405`, `details.reason = "duplex_over_http"`);
- pins one instance for its whole life: balancing happens once at
  `call.open`, and draining waits for it, bounded by the drain timeout;
- is seen by deadlines, circuit breakers and interceptors as one call: they
  observe the open and the end, not individual items;
- can deadlock itself: if the client sends without reading output, the
  server's output credit runs out, it stops reading input, and the client's
  input credit runs out too. Read output concurrently with sending (or
  alternate one item each way). The runtime does not detect this.

Prefer a server stream plus unary calls, or a client stream, whenever the
two directions do not truly interleave.

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
| `invalid_argument` | request payload or input stream item failed validation, or the call cannot be routed as sent | server, Router |
| `method_not_found` | unknown service or method | server, Router |
| `permission_denied` | the identity (including the anonymous user) may not make this call; identical whether or not the target exists | Router |
| `unauthenticated` | credentials were presented but are invalid; refused at connect | Router |
| `resource_exhausted` | a capacity limit was reached; usually retryable | Router, server, transport |
| `unavailable` | no instance, or the connection was lost; usually retryable | Router, client |
| `protocol_error` | a frame violated this document | any |
| `internal` | an unexpected failure the server does not describe further, including a response or output item that failed the service's own validation | server |

`details` is an optional object with implementation-defined keys. The runtime
uses `source` (`"client"`, `"router"`, `"server"`), `reason` and
`instance_id`. Router-synthesised errors carry `source: "router"` and one of
these reasons: `no_instances` (nothing registered for the service),
`instance_lost` (the serving instance disconnected; `instance_id` names it),
`overloaded` (a capacity limit: the Router's own, or every instance at its
`max_inflight`), `circuit_open` (every instance is excluded by the Router's
circuit breaker), `balance_disabled` (a method whose balancing is `disabled` has several
instances), `cancelled` (a joined idempotent call
was cancelled by its first caller), `instance_unknown` / `instance_draining`
(a targeted call named an instance that is not registered or is leaving) and
`instance_required` (an `invalid_argument` for a targeted service called
without `instance_id`), `instance_offline` (a retryable `unavailable`: a
tunnel's target is not connected), `tunnel_rejected` (a `permission_denied`
from `TunnelService/open`: the instance refused or does not accept tunnels),
`tunnel_scope` (a `permission_denied` on a tunnel connection: the call is
outside the tunnel's view), `tunnel_e2e_required` (a `permission_denied`: a
plain call on an end-to-end tunnel), `e2e_handshake_failed` /
`e2e_session_unknown` / `e2e_frame_rejected` (an `unauthenticated` from the client or the instance,
see [End-to-end encryption](#end-to-end-encryption)), `tunnel_revoked` / `tunnel_closed` (an
`unauthenticated` for calls on a tunnel connection the Router closed with
4401, see [Tunnel connections](#tunnel-connections)),
`idempotency_key_on_duplex` (an
`invalid_argument` for a duplex call carrying an idempotency key) and
`flow_control_violation` (a `protocol_error`: a stream item beyond the
consumer's credit, see [Flow control](#flow-control)), `router_draining` and
`router_shutdown` (an `unavailable` from a Router that is stopping, see
[Router shutdown](#router-shutdown)). A direct server uses the same reasons with
`source: "server"`. A server enforcing its own `max_inflight`
answers `resource_exhausted` with `source: "server"`, `reason: "overloaded"`.
Clients and servers report a frame they
did not send because it was over the size limit as non-retryable
`resource_exhausted`, `reason: "frame_too_large"`, with their own `source`
(see [Frame size](#frame-size)). Client retry policies add
`attempts` to the final error when more than one attempt was made.

Clients report how the Router ended a connection as an error, the same in
every runtime: an upgrade refused with HTTP 401, 403, 429 or 503, or a
`hello` refused with close 4401, 4403, 4429 or 4503 (see [Connection
handshake](#connection-handshake)), is `unauthenticated`,
`permission_denied`, retryable `resource_exhausted` or retryable
`unavailable` (`source: "router"`); a close with 4401 is non-retryable
`unauthenticated` (with `details.reason` when the close reason is one of the
reasons above); a close with 1008 is `permission_denied` with
`reason: "policy_violation"` and the Router's close reason in the message
(for a refused registration, why: an unauthorised service, an `instance_id`
outside `instance_ids`, a conflicting method). Any other loss is retryable
`unavailable`. A client does not reconnect after `unauthenticated` or
`permission_denied`: the same credentials fail the same way again.

### Validation failures

A server validates the request, every input stream item, the response and
every output stream item against the contract. The error it sends depends on
who is at fault:

- Request or input item invalid → `invalid_argument`, `details.source =
  "server"`, `details.reason = "request"` or `"input_item"`, and
  `details.violations`: a list of `{path, code, message}` where `path` is the
  dotted field path (`"profile.tags.0"`, `""` for the root), `code` a short
  validator keyword (`type`, `missing`, `int_parsing`, …) and `message` a
  human-readable explanation. The `message` field of the envelope is the fixed
  text `Invalid request` / `Invalid input stream item`.
- Response or output item invalid → `internal`, `details.source = "server"`,
  `details.reason = "invalid_response"` / `"invalid_output_item"`. This is a
  service bug; the violations are logged by the service, not sent.

- A client that holds the contract validates what it receives too. A
  response or output item that fails the *client's* schema is reported
  locally as `protocol_error`, `details.source = "client"`, `details.reason
  = "invalid_response"` / `"invalid_output_item"` with `violations`: the
  server and the client disagree on the contract (version skew), which no
  retry fixes. Generated clients always carry their response and item
  schemas; Python clients validate through their Pydantic models.

No field of a validation error may contain the rejected input value. Clients
and tooling rely on `violations` to point at the offending parameter; the
runtime exposes a typed subclass per code (`InvalidArgumentError`,
`DeadlineExceededError`, …) so callers never compare code strings.

## HTTP calls

A Router also accepts calls as plain HTTP requests on the same listener, so
any HTTP client can call the mesh without a MeshCall runtime. Each request
is one call; the Router translates it into the frames above, so permission
checks, balancing, deadlines, breakers, deduplication and targeting behave
exactly as for a WebSocket caller. HTTP carries **unary calls and server
streams fully, client streams with the limits below, and no duplex calls**.
Requests whose path does not start with `/v1/` are WebSocket upgrades,
except `/healthz` and `/readyz`.

This mapping is stable under `/v1`; breaking changes need a new prefix.

### Requests

`POST /v1/call/{service}/{method}`. Headers:

| Header | Meaning |
| --- | --- |
| `Authorization` | `Basic …` (account), `Bearer mk_…` (API key), `Bearer mt_…` (token) or `Bearer mtun_…` (tunnel), as for WebSocket connections; none means the `anonymous` user |
| `MeshCall-Timeout-Ms` | `timeout_ms`; at most the Router's `--http-max-timeout` |
| `MeshCall-Idle-Timeout-Ms` | `idle_timeout_ms` |
| `Idempotency-Key` | `idempotency_key` |
| `MeshCall-Instance` | `instance_id` (targeted call) |
| `MeshCall-Meta-<key>` | one metadata entry; `<key>` is lower-cased |

Without `MeshCall-Timeout-Ms`, unary and client-stream calls get the
Router's `--http-default-timeout` (30 s by default); server streams get no
default, since a stream of updates may run indefinitely.

The body depends on the method's shape:

- **unary, server stream**: the request payload as JSON; an empty body is
  `{}`. The content type is not checked (`curl -d` labels JSON as a form).
- **client stream**: `Content-Type: application/x-ndjson`; the first line is
  the request payload, every further non-empty line one input item. The
  Router reads a line only when the instance has granted credit for it, so
  a slow instance slows the sender through TCP backpressure rather than
  buffering in the Router; a client that cannot stream a request body
  simply sends it whole. The input stream ends with the body. A body that
  stops arriving for the Router's idle timeout while it is reading is cut
  off.
- **duplex**: refused with `405` and `method_not_found`,
  `details.reason = "duplex_over_http"`.

Closing the request (the HTTP client disconnects) sends `call.cancel`.

### Responses

- **unary**: `200`, `Content-Type: application/json`, the result payload.
  Each trailer is a `MeshCall-Trailer-<key>` response header.
- **server stream**: `200`, `Content-Type: application/x-ndjson`, one JSON
  envelope per line:

  ```text
  {"item": {...}}
  {"item": {...}}
  {"end": {"trailers": {"key": "value"}}}
  ```

  An error after the `200` replaces the end line with
  `{"error": {...}, "trailers": {...}}`. Exactly one `end` or `error` line
  closes every complete stream; a body that ends without one was cut off.
  Typed lines leave no ambiguity between an item and the end of the stream
  and need no HTTP trailers, so browser `fetch` can read every part.
- **client stream**: as unary, once the instance answers.

Errors before any item use the error envelope as the body,
`{"error": {"code", "message", "retryable", "details"}}`, with this status:

| Code | Status |
| --- | --- |
| `invalid_argument` | 400 |
| `unauthenticated` | 401 |
| `permission_denied` | 403 |
| `method_not_found` | 404 (405 for `duplex_over_http`) |
| `resource_exhausted` | 429 |
| `cancelled` | 499 |
| `internal`, `protocol_error`, application codes | 500 |
| `unavailable` | 503 |
| `deadline_exceeded` | 504 |

A body that is not valid JSON / NDJSON, or a timeout above the maximum, is
`400`, and a client stream sent without `Content-Type: application/x-ndjson`
is `415`, all with `invalid_argument` and `details.source = "router"`. More
than `--http-max-inflight` concurrent HTTP calls are refused with `429`,
`resource_exhausted`, `reason: "overloaded"` and `Retry-After`; WebSocket
connections are counted separately.

### Discovery and health

`GET /v1/services` is `meshcall.router.v1.Registry/list` and
`GET /v1/services/{service}` is `describe`. `GET /v1/openapi.json` is an
OpenAPI 3.1 document generated from the registered contracts. All three need
the Registry permission and show only the services and methods the caller
may call (on a tunnel: the tunnel's view). `GET /healthz` (the process is up) and `GET /readyz` (it accepts
calls; `503` while stopping) need no credentials and reveal no services.

### Browsers

Cross-origin calls are allowed only for origins listed with
`--http-cors-origin` (exact origins, or `*`); preflight requests from other
origins are refused, and so is a WebSocket upgrade whose `Origin` is not
listed (a same-origin page, or a client that sends no `Origin`, is always
accepted). Cookies are never used, so credentials are always explicit: the
`Authorization` header, or — on a WebSocket, where browsers cannot set it —
`authorization` in `hello`. The TypeScript package's `meshcall/web` entry
has no Node dependency and picks the transport from the Router URL:
`new Mesh(new Router("wss://…"), { tunnel })` speaks these frames over the
browser's WebSocket (every call shape, duplex included), and
`new Mesh(new Router("https://…"), { token })` implements this HTTP mapping
over `fetch`. Generated TypeScript clients accept either.

### Deployment

A reverse proxy in front of the Router must not buffer request bodies for
client streams to stream (nginx: `proxy_request_buffering off`), and should
not buffer responses for server streams to arrive item by item (nginx:
`proxy_buffering off`, or the `X-Accel-Buffering: no` header the Router
sends on streaming responses).

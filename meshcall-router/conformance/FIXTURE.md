# Conformance fixture contract

A runtime is tested by running a *fixture program* that registers the service
`conformance.v1.Fixture` with the Router whose URL is in `MESHCALL_ROUTER_URL`,
using the instance id in `MESHCALL_CONFORMANCE_INSTANCE` and a keepalive ping
interval of `MESHCALL_PING_INTERVAL_MS` milliseconds, and, when
`MESHCALL_MAX_FRAME_SIZE` is set, that maximum frame size in bytes. The program keeps running
until it is drained (see `drain_self`) or killed. It must start under
`MESHCALL_STRICT=1`, so every method needs a description and mutating ones an
annotation; duplex methods need a reason.

| Method | Shape | Request | Response | Behaviour |
| --- | --- | --- | --- | --- |
| `echo` | unary | `value: int`, `text: str` | `{value, text}` | returns its input |
| `bad_response` | unary | — | `{ok}` | returns `{ok: "not-a-bool"}`, which fails its own response contract |
| `fail` | unary | `code: str`, `message: str`, `retryable: bool = false` | — | raises an application error with that code/message/retryable and `details = {"from": "fixture"}` |
| `slow` | unary | `ms: int`, `tag: str = ""` | `{tag}` | records `tag` as invoked, sleeps `ms`; if cancelled, also records `tag` as cancelled |
| `invoked_tags` | unary | — | `{tags: [str]}` | tags of every `slow` call that started, in order |
| `cancelled_tags` | unary | — | `{tags: [str]}` | tags recorded by cancelled `slow` calls |
| `count` | server stream | `n: int`, `gap_ms: int = 0` | items `{index}` | yields `0..n-1`, sleeping `gap_ms` between items |
| `sum` | client stream | `offset: int = 0`, items `{value}` | `{total}` | `offset + Σ value` |
| `metadata_echo` | unary | — | `{metadata: {str: str}}` | returns the call metadata; sets trailers `echoed="yes"` and `count=<number of metadata keys>` |
| `drain_self` | unary | `delay_ms: int = 0` | `{ok: true}` | schedules `stop(drain=True)` after `delay_ms`, then exits the process |
| `chat` | duplex | `prefix: str = ""`, items `{text}` | items `{text}` | sends `{text: prefix + text}` for each input as it arrives; when the input ends, sends `{text: "done:<inputs>"}` and returns |
| `announce` | duplex | — , items `{text}` | items `{text}` | sends `{text: "hello"}`, closes its output, then reads the input to its end and returns |
| `big` | unary | `bytes: int` | `{text}` | returns a text of `bytes` characters |
| `big_items` | server stream | `bytes: int` | items `{text}` | yields `{text: "small"}`, then a text of `bytes` characters |
| `tunnel_info` | unary | — | `{tunnel_id: str \| null, principal: str \| null, sealed: bool}` | the call's `ctx.tunnel`, or nulls and `false` |
| `closed_tunnels` | unary | — | `{closed: [{tunnel_id, reason}]}` | every tunnel close notice received, in order |

The program also registers `conformance.v1.Targeted` with `targeted: true`
and one method, `whoami` (unary, read-only, no parameters), returning
`{instance: <this instance's id>}`.

Both services accept tunnels (`accept_tunnels` / `acceptTunnels` /
`TunnelPolicies` at mount): every offer is accepted unless its principal is
`refuse:me`, and each close notice is recorded for `closed_tunnels`. The
program serves with the end-to-end private key in `MESHCALL_E2E_PRIVATE_KEY`
(`e2e_private_key` / `e2ePrivateKey` / `E2EPrivateKey`), so it can accept
end-to-end tunnels pinned to the matching public key.

It also registers `conformance.v1.Vault`, accepting tunnels like the others
but only end-to-end ones (`require_e2e` / `requireE2E` / `RequireE2E`), with
one method, `peek` (unary, read-only, no parameters), returning
`{sealed: <ctx.tunnel is sealed>}`.

The suite expects the Python-style snake_case method names above in the
registration (`echo`, `cancelled_tags`, `metadata_echo`, `drain_self`).
Reference fixtures: `meshcall-py/conformance/fixture.py`,
`meshcall-ts/conformance/fixture.ts`, `meshcall-go/conformance/fixture`.

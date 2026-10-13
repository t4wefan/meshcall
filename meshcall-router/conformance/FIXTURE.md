# Conformance fixture contract

A runtime is tested by running a *fixture program* that registers the service
`conformance.v1.Fixture` with the Router whose URL is in `MESHCALL_ROUTER_URL`,
using the instance id in `MESHCALL_CONFORMANCE_INSTANCE` and a keepalive ping
interval of `MESHCALL_PING_INTERVAL_MS` milliseconds. The program keeps running
until it is drained (see `drain_self`) or killed. It must start under
`MESHCALL_STRICT=1`, so every method needs a description and mutating ones an
annotation.

| Method | Shape | Request | Response | Behaviour |
| --- | --- | --- | --- | --- |
| `echo` | unary | `value: int`, `text: str` | `{value, text}` | returns its input |
| `fail` | unary | `code: str`, `message: str`, `retryable: bool = false` | — | raises an application error with that code/message/retryable and `details = {"from": "fixture"}` |
| `slow` | unary | `ms: int`, `tag: str = ""` | `{tag}` | sleeps `ms`; if cancelled, records `tag` |
| `cancelled_tags` | unary | — | `{tags: [str]}` | tags recorded by cancelled `slow` calls |
| `count` | server stream | `n: int`, `gap_ms: int = 0` | items `{index}` | yields `0..n-1`, sleeping `gap_ms` between items |
| `sum` | client stream | `offset: int = 0`, items `{value}` | `{total}` | `offset + Σ value` |
| `metadata_echo` | unary | — | `{metadata: {str: str}}` | returns the call metadata; sets trailers `echoed="yes"` and `count=<number of metadata keys>` |
| `drain_self` | unary | `delay_ms: int = 0` | `{ok: true}` | schedules `stop(drain=True)` after `delay_ms`, then exits the process |

The suite expects the Python-style snake_case method names above in the
registration (`echo`, `cancelled_tags`, `metadata_echo`, `drain_self`).
Reference fixtures: `meshcall-py/conformance/fixture.py`,
`meshcall-ts/conformance/fixture.ts`.

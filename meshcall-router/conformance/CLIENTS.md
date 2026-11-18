# Client conformance

`FIXTURE.md` tests a runtime's **server** side: the suite plays scripted frames
against a fixture service. This file tests a runtime's **client** side: the
runtime's own tests make the calls, against a shared environment that
provides the mesh and the disruptions a client must survive. Every client
runtime plugs into it the same way.

## The environment

```bash
meshcall-conformance env --fixture "<command that starts a FIXTURE.md program>"
```

It starts a Router, a TCP proxy in front of it, and the fixture (any
runtime's: `meshcall-go/bin/conformance-fixture`,
`node meshcall-ts/dist/conformance/fixture.js`,
`uv run --project meshcall-py --locked python meshcall-py/conformance/fixture.py`),
waits until the fixture has registered `conformance.v1.Fixture`, and prints one
line:

```json
{"kind":"env.ready","router_url":"ws://127.0.0.1:PORT","instance_id":"fixture-1","e2e_key":"<base64url>"}
```

Clients connect to `router_url` (the proxy). The Router runs in development
mode, so anyone may open tunnels (`TunnelService/open`) for `instance_id`;
`e2e_key` is the fixture's end-to-end public key, to pin in an end-to-end
tunnel. The fixture serves `conformance.v1.Fixture`, `conformance.v1.Targeted`
and `conformance.v1.Vault` as FIXTURE.md describes.

### Commands

Then each stdin line is a command object, answered by one stdout line,
`{"ok":true}` or `{"ok":false,"error":"…"}`, once the command is done. The
environment stops, fixture included (its whole process group), when stdin
ends or on an interrupt.

| Command | Effect |
| --- | --- |
| `restart_router` | the Router crashes and a new one starts on the same port: client connections are cut without a call error or close frame (the client must notice the loss itself), connections are refused until the new Router listens, tunnels and tokens are lost; answered once the fixture has registered again |
| `pause_router` | every client connection through the proxy stalls: open, but no byte moves either way (what an app finds after the system suspended it) |
| `resume_router` | bytes move again |
| `restart_fixture` | the fixture is stopped and started again: it re-registers and has lost its end-to-end sessions; answered once it has registered |
| `stop` | stop now |

## The scenarios

`ClientScenarios` in [`clients.go`](clients.go) lists what every client
runtime must show, each with an id (`unary.retry`, `e2e.session_lost`, …) and
the expected outcome. A runtime implements them in its own test framework
(XCTest, JUnit, …) against the environment and marks each test with a comment:

```text
// conformance-scenario: e2e.session_lost
```

`TestClientRuntimesCoverEveryScenario` (`go test ./conformance`, so every CI
run) checks that each registered runtime marks every scenario or skips it
with a reason, and that no marker names an unknown scenario. A new scenario
therefore fails every runtime until each covers or skips it.

## Adding a client runtime

1. Register it in its own file, `clients_<runtime>.go`:

   ```go
   func init() {
       ClientRuntimes = append(ClientRuntimes, ClientRuntime{
           Name: "jvm", Tests: "meshcall-jvm/src/test",
           Skipped: map[string]string{"…": "why it cannot apply"},
       })
   }
   ```

2. In its tests, start `meshcall-conformance env` once per test class or
   suite (build it with `go build -o bin/meshcall-conformance
   ./cmd/meshcall-conformance` when `MESHCALL_CONFORMANCE_BINARY` is unset),
   read the ready line, and send commands through stdin. Honour
   `MESHCALL_FIXTURE` so the same tests can run against another runtime's
   fixture. `meshcall-swift/Tests/MeshCallTests/LiveMesh.swift` is the
   reference harness (about 100 lines).

3. Implement and mark every scenario, then add the run to `scripts/check.sh`
   and CI.

Tests sharing one environment share the Router's circuit breaker
(`internal/router/breaker.go`): five consecutive failures from the fixture
instance, that is `internal` or `unavailable` answers or the Router's own
deadline firing, exclude it from routing for 10 seconds, so every test after
them fails with it. End a test that fails on purpose (the attempts of
`unary.retry`, `deadline.server`) with a successful call, which resets the
count, or keep its failures under five; `restart_fixture` and
`restart_router` reset it too.

# SDK Router startup review

Router is the recommended connection model. Direct remains a compatibility
transport. This review describes the launchers at runtime commit `787f6c7` and
proposes follow-up work; the proposed APIs and safeguards are not implemented.

## Process ownership

The normal deployment has one independently managed Go Router endpoint.
Services register with it and clients connect with their own credentials.
Docker or another process manager owns its lifecycle. Applications must not
start a separate Router for each client or service: each Router has independent
registrations, tokens, and active calls.

SDK process ownership is useful for development, tests, desktop applications,
and other self-contained deployments. One application explicitly creates
`WebSocketRouter`, starts it, shares its endpoint, and closes service/client
connections before stopping the child. Both languages launch the same Go
program. Ordinary client construction only connects to an endpoint.

| Concern | Current behavior | Consequence |
| --- | --- | --- |
| Executable setup | SDK packages contain no Go binary; launchers search explicit configuration, package/source locations, then `PATH` | Installing the SDK alone is insufficient to start Router |
| Binary discovery | Source discovery also scans the caller's working directory and ancestors; a `meshcall-router/go.mod` marker enables a candidate | Working directory can select an unexpected executable before `PATH` |
| Compatibility | Readiness checks `router.ready`, `meshcall/1`, child PID, and port/socket | Wire protocol compatibility does not establish launcher API version, authentication capabilities, or binary authenticity |
| Authentication | An omitted auth file means anonymous access, including when bound to `0.0.0.0` or `::` | A listener configuration change can expose callable services and service registration without credentials |
| Initialization | Developers manually hash passwords, write an account file, and distribute credentials | There is no authenticated one-call local setup or private bootstrap channel |
| Diagnostics | SDKs consume stderr into a bounded private tail, surfaced on startup failure | Runtime Router warnings and unexpected exit details are difficult to observe from the application |
| Isolation | Child processes inherit the application environment and user privileges | Process separation is not a sandbox; unrelated environment secrets are also visible to the child |

Sources: [Python launcher](../meshcall-py/src/meshcall/router.py),
[TypeScript launcher](../meshcall-ts/src/router.ts),
[Go startup](../meshcall-router/cmd/meshcall-router/main.go),
[listener](../meshcall-router/internal/router/router.go), and
[authentication](../meshcall-router/internal/router/auth.go).

## What the current implementation already handles

- Both SDKs use argument arrays and spawn without a shell. Passwords and tokens
  are not Router command-line arguments.
- Listener defaults are loopback and an OS-assigned port. The child reports the
  bound endpoint after listening, avoiding a separate free-port selection race.
- Startup and shutdown have timeouts and cleanup. Stdin closure lets the Go
  child exit when the owning process disappears. Python also offers an async
  context manager; TypeScript supports `stop()` and `close()`.
- Invalid or empty supplied auth files fail startup. Configured accounts,
  registration ACLs, service/method scopes, and token expiry/revocation are
  enforced inside Go. Temporary tokens cannot delegate beyond their issuer or
  issue further tokens.
- Go retains the WebSocket library's default Origin checks, bounds frame and
  queue sizes, and limits concurrent expensive password checks.

These provide a usable process foundation. Readiness is a compatibility check
after executing the binary; it cannot make an untrusted executable safe.

## Prioritized follow-up

### 1. Enforce authentication at the Go listener boundary

Require authenticated configuration for normal startup. Keep anonymous startup
behind an explicit development option, restricted to literal loopback addresses
or a protected local socket. Reject unauthenticated wildcard/network binds in
Go so that the CLI and both SDKs share the same policy.

Today, `LoadAuth("")` returns no authentication provider and a nil provider
accepts the connection. Nil identities can call and register services. This is
why omitting `auth_file` while setting `host="0.0.0.0"` has a concrete impact;
the current loopback default itself does not expose the listener to the LAN.
Go's [Listen documentation](https://pkg.go.dev/net#Listen) confirms the wildcard
binding semantics.

Loopback restricts network reachability; it does not identify the calling local
application. Local managed Routers should also use credentials. For Unix
sockets, establish private directory/socket permissions or platform ACLs; the
current listener creates parent directories with mode `0750` and leaves socket
permissions to the OS and inherited umask.

Keep account/token authorization in Go. Network credentials require TLS at the
documented reverse proxy boundary; merely enabling an auth file does not encrypt
plain `ws://` traffic. Deployment configuration must also protect the backend
hop when it crosses a network.

### 2. Make executable selection deterministic

Remove implicit working-directory/ancestor discovery from normal installed SDK
usage. Keep source-checkout discovery as an explicit developer choice. Resolve
an explicitly selected binary or a versioned installed artifact and show its
path/version in diagnostics. Treat custom executables and the user's `PATH` as
trusted configuration, not as verified distribution sources.

The current risk is conditional: when no higher-priority binary is available,
a writable or untrusted working tree can provide the marker and executable
chosen by a trusted caller. The launcher executes it with the caller's
privileges before validating readiness. No shell injection is necessary.

Published artifacts should be pinned to a release and platform, with integrity
checked against trusted release metadata before execution. Keep downloads and
builds in an explicit installation step, support offline/custom binaries, and
avoid network access during imports or `start()`. A useful distribution shape
is a platform package for npm and platform wheels or a separate installer for
Python; choosing the packaging mechanism is follow-up implementation work.

### 3. Provide authenticated local initialization

A managed startup helper should initialize the Go child through an inherited
private pipe or equivalent private control channel, establish fresh random
credentials, and return connection information to its owning application.
Do not pass bootstrap secrets through argv, general logs, a public management
RPC, or a publicly reachable unauthenticated bootstrap window.

The owner may have an explicitly configured issuer account. Each service gets
registration permission only for its services; clients receive allowed call
scopes. Use the existing AuthService for temporary tokens after authenticated
initialization. The parent must declare allowed services/methods; the helper
must not infer a wildcard grant from convenience. Omit token-issuance permission
from ordinary worker/client credentials. Temporary credentials must be renewed
deliberately within the current maximum lifetime, and restart requires a fresh
bootstrap because tokens are process-local.

For a long-lived deployed Router, keep account configuration in a protected
file/secret mount and supervise the Go process independently. A future explicit
initialization command can generate that configuration. Starting a client must
never initialize or take ownership of a shared Router.

### 4. Version the launcher contract and expose lifecycle events

Extend readiness with a launcher contract version, Router build version,
instance identifier, supported capabilities, and active authentication mode.
SDKs should reject missing required authentication capabilities before creating
business connections. Version negotiation is distinct from verifying the
artifact's publisher and integrity.

Return a usable connection descriptor, expose sanitized structured logs and
unexpected-exit details, and support deterministic disposal in both SDKs.
Document start order (Router, services, clients) and reverse shutdown order.
Make restart policy explicit: a new process loses tokens, registrations, and
active streams. Reconnection must not silently replay non-idempotent calls.

Pass only required environment values by default, with explicit overrides for
deployment needs. Both [Python subprocesses](https://docs.python.org/3/library/subprocess.html#subprocess.Popen)
and [Node spawn](https://nodejs.org/api/child_process.html#child_processspawncommand-args-options)
inherit the parent environment unless an environment is supplied. Reducing that
inheritance reduces accidental exposure; it does not sandbox a process running
as the same user.

## Migration and verification

The documentation points new applications at `demo/router/` for a compact
example and [best-practice](../best-practice/README.md) for a separately managed,
authenticated Router. The latter now includes explicit account initialization
through the Go CLI and scoped token commands; it does not implement the proposed
SDK bootstrap protocol. Existing `quickstart`, `showcase`, `py2ts`, and `ts2py`
runners still use Direct as references. Migrate their orchestration after the
authenticated managed-start flow is ready. Preserve Direct protocol coverage
and avoid silently changing existing service constructor behavior.

Follow-up validation should cover fresh installs outside a source checkout,
offline operation, incompatible/missing binaries, executable selection from an
untrusted working directory, rejected anonymous network binds, bootstrap
failure cleanup, missing credentials, client-versus-registration scopes,
parent death, and restart/token invalidation in both SDKs. No local listener
was started for this documentation and source review.

An isolated resolution check of each current launcher, simulating an installed
SDK outside a source checkout, selected an executable fixture from the caller's
working directory. The checks only resolved paths and did not execute those
fixtures. Documentation checks validated local links/anchors and parsed the
Python, JSON, and TypeScript examples; they do not replace runtime integration
tests for the proposed changes.

# meshcall-router

**English** | [简体中文](./README.zh-CN.md)

The standalone Go implementation of MeshCall's Router. Python and TypeScript
start this executable as a child process; they do not implement routing.

## Build and run

From the monorepo root, with Go 1.26 or newer:

```bash
go -C meshcall-router build -trimpath -o bin/meshcall-router ./cmd/meshcall-router
./meshcall-router/bin/meshcall-router --host 127.0.0.1 --port 8765
```

Only building requires Go. Each tagged release publishes prebuilt binaries for
Linux (amd64, arm64, 386, armv7, riscv64, ppc64le, s390x), macOS (amd64,
arm64), Windows (amd64, arm64, 386) and FreeBSD (amd64, arm64) with
a `checksums.txt`, and the
same binaries are bundled into the SDK packages (`scripts/release/bundle.sh`):
a PyPI platform wheel per target and an npm `meshcall-router-<os>-<cpu>`
package that `meshcall` lists as an optional dependency. Installing the SDK
therefore installs the binaries. The download path remains as a fallback for
platforms without a bundle; it fetches the release that matches the SDK
version into the per-user cache (`~/.cache/meshcall/router/<version>/`,
honouring `XDG_CACHE_HOME`), verifying the SHA-256 first:

```bash
uv run meshcall router install      # Python
npx meshcall router install         # Node
```

The archive holds two binaries, `meshcall-router` and the `meshcall` CLI
(`generate`, `export`, `router inspect`, `call`, `init`), installed side by
side. `uv run meshcall …` and `npx meshcall …` are thin launchers that run
the CLI (installing it on first use); they answer only `router install` and
`router path` themselves.

Launchers resolve the binary in this order: `MESHCALL_ROUTER_BINARY` or an
explicit `binary_path` / `binaryPath`; the bundled binary (the wheel's
`meshcall/bin/`, or the npm platform package); the per-user cache; `meshcall-router/bin` of the source checkout that contains the SDK;
`meshcall-router` on `PATH`. The working directory is never searched.
`meshcall router path` prints the resolved binary. Launchers never build or
fall back to a language-specific Router.

The CLI defaults to `127.0.0.1` and an automatically selected port. It prints one
`router.ready` JSON line on stdout after binding. Diagnostics go to stderr.
SIGINT/SIGTERM stop the process. SDKs additionally use
`--shutdown-on-stdin-close` so an abruptly terminated parent does not leave a
Router running. Both launchers wait for readiness, bound startup/shutdown waits,
reap failed children, and can restart after a failed bind.

Use `--unix-socket /tmp/meshcall.sock` instead of `--host` / `--port` for Unix
WebSocket transport. Existing paths are never replaced. Shutdown removes the
socket only when it is still the file owned by this Router.

`--deadline-grace` (default `500ms`) is added to every call's `timeout_ms`
before the Router enforces it, so the service's own `deadline_exceeded`
normally wins and the Router only steps in for an instance that has stopped
answering. `--idle-timeout` (default `45s`) closes any connection that sends
nothing for that long; clients and services keep healthy connections alive
with protocol pings every 15 s. `--breaker-threshold` (default `5`) and
`--breaker-cooldown` (default `10s`) configure the per-instance circuit
breaker. See [the protocol](../docs/protocol.md#timeouts).
`--max-connections` (default `10000`) bounds open WebSocket connections;
past it new upgrades get `503` with `Retry-After`. HTTP calls are not
counted (their admission control is still to be designed). HTTP
request bodies must keep arriving within `--idle-timeout` while the Router
is reading them.

HTTP calls (`/v1/…`, see [the protocol](../docs/protocol.md#http-calls))
have `--http-default-timeout` (default `30s`), `--http-max-timeout` (no
maximum by default), `--http-max-inflight` (default `10000`) and
`--http-cors-origin` (repeatable or comma-separated). `/healthz` and
`/readyz` are liveness and readiness probes.

## Environment variables

Every flag can be set as `MESHCALL_ROUTER_<FLAG>`: upper case, `-` becomes
`_` (`--idle-timeout` → `MESHCALL_ROUTER_IDLE_TIMEOUT=30s`,
`--auth-file` → `MESHCALL_ROUTER_AUTH_FILE`). A flag given on the command
line wins over its variable; an invalid value fails startup and names the
variable. `MESHCALL_ROUTER_AUTH` holds the auth document itself (the JSON an
auth file would contain), for platforms that inject secrets as variables;
setting it together with an auth file is an error. Routers started by the
Python and TypeScript launchers inherit the launching process's
environment, so these variables apply to them too.

## Authentication

Use `--auth-file /path/to/router-auth.json` (or `MESHCALL_ROUTER_AUTH_FILE`,
or the document inline in `MESHCALL_ROUTER_AUTH`) to enable account
authentication.
See [Router authentication and RPC management](../docs/router.md) for the
configuration format, scoped temporary tokens, and Python/TypeScript examples.
Without an auth file, the listener is in development mode; AuthService cannot
issue tokens. An invalid or empty supplied auth file fails startup.

Without credentials a caller is the reserved `anonymous` user (configurable
in the auth file, no permissions by default); `--max-anonymous-connections`
(default `1000`) caps those connections. `new-api-key ID` prints a new API
key once and the `api_keys` entry (its SHA-256 digest) for the auth file.

`hash-password` reads a password from stdin and prints a salted
PBKDF2-HMAC-SHA256 hash. For a hidden interactive password prompt:

```bash
python3 -c 'import getpass,sys; sys.stdout.write(getpass.getpass("Password: "))' \
  | ./meshcall-router/bin/meshcall-router hash-password
```

Copy only the resulting hash into the auth file. Passwords and tokens are sent
in the WebSocket HTTP handshake, never URL query strings or Router command-line
arguments. Use `wss://` through a TLS-terminating reverse proxy for network
deployments; the Go listener itself serves plain WebSocket.

## Docker

```bash
docker build -t meshcall-router ./meshcall-router
docker run --rm -p 127.0.0.1:8765:8765 \
  --mount type=bind,src=/absolute/path/router-auth.json,dst=/config/auth.json,readonly \
  meshcall-router --host 0.0.0.0 --port 8765 --auth-file /config/auth.json
```

or, configured entirely through the environment:

```bash
docker run --rm -p 127.0.0.1:8765:8765 \
  -e MESHCALL_ROUTER_HOST=0.0.0.0 -e MESHCALL_ROUTER_PORT=8765 \
  -e MESHCALL_ROUTER_AUTH="$(cat router-auth.json)" \
  meshcall-router
```

The image contains only the static Go executable and runs as UID/GID 65532.
Make the mounted auth file readable by that user. Production TLS and any
replication or deployment supervision belong outside this process.

## Verification

```bash
go -C meshcall-router vet ./...
go -C meshcall-router test -race ./...
```

Go tests cover ownership, route cleanup, authentication, scope delegation,
expiry/revocation, frame scheduling, sticky canonicalization and socket cleanup.
The Python/TypeScript suites and cross-language suite use the same executable.
CI builds it once and distributes that exact artifact to all runtime jobs.

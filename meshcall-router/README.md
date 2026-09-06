# meshcall-router

The standalone Go implementation of MeshCall's Router. Python and TypeScript
start this executable as a child process; they do not implement routing.

Router is the recommended connection model. In deployments, run this program
under Docker or another process manager and give clients/services its endpoint
and their credentials. SDK launchers support applications that explicitly own
a local Router. See [the startup review](../docs/router-launcher.md).

## Build and run

From the monorepo root, with Go 1.26 or newer:

```bash
go -C meshcall-router build -trimpath -o bin/meshcall-router ./cmd/meshcall-router
./meshcall-router/bin/meshcall-router --host 127.0.0.1 --port 8765
```

Only building requires Go. Deploy the executable for your OS/architecture and
set `MESHCALL_ROUTER_BINARY`, pass `binary_path` / `binaryPath`, or place
`meshcall-router` on `PATH`. SDK launchers also find `meshcall-router/bin` in a
source checkout and a package-local `bin` directory. Current Python/npm packages
do not bundle binaries; launchers never download, build, or fall back to a
language-specific Router.

Set an explicit trusted binary path for predictable SDK startup. Current source
discovery also searches the caller's working directory and its ancestors before
`PATH`; a source marker is not an integrity check. Readiness validates the wire
protocol and child identity, but does not check a Router release or capability
version. The startup review records proposed changes to these defaults.

The CLI defaults to `127.0.0.1` and an automatically selected port. It prints one
`router.ready` JSON line on stdout after binding. Diagnostics go to stderr.
SIGINT/SIGTERM stop the process. SDKs additionally use
`--shutdown-on-stdin-close` so an abruptly terminated parent does not leave a
Router running. Both launchers wait for readiness, bound startup/shutdown waits,
reap failed children, and can restart after a failed bind.

Use `--unix-socket /tmp/meshcall.sock` instead of `--host` / `--port` for Unix
WebSocket transport. Existing paths are never replaced. Shutdown removes the
socket only when it is still the file owned by this Router.

## Authentication

Use `--auth-file /path/to/router-auth.json` to enable account authentication.
See [Router authentication and RPC management](../docs/router.md) for the
configuration format, scoped temporary tokens, and Python/TypeScript examples.
Without an auth file, the listener is in development mode; AuthService cannot
issue tokens. An invalid or empty supplied auth file fails startup.
Development mode permits anonymous calls and service registration, even if a
network interface is selected. Configure authentication before exposing the
listener; the current runtime does not enforce that restriction automatically.

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

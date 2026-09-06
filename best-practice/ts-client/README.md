# meshcall-best-practice-client

Generated MeshCall client plus a handwritten CLI that connects to the
authenticated Go Router. Prepare accounts and start Router and the Python
worker using [the application README](../README.md).

From this directory, after building the runtime and installing dependencies:

```bash
yarn build
yarn cli
```

The CLI uses `MESHCALL_ROUTER_URL` (default `ws://127.0.0.1:8765`) and the
application's `.local/client.json`. It never starts Router or connects directly
to the Python service. Select another credential file, such as a scoped token
created by the separate issuer command, with:

```bash
MESHCALL_CLIENT_CREDENTIALS=../.local/chat-token.json yarn cli
```

For a headless run using the normal client account:

```bash
node dist/cli.js "Hello MeshCall"
```

`src/client.ts` and `src/models.ts` are generated; `src/cli.ts` and `src/config.ts`
are the handwritten application layer. The CLI checks Router authentication
before calling the service. See the application README for token
issuance/revocation, all three call shapes, and deployment configuration.

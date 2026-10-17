# meshcall-best-practice-client

Generated MeshCall TypeScript client package.

```bash
yarn install
yarn build
MESHCALL_ROUTER_URL=ws://127.0.0.1:8765 yarn cli
```

```typescript
import { Router } from "meshcall";
import { LlmServiceClient } from "meshcall-best-practice-client";

const router = await Router.connect();
const client = await LlmServiceClient.connect(router);
const sessions = await client.list_sessions();
```

`src/client.ts` and `src/models.ts` are generated from the Python service.
`src/cli.ts` is the small handwritten interactive layer. Start the Python
server from `../` with `uv run server` (or use the Go client in `../go-client`)
and point `MESHCALL_ROUTER_URL` at the same Router.

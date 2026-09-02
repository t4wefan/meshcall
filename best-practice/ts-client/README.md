# meshcall-best-practice-client

Generated MeshCall TypeScript client package.

```bash
yarn install
yarn build
MESHCALL_SERVER_URL=ws://127.0.0.1:8765 yarn cli
```

```typescript
import { LlmServiceClient, LlmServiceNewSessionRequest, NewSessionResponse, LlmServiceListSessionsRequest, ListSessionsResponse, LlmServiceCountTokensRequest, TokenCountResponse, LlmServiceAssemblePromptRequest, PromptAssembly, PromptChunk, LlmServiceStreamChatRequest, ChatChunk } from "meshcall-best-practice-client";
```

`src/client.ts` and `src/models.ts` are generated from the Python service.
`src/cli.ts` is the small handwritten interactive layer. Start the Python
server from `../` with `uv run server`, then use `yarn cli` to enter prompts.

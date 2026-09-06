import { pathToFileURL } from "node:url";
import { createInterface } from "node:readline";

const { MeshCallClient, RouterAuthClient, WebSocketRouter } =
  await import(pathToFileURL(process.argv[2]).href);
const input = createInterface({ input: process.stdin });
const options = JSON.parse(await new Promise((resolve) => input.once("line", resolve)));

if (options.action === "router") {
  const router = new WebSocketRouter({ authFile: options.authFile });
  try {
    await router.start();
    process.stdout.write(JSON.stringify({ port: router.boundPort, pid: router.pid }) + "\n");
    await new Promise((resolve) => input.once("close", resolve));
  } finally {
    await router.stop();
    input.close();
  }
} else {
  const client = new MeshCallClient(options.endpoint, undefined, { auth: options.auth });
  try {
    let result;
    if (options.action === "issue") {
      result = await new RouterAuthClient(client).issueToken({
        scopes: [{ service: options.service, methods: ["unary", "download", "upload"] }],
      });
    } else {
      const unary = await client.unary(options.service, "unary", { value: 7 });
      const items = [];
      for await (const item of client.serverStream(options.service, "download", { value: 64 })) items.push(item.value);
      const upload = await client.clientStream(options.service, "upload", { value: 7 },
        (async function* () { for (const value of items) yield { value }; })());
      result = { unary, count: items.length, upload };
    }
    process.stdout.write(JSON.stringify(result) + "\n");
  } finally {
    await client.close();
    input.close();
  }
}

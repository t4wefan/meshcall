// A TypeScript service for meshcall-py/tests/test_multilang_interop.py: it
// writes its contract, serves directly or through a Router (options as JSON),
// and stops when its stdin receives a line.
import {
  MeshCallServer, method, service, writeContract, z, type DuplexChannel, type MeshCallServerOptions,
} from "../../src/index.js";

const [contractPath, rawOptions] = process.argv.slice(2);
if (contractPath === undefined) throw new Error("Expected the contract output path");
const options = JSON.parse(rawOptions ?? "{}") as Omit<MeshCallServerOptions, "services">;
const handledBy = options.router?.instanceId ?? "typescript";

const Request = z.object({ value: z.int(), factor: z.int() }).meta({ id: "TypeScriptRequest" });
const Result = z.object({ product: z.int(), handled_by: z.string() }).meta({ id: "TypeScriptResult" });
const Item = z.object({ value: z.int(), handled_by: z.string() }).meta({ id: "TypeScriptItem" });
const Ticks = z.object({ since: z.int().nullable().default(null) }).meta({ id: "TypeScriptTicks" });
const Tick = z.object({ seq: z.int() }).meta({ id: "TypeScriptTick" });
type Request = z.infer<typeof Request>;
type Item = z.infer<typeof Item>;

@service("test.v1.TypeScriptInteropService", { description: "" })
class TypeScriptInteropService {
  @method({ description: "", request: Request, response: Result })
  async multiply({ value, factor }: Request) {
    return { product: value * factor, handled_by: handledBy };
  }

  @method.serverStream({ description: "", request: Request, outputItem: Item })
  async *download({ value, factor }: Request) {
    for (let index = 0; index < value; index += 1) yield { value: index * factor, handled_by: handledBy };
  }

  @method.clientStream({ description: "", request: Request, inputItem: Item, response: Result })
  async upload({ value, factor }: Request, items: AsyncIterable<Item>) {
    let product = value;
    for await (const item of items) product += item.value * factor;
    return { product, handled_by: handledBy };
  }

  @method.subscription({ description: "", cursor: "seq", resume: "since", request: Ticks, outputItem: Tick })
  async *ticks({ since }: z.infer<typeof Ticks>) {
    for (let seq = since === null ? 0 : since + 1; seq < 5; seq += 1) yield { seq };
  }

  @method.duplex({ description: "", reason: "interop: answers each item as it arrives", request: Request, inputItem: Item, outputItem: Item })
  async exchange({ factor }: Request, channel: DuplexChannel<Item, Item>) {
    for await (const item of channel) await channel.send({ value: item.value * factor, handled_by: handledBy });
  }
}

await writeContract([TypeScriptInteropService], contractPath);
const server = new MeshCallServer({ services: [new TypeScriptInteropService()], accessLog: false, ...options });
await server.start();
process.stdout.write(JSON.stringify({
  port: options.router !== undefined || options.unixPath !== undefined || options.pipe !== undefined ? null : server.boundPort,
}) + "\n");

process.stdin.resume();
await new Promise((resolve) => process.stdin.once("data", resolve));
process.stdin.pause();
await server.close();

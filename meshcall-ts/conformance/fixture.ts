/** Reference TypeScript fixture for the conformance suite (see meshcall-router/conformance/FIXTURE.md). */
import { setTimeout as delay } from "node:timers/promises";

import {
  MeshCallError, MeshCallServer, method, service, z,
  type DuplexChannel, type RpcContext, type WebSocketEndpoint,
} from "../src/index.js";

const Echo = z.object({ value: z.int(), text: z.string() }).meta({ id: "Echo" });
const Fail = z.object({ code: z.string(), message: z.string(), retryable: z.boolean().default(false) }).meta({ id: "Fail" });
const Ok = z.object({ ok: z.boolean() }).meta({ id: "Ok" });
const Slow = z.object({ ms: z.int(), tag: z.string().default("") }).meta({ id: "Slow" });
const Tag = z.object({ tag: z.string() }).meta({ id: "Tag" });
const Empty = z.object({}).meta({ id: "Empty" });
const Tags = z.object({ tags: z.array(z.string()) }).meta({ id: "Tags" });
const Count = z.object({ n: z.int(), gap_ms: z.int().default(0) }).meta({ id: "Count" });
const Index = z.object({ index: z.int() }).meta({ id: "Index" });
const Sum = z.object({ offset: z.int().default(0) }).meta({ id: "Sum" });
const Value = z.object({ value: z.int() }).meta({ id: "Value" });
const Total = z.object({ total: z.int() }).meta({ id: "Total" });
const Metadata = z.object({ metadata: z.record(z.string(), z.string()) }).meta({ id: "Metadata" });
const Instance = z.object({ instance: z.string() }).meta({ id: "Instance" });
const Chat = z.object({ prefix: z.string().default("") }).meta({ id: "Chat" });
const Text = z.object({ text: z.string() }).meta({ id: "Text" });
const Size = z.object({ bytes: z.int() }).meta({ id: "Size" });
const Drain = z.object({ delay_ms: z.int().default(0) }).meta({ id: "Drain" });

type Text = z.infer<typeof Text>;

/** unix:///path and pipe://NAME select the local transport. */
function routerEndpoint(url: string): WebSocketEndpoint {
  if (url.startsWith("unix://")) return { unixPath: url.slice("unix://".length) };
  if (url.startsWith("pipe://")) return { pipe: url.slice("pipe://".length) };
  return url;
}

let server: MeshCallServer;

@service("conformance.v1.Fixture", { description: "Deterministic behaviours the conformance suite drives through the Router." })
class Fixture {
  private readonly cancelled: string[] = [];
  private readonly invoked: string[] = [];

  @method({ description: "Return the input unchanged.", readOnly: true, request: Echo, response: Echo })
  async echo(request: z.infer<typeof Echo>) {
    return request;
  }

  @method({ description: "Raise an application error with the given code.", idempotent: true, request: Fail, response: Ok })
  async fail({ code, message, retryable }: z.infer<typeof Fail>): Promise<z.infer<typeof Ok>> {
    throw new MeshCallError(code, message, { retryable, details: { from: "fixture" } });
  }

  @method({ description: "Return a response that breaks this method's own contract.", readOnly: true, request: Empty, response: Ok })
  async bad_response() {
    return { ok: "not-a-bool" } as unknown as z.infer<typeof Ok>;
  }

  @method({ description: "Sleep ms milliseconds; record tag if cancelled meanwhile.", idempotent: true, request: Slow, response: Tag })
  async slow({ ms, tag }: z.infer<typeof Slow>, { signal }: RpcContext) {
    this.invoked.push(tag);
    try {
      await delay(ms, undefined, { signal });
    } catch (error) {
      if (signal.aborted) this.cancelled.push(tag);
      throw error;
    }
    return { tag };
  }

  @method({ description: "Tags of every slow call that started, in order.", readOnly: true, request: Empty, response: Tags })
  async invoked_tags() {
    return { tags: [...this.invoked] };
  }

  @method({ description: "Tags of slow calls that observed cancellation.", readOnly: true, request: Empty, response: Tags })
  async cancelled_tags() {
    return { tags: [...this.cancelled] };
  }

  @method.serverStream({ description: "Yield 0..n-1 with gap_ms between items.", readOnly: true, request: Count, outputItem: Index })
  async *count({ n, gap_ms }: z.infer<typeof Count>, { signal }: RpcContext) {
    for (let index = 0; index < n; index += 1) {
      if (gap_ms > 0) await delay(gap_ms, undefined, { signal });
      yield { index };
    }
  }

  @method.clientStream({ description: "Add every streamed value to offset.", idempotent: true, request: Sum, inputItem: Value, response: Total })
  async sum({ offset }: z.infer<typeof Sum>, items: AsyncIterable<z.infer<typeof Value>>) {
    let total = offset;
    for await (const item of items) total += item.value;
    return { total };
  }

  @method({ description: "Return the call metadata and set trailers describing it.", readOnly: true, request: Empty, response: Metadata })
  async metadata_echo(_: unknown, ctx: RpcContext) {
    ctx.setTrailer("echoed", "yes");
    ctx.setTrailer("count", String(Object.keys(ctx.metadata).length));
    return { metadata: { ...ctx.metadata } };
  }

  @method({ description: "Return a text of bytes bytes.", readOnly: true, request: Size, response: Text })
  async big({ bytes }: z.infer<typeof Size>) {
    return { text: "x".repeat(bytes) };
  }

  @method.serverStream({ description: "Yield one small item, then one of bytes bytes.", readOnly: true, request: Size, outputItem: Text })
  async *big_items({ bytes }: z.infer<typeof Size>) {
    yield { text: "small" };
    yield { text: "x".repeat(bytes) };
  }

  @method.duplex({
    description: "Echo each input with prefix; send done:<inputs> when input ends.",
    reason: "conformance: answers interleave with input",
    request: Chat, inputItem: Text, outputItem: Text,
  })
  async chat({ prefix }: z.infer<typeof Chat>, channel: DuplexChannel<Text, Text>) {
    let count = 0;
    for await (const item of channel) {
      count += 1;
      await channel.send({ text: prefix + item.text });
    }
    await channel.send({ text: "done:" + count });
  }

  @method.duplex({
    description: "Send hello, close the output, then read the input to its end.",
    reason: "conformance: output ends before input",
    request: Empty, inputItem: Text, outputItem: Text,
  })
  async announce(_: unknown, channel: DuplexChannel<Text, Text>) {
    await channel.send({ text: "hello" });
    await channel.closeSend();
    for await (const _item of channel) { /* read to the end */ }
  }

  @method({ description: "Drain and stop this instance after delay_ms.", destructive: true, request: Drain, response: Ok })
  async drain_self({ delay_ms }: z.infer<typeof Drain>) {
    void delay(delay_ms).then(() => server.close({ drain: true, drainTimeoutMs: 30_000 })).then(() => process.exit(0));
    return { ok: true };
  }
}

@service("conformance.v1.Targeted", { description: "A targeted service: every call must name the instance it is for.", targeted: true })
class Targeted {
  @method({ description: "Return the id of the instance that served the call.", readOnly: true, request: Empty, response: Instance })
  async whoami(_: unknown, ctx: RpcContext) {
    return { instance: ctx.instanceId };
  }
}

const ping = Number(process.env.MESHCALL_PING_INTERVAL_MS ?? "15000");
server = new MeshCallServer({
  services: [new Fixture(), new Targeted()],
  router: { endpoint: routerEndpoint(process.env.MESHCALL_ROUTER_URL as string), instanceId: process.env.MESHCALL_CONFORMANCE_INSTANCE ?? "fixture-1" },
  pingIntervalMs: ping, pingTimeoutMs: Math.max(1000, ping * 2),
  ...(process.env.MESHCALL_MAX_FRAME_SIZE === undefined ? {} : { maxFrameSize: Number(process.env.MESHCALL_MAX_FRAME_SIZE) }),
  accessLog: false,
});
await server.start();
process.stderr.write("fixture registered\n");

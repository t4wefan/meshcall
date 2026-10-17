import { mutates, query, service, stream, z, type RpcContext } from "meshcall";

const GreetingRequest = z.object({
  name: z.string(),
  repeat: z.int().min(1).max(5),
}).meta({ id: "GreetingRequest" });

const GreetingResult = z.object({
  message: z.string(),
  handled_by: z.string(),
}).meta({ id: "GreetingResult" });

const CountRequest = z.object({ count: z.int().min(0).max(1000) }).meta({ id: "CountRequest" });
const NumberItem = z.object({ value: z.int() }).meta({ id: "NumberItem" });
const SumRequest = z.object({ offset: z.int().default(0) }).meta({ id: "SumRequest" });
const SumResult = z.object({ total: z.int() }).meta({ id: "SumResult" });

@service("demo.v1.TypeScriptGreetingService", {
  description: "A TypeScript service consumed by the generated Python client.",
})
export class TypeScriptGreetingService {
  @stream({
    description: "Stream the integers from 0 up to, but not including, count.",
    request: CountRequest, outputItem: NumberItem,
  })
  async *count({ count }: z.infer<typeof CountRequest>, { signal }: RpcContext) {
    for (let value = 0; value < count; value += 1) {
      signal.throwIfAborted();
      yield { value };
    }
  }

  @mutates({
    description: "Add every streamed value to offset and return the total.",
    idempotent: true, request: SumRequest, inputItem: NumberItem, response: SumResult,
  })
  async sum({ offset }: z.infer<typeof SumRequest>, items: AsyncIterable<z.infer<typeof NumberItem>>, { logger }: RpcContext) {
    let total = offset;
    for await (const item of items) total += item.value;
    logger.info("summed input stream");
    return { total };
  }

  @query({
    description: "Greet name repeat times and say which runtime answered.",
    request: GreetingRequest, response: GreetingResult,
  })
  async greet({ name, repeat }: z.infer<typeof GreetingRequest>) {
    return {
      message: Array.from({ length: repeat }, () => `Hello, ${name}!`).join(" "),
      handled_by: "typescript",
    };
  }
}

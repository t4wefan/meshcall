#!/usr/bin/env node

import { createInterface } from "node:readline/promises";

import { MeshCallClient } from "@meshcall/runtime";

import { LlmServiceClient } from "./index.js";

const url = process.env.MESHCALL_SERVER_URL ?? "ws://127.0.0.1:8765";
const defaultPrompt =
  "Why should a server stream tokens instead of waiting for one big answer?";
const rpc = new MeshCallClient(url);
const client = new LlmServiceClient(rpc);

let session: { id: string; title: string; message_count: number };

async function* promptChunks(text: string): AsyncIterable<{ text: string }> {
  for (const chunk of text.match(/\S+\s*/gu) ?? [text]) {
    yield { text: chunk };
  }
}

async function createSession(title = "best-practice"): Promise<void> {
  const created = await client.new_session(title);
  session = created.session;
  console.log(`session ${session.id} · ${session.title}`);
}

async function listSessions(): Promise<void> {
  const result = await client.list_sessions();
  if (result.sessions.length === 0) {
    console.log("sessions: (none)");
    return;
  }
  console.log(
    `sessions: ${result.sessions
      .map((item) => `${item.title} (${item.message_count} messages)`)
      .join(", ")}`,
  );
}

async function answer(prompt: string): Promise<void> {
  const assembled = await client.assemble_prompt(promptChunks(prompt));
  const tokenCount = await client.count_tokens(assembled.text);
  console.log(`prompt chunks: ${assembled.chunk_count}`);
  console.log(
    `prompt tokens: ${tokenCount.count} (${tokenCount.tokenizer})`,
  );
  process.stdout.write("assistant> ");
  for await (const chunk of client.stream_chat(
    session.id,
    assembled.text,
    24,
  )) {
    process.stdout.write(chunk.delta);
  }
  process.stdout.write("\n");
  session.message_count += 2;
}

async function runInteractive(): Promise<void> {
  const readline = createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: true,
  });
  readline.on("SIGINT", () => readline.close());
  console.log("MeshCall best-practice CLI");
  console.log("Enter a prompt. /new, /sessions, /tokens TEXT, /help, /quit");
  try {
    while (true) {
      let line: string;
      try {
        line = await readline.question("you> ");
      } catch {
        break;
      }
      const input = line.trim();
      if (input === "" || input === "/quit" || input === "/exit") {
        if (input !== "") {
          break;
        }
        continue;
      }
      if (input === "/help") {
        console.log("commands: /new [TITLE], /sessions, /tokens TEXT, /quit");
        continue;
      }
      if (input === "/sessions") {
        await listSessions();
        continue;
      }
      if (input.startsWith("/tokens ")) {
        const result = await client.count_tokens(input.slice("/tokens ".length));
        console.log(`tokens: ${result.count} (${result.tokenizer})`);
        continue;
      }
      if (input === "/new" || input.startsWith("/new ")) {
        await createSession(input.slice("/new".length).trim() || "best-practice");
        continue;
      }
      await answer(line);
    }
  } finally {
    readline.close();
  }
}

async function runOnce(prompt: string): Promise<void> {
  console.log(`prompt: ${prompt}`);
  await answer(prompt);
  await listSessions();
}

async function main(): Promise<void> {
  await createSession();
  const arguments_ = process.argv.slice(2);
  if (arguments_.length > 0) {
    await runOnce(arguments_.join(" "));
  } else if (process.stdin.isTTY && process.stdout.isTTY) {
    await runInteractive();
  } else {
    await runOnce(defaultPrompt);
  }
}

try {
  await main();
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`meshcall cli: ${message}`);
  process.exitCode = 1;
} finally {
  await rpc.close();
}

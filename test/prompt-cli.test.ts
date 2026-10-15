import { spawn } from "node:child_process";
import { once } from "node:events";
import {
  createServer,
  type IncomingHttpHeaders,
  type ServerResponse,
} from "node:http";
import type { AddressInfo } from "node:net";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

import { deferred } from "./support.js";

// The deterministic gate for the one-shot CLI: a local OpenAI Responses SSE
// server stands in for camelStream. No network or secret is involved.

const repositoryRoot = resolve(import.meta.dirname, "..", "..", "..");

type Captured = {
  readonly headers: IncomingHttpHeaders;
  readonly body: unknown;
};

type Reply =
  | {
      readonly kind: "sse";
      readonly body: string;
      readonly disconnect?: boolean;
    }
  | { readonly kind: "status"; readonly status: number; readonly body: string }
  | { readonly kind: "hold" };

/** The subset of OpenAI Responses stream events this fixture serializes. */
type ResponsesStreamEvent = {
  readonly type: string;
  readonly output_index?: number;
  readonly delta?: string;
  readonly item?: {
    readonly type: "message";
    readonly id: string;
    readonly role: "assistant";
    readonly content: readonly {
      readonly type: "output_text";
      readonly text: string;
    }[];
  };
  readonly response?: {
    readonly id?: string;
    readonly status?: string;
    readonly usage?: {
      readonly input_tokens: number;
      readonly output_tokens: number;
      readonly total_tokens: number;
    };
    readonly error?: { readonly code: string; readonly message: string };
  };
};

const frame = (event: ResponsesStreamEvent): string =>
  `data: ${JSON.stringify(event)}\n\n`;

const message = { type: "message", id: "msg_1", role: "assistant" } as const;

const completedAnswer = [
  frame({ type: "response.created", response: { id: "resp_1" } }),
  frame({
    type: "response.output_item.added",
    output_index: 0,
    item: { ...message, content: [] },
  }),
  frame({ type: "response.output_text.delta", output_index: 0, delta: "hé" }),
  frame({
    type: "response.output_text.delta",
    output_index: 0,
    delta: "🙂llo",
  }),
  frame({
    type: "response.output_item.done",
    output_index: 0,
    item: { ...message, content: [{ type: "output_text", text: "hé🙂llo" }] },
  }),
  frame({
    type: "response.completed",
    response: {
      id: "resp_1",
      status: "completed",
      usage: { input_tokens: 2, output_tokens: 3, total_tokens: 5 },
    },
  }),
].join("");

/** Write `body` in small, UTF-8-splitting fragments. */
async function writeFragmented(
  response: ServerResponse,
  body: string,
): Promise<void> {
  const bytes = Buffer.from(body, "utf8");
  const sizes = [1, 2, 3, 5, 8];
  for (let offset = 0, index = 0; offset < bytes.byteLength; index += 1) {
    const size = sizes[index % sizes.length] ?? 1;
    response.write(bytes.subarray(offset, offset + size));
    offset += size;
    await new Promise<void>((next) => setImmediate(next));
  }
}

async function responsesServer(reply: Reply) {
  const requests: Captured[] = [];
  const received = deferred();
  const closed = deferred();
  const server = createServer((request, response) => {
    void (async () => {
      const chunks: Buffer[] = [];
      for await (const chunk of request)
        // SAFETY: Node's IncomingMessage async iterator yields request-body Buffers.
        chunks.push(Buffer.from(chunk as Uint8Array));
      requests.push({
        headers: request.headers,
        body: JSON.parse(Buffer.concat(chunks).toString("utf8")),
      });
      received.resolve();
      response.on("close", () => {
        closed.resolve();
      });
      if (reply.kind === "status") {
        response.statusCode = reply.status;
        response.end(reply.body);
        return;
      }
      response.setHeader("content-type", "text/event-stream");
      response.flushHeaders();
      if (reply.kind === "hold") return;
      await writeFragmented(response, reply.body);
      if (reply.disconnect === true) response.destroy();
      else response.end();
    })();
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  // SAFETY: listen(0, "127.0.0.1") binds a TCP socket, so address() is AddressInfo.
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${String(port)}/v1`,
    requests,
    received: received.promise,
    closed: closed.promise,
    close: async () => {
      server.closeAllConnections();
      server.close();
      await once(server, "close");
    },
  };
}

type CliResult = {
  readonly status: number | null;
  readonly stdout: string;
  readonly stderr: string;
};

async function runCli(
  args: readonly string[],
  environment: Readonly<Record<string, string>>,
  interruptWhen?: Promise<void>,
): Promise<CliResult> {
  const child = spawn(
    process.execPath,
    ["--import", "tsx", "packages/mule/examples/prompt.ts", ...args],
    {
      cwd: repositoryRoot,
      env: {
        ...process.env,
        CAMEL_API_KEY: "",
        CAMEL_API_URL: "",
        CAMEL_MODEL: "",
        ...environment,
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
  child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
  void interruptWhen?.then(() => child.kill("SIGINT"));
  // SAFETY: a ChildProcess "exit" event's first argument is its exit code.
  const [status] = (await once(child, "exit")) as [number | null];
  return {
    status,
    stdout: Buffer.concat(stdout).toString("utf8"),
    stderr: Buffer.concat(stderr).toString("utf8"),
  };
}

describe("Mule prompt CLI", () => {
  it("streams fragmented Responses deltas and exits 0", async () => {
    const server = await responsesServer({
      kind: "sse",
      body: completedAnswer,
    });
    try {
      const result = await runCli(["say hello"], {
        CAMEL_API_KEY: "test-secret",
        CAMEL_API_URL: server.url,
      });

      expect(result).toEqual({ status: 0, stdout: "hé🙂llo\n", stderr: "" });
      expect(server.requests).toHaveLength(1);
      expect(server.requests[0]?.headers.authorization).toBe(
        "Bearer test-secret",
      );
      expect(server.requests[0]?.body).toMatchObject({
        model: "auto",
        stream: true,
      });
      expect(server.requests[0]?.body).not.toHaveProperty("tools");
    } finally {
      await server.close();
    }
  }, 30_000);

  it("rejects invalid invocation and configuration with exit 2", async () => {
    await expect(runCli([], { CAMEL_API_KEY: "k" })).resolves.toMatchObject({
      status: 2,
    });
    await expect(
      runCli(["one", "two"], { CAMEL_API_KEY: "k" }),
    ).resolves.toMatchObject({ status: 2 });
    await expect(runCli(["hello"], {})).resolves.toMatchObject({ status: 2 });
  }, 30_000);

  it.each([
    {
      name: "a provider HTTP failure",
      reply: { kind: "status", status: 503, body: "provider unavailable" },
      stdout: "",
    },
    {
      name: "malformed SSE",
      reply: { kind: "sse", body: "data: {not-json}\n\n" },
      stdout: "",
    },
    {
      name: "a provider-failed response",
      reply: {
        kind: "sse",
        body: frame({
          type: "response.failed",
          response: {
            status: "failed",
            error: { code: "server_error", message: "boom" },
          },
        }),
      },
      stdout: "",
    },
    {
      name: "a disconnect after partial output",
      reply: {
        kind: "sse",
        body: [
          frame({
            type: "response.output_item.added",
            output_index: 0,
            item: { ...message, content: [] },
          }),
          frame({
            type: "response.output_text.delta",
            output_index: 0,
            delta: "partial",
          }),
        ].join(""),
        disconnect: true,
      },
      stdout: "partial",
    },
  ] satisfies readonly { name: string; reply: Reply; stdout: string }[])(
    "exits 1 without leaking the credential for $name",
    async ({ reply, stdout }) => {
      const server = await responsesServer(reply);
      try {
        const result = await runCli(["hello"], {
          CAMEL_API_KEY: "test-secret",
          CAMEL_API_URL: server.url,
        });

        expect(result.status).toBe(1);
        expect(result.stdout).toBe(stdout);
        // Pi's OpenAI SDK may log its own parse diagnostics first.
        expect(result.stderr.endsWith("Mule prompt failed\n")).toBe(true);
        expect(result.stderr).not.toContain("test-secret");
      } finally {
        await server.close();
      }
    },
    30_000,
  );

  it("maps SIGINT to 130 after tearing down the provider stream", async () => {
    const server = await responsesServer({ kind: "hold" });
    try {
      const result = await runCli(
        ["hello"],
        { CAMEL_API_KEY: "test-secret", CAMEL_API_URL: server.url },
        server.received,
      );
      await server.closed;

      expect(result).toEqual({
        status: 130,
        stdout: "",
        stderr: "Mule prompt cancelled\n",
      });
    } finally {
      await server.close();
    }
  }, 30_000);
});

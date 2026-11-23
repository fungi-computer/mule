import { expect, it } from "vitest";
import { assistantTextBlocks } from "../src/assistant-text.js";

const sign = (phase?: "commentary" | "final_answer") =>
  JSON.stringify({ v: 1, id: "msg_1", ...(phase && { phase }) });

it("lets a final answer replace the commentary block that precedes it", () => {
  expect(
    assistantTextBlocks([
      { type: "text", text: "Hello", textSignature: sign("commentary") },
      { type: "text", text: "Hello", textSignature: sign("final_answer") },
    ]),
  ).toEqual(["Hello"]);
});

it("keeps commentary when the message has no final answer", () => {
  expect(
    assistantTextBlocks([
      { type: "text", text: "Looking", textSignature: sign("commentary") },
      { type: "toolCall" },
    ]),
  ).toEqual(["Looking"]);
});

it("keeps repeated unsigned or legacy-signed text untouched", () => {
  expect(
    assistantTextBlocks([
      { type: "text", text: "Go" },
      { type: "text", text: "Go", textSignature: "legacy-id" },
      { type: "text", text: "Go", textSignature: sign() },
    ]),
  ).toEqual(["Go", "Go", "Go"]);
});

it("hides a differing commentary preamble when a final answer exists", () => {
  expect(
    assistantTextBlocks([
      {
        type: "text",
        text: "Let me check.",
        textSignature: sign("commentary"),
      },
      { type: "text", text: "Done.", textSignature: sign("final_answer") },
    ]),
  ).toEqual(["Done."]);
});

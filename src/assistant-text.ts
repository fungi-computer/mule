import * as Schema from "effect/Schema";

/** Pi's OpenAI Responses signature: the message id plus its optional phase. */
const TextSignature = Schema.fromJsonString(
  Schema.Struct({
    v: Schema.Literal(1),
    id: Schema.String,
    phase: Schema.optionalKey(Schema.Literals(["commentary", "final_answer"])),
  }),
);
const decodeTextSignature = Schema.decodeUnknownOption(TextSignature);

export type AssistantPart = Readonly<{
  type: string;
  text?: string | undefined;
  textSignature?: string | undefined;
}>;

function phaseOf(part: AssistantPart): "commentary" | "final_answer" | null {
  if (part.textSignature === undefined) return null;
  const decoded = decodeTextSignature(part.textSignature);
  return decoded._tag === "Some" ? (decoded.value.phase ?? null) : null;
}

/**
 * The reply a reader sees: every text block, except that an OpenAI
 * `commentary` block is a draft of the turn and yields to a `final_answer`
 * block in the same message. A commentary preamble that differs from the final
 * answer is hidden here too: it stays in storage and in provider replay, and
 * is only left out of what a person reads. Commentary stays visible when no
 * final answer exists, such as progress text ahead of a tool call.
 */
export function assistantTextBlocks(
  content: readonly AssistantPart[],
): readonly string[] {
  const texts = content.flatMap((part) =>
    part.type === "text" && part.text !== undefined
      ? [{ text: part.text, phase: phaseOf(part) }]
      : [],
  );
  const hasFinal = texts.some(({ phase }) => phase === "final_answer");
  return texts.flatMap(({ text, phase }) =>
    hasFinal && phase === "commentary" ? [] : [text],
  );
}

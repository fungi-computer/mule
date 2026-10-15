import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

describe("Mule without code generation from strings", () => {
  it("runs the built Promise root through a Pi tool round trip", () => {
    const result = spawnSync(
      process.execPath,
      [
        "--disallow-code-generation-from-strings",
        resolve(import.meta.dirname, "fixtures", "no-codegen.mjs"),
      ],
      { encoding: "utf8" },
    );

    expect(result.status, result.stderr).toBe(0);
  });
});

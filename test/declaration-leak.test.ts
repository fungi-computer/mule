import { readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { describe, expect, expectTypeOf, it } from "vitest";

import type * as EffectRoot from "../src/effect.js";
import type * as PromiseRoot from "../src/index.js";

const dist = resolve(import.meta.dirname, "..", "dist");

/** Every declaration file reachable from `entry` through relative imports. */
async function declarationGraph(entry: string): Promise<Map<string, string>> {
  const graph = new Map<string, string>();
  const pending = [entry];
  for (let path = pending.pop(); path !== undefined; path = pending.pop()) {
    if (graph.has(path)) continue;
    const source = await readFile(path, "utf8");
    graph.set(path, source);
    for (const [, specifier] of source.matchAll(/from\s+["'](\.[^"']+)["']/gu))
      if (specifier !== undefined)
        pending.push(
          resolve(dirname(path), specifier.replace(/\.js$/u, ".d.ts")),
        );
  }
  return graph;
}

const importsEffect = (source: string): boolean =>
  /(?:from|import)\s*\(?\s*["']effect(?:\/[^"']*)?["']/u.test(source);

describe("Mule declaration surfaces", () => {
  it("keeps Effect out of every declaration reachable from the Promise root", async () => {
    const graph = await declarationGraph(join(dist, "index.d.ts"));

    expect(
      [...graph.keys()].map((path) => path.slice(dist.length + 1)).sort(),
    ).toEqual(["contract.d.ts", "index.d.ts", "provider-status.d.ts"]);
    for (const [path, source] of graph)
      expect(importsEffect(source), path).toBe(false);
  });

  it("exposes Effect's typed channels from the ./effect root", async () => {
    const source = await readFile(join(dist, "effect.d.ts"), "utf8");

    expect(importsEffect(source)).toBe(true);
  });

  it("shares one declaration of every data contract across both roots", () => {
    expectTypeOf<PromiseRoot.MuleEvent>().toEqualTypeOf<EffectRoot.MuleEvent>();
    expectTypeOf<PromiseRoot.MuleError>().toEqualTypeOf<EffectRoot.MuleError>();
    expectTypeOf<PromiseRoot.MuleRunOutcome>().toEqualTypeOf<EffectRoot.MuleRunOutcome>();
    expectTypeOf<PromiseRoot.MuleRunSettings>().toEqualTypeOf<EffectRoot.MuleRunSettings>();
    expectTypeOf<PromiseRoot.MuleToolExecutionPart>().toEqualTypeOf<EffectRoot.MuleToolExecutionPart>();
  });
});

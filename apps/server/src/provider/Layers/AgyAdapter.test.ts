import { describe, expect, it } from "@effect/vitest";

import { parseAgyResume, usageFromAgy } from "./AgyAdapter.ts";

describe("parseAgyResume", () => {
  it("accepts a v1 cursor with a conversation id", () => {
    expect(parseAgyResume({ schemaVersion: 1, conversationId: "  abc-123  " })).toEqual({
      conversationId: "abc-123",
    });
  });

  it("rejects foreign or malformed cursors", () => {
    expect(parseAgyResume(undefined)).toBeUndefined();
    expect(parseAgyResume({ schemaVersion: 2, conversationId: "abc" })).toBeUndefined();
    expect(parseAgyResume({ schemaVersion: 1, conversationId: "   " })).toBeUndefined();
    expect(parseAgyResume({ schemaVersion: 1 })).toBeUndefined();
    expect(parseAgyResume("not-a-cursor")).toBeUndefined();
  });
});

describe("usageFromAgy", () => {
  it("maps agy usage fields onto the canonical snapshot", () => {
    const usage = usageFromAgy({
      input_tokens: 10415,
      output_tokens: 657,
      thinking_tokens: 616,
      cache_read_tokens: 8113,
      total_tokens: 11072,
    }) as Record<string, unknown>;
    expect(usage).toBeDefined();
    expect(usage.usedTokens).toBe(11072);
    expect(usage.inputTokens).toBe(10415);
    expect(usage.outputTokens).toBe(657);
    expect(usage.reasoningOutputTokens).toBe(616);
    expect(usage.cachedInputTokens).toBe(8113);
  });

  it("returns undefined for non-object or non-numeric payloads", () => {
    expect(usageFromAgy(undefined)).toBeUndefined();
    expect(usageFromAgy("nope")).toBeUndefined();
    expect(usageFromAgy({ total_tokens: "lots" })).toMatchObject({ usedTokens: 0 });
  });
});

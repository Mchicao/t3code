import { describe, expect, it } from "@effect/vitest";

import {
  agyUserEventLine,
  appendAgyImageAttachments,
  parseAgyResume,
  usageFromAgy,
} from "./AgyAdapter.ts";

describe("agyUserEventLine", () => {
  it("wraps a prompt as the NDJSON user event stream-json mode consumes", () => {
    expect(JSON.parse(agyUserEventLine("hola mundo"))).toEqual({
      event: "user",
      message: { content: "hola mundo" },
    });
  });

  it("escapes newlines inside the prompt so one line is one event", () => {
    const line = agyUserEventLine("linea1\nlinea2");
    expect(line.split("\n")).toHaveLength(1);
    expect(JSON.parse(line).message.content).toBe("linea1\nlinea2");
  });
});

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
  it("adds cache reads back into canonical input/context usage", () => {
    const usage = usageFromAgy({
      input_tokens: 10415,
      output_tokens: 657,
      thinking_tokens: 616,
      cache_read_tokens: 8113,
      total_tokens: 11072,
    }) as Record<string, unknown>;
    expect(usage).toBeDefined();
    expect(usage.usedTokens).toBe(19185);
    expect(usage.inputTokens).toBe(18528);
    expect(usage.outputTokens).toBe(657);
    expect(usage.reasoningOutputTokens).toBe(616);
    expect(usage.cachedInputTokens).toBe(8113);
    expect(usage.lastInputTokens).toBe(18528);
  });

  it("turns cumulative persistent-session results into per-turn usage", () => {
    const usage = usageFromAgy(
      {
        input_tokens: 30662,
        output_tokens: 8,
        thinking_tokens: 0,
        cache_read_tokens: 30214,
        total_tokens: 30670,
      },
      {
        cumulativeResult: true,
        previousCumulative: {
          input_tokens: 30384,
          output_tokens: 4,
          thinking_tokens: 0,
          cache_read_tokens: 0,
          total_tokens: 30388,
        },
      },
    ) as Record<string, unknown>;

    expect(usage.usedTokens).toBe(30496);
    expect(usage.inputTokens).toBe(30492);
    expect(usage.cachedInputTokens).toBe(30214);
    expect(usage.outputTokens).toBe(4);
    expect(usage.totalProcessedTokens).toBe(30670);
  });

  it("returns undefined for non-object or non-numeric payloads", () => {
    expect(usageFromAgy(undefined)).toBeUndefined();
    expect(usageFromAgy("nope")).toBeUndefined();
    expect(usageFromAgy({ total_tokens: "lots" })).toBeUndefined();
  });
});

describe("appendAgyImageAttachments", () => {
  it("projects image metadata as a delimited path manifest", () => {
    const prompt = appendAgyImageAttachments("Review this screenshot", [
      {
        name: "screen.png",
        mimeType: "image/png",
        sizeBytes: 1234,
        path: "/tmp/t3/attachments/abc.png",
      },
    ]);
    expect(prompt).toContain("Review this screenshot");
    expect(prompt).toContain("<t3_attached_images>");
    expect(prompt).toContain('"path":"/tmp/t3/attachments/abc.png"');
    expect(prompt).toContain('"mimeType":"image/png"');
  });
});

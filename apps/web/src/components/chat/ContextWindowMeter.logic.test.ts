import { ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import {
  formatContextWindowCompactionMessage,
  resolveContextWindowModelDisplayName,
  resolveContextWindowTokenBreakdown,
} from "./ContextWindowMeter.logic";

describe("resolveContextWindowModelDisplayName", () => {
  it("uses the selected model from the exact provider instance", () => {
    const primaryInstanceId = ProviderInstanceId.make("codex");
    const selectedInstanceId = ProviderInstanceId.make("codex-work");
    const modelOptionsByInstance = new Map([
      [
        primaryInstanceId,
        [{ slug: "gpt-5.6-sol", name: "Primary profile model", shortName: "Primary" }],
      ],
      [selectedInstanceId, [{ slug: "gpt-5.6-sol", name: "GPT-5.6 Sol", shortName: "5.6 Sol" }]],
    ]);

    expect(
      resolveContextWindowModelDisplayName(
        {
          instanceId: selectedInstanceId,
          model: "gpt-5.6-sol",
        },
        modelOptionsByInstance,
      ),
    ).toBe("5.6 Sol");
  });

  it("falls back to the selected model slug when model metadata is unavailable", () => {
    const selectedInstanceId = ProviderInstanceId.make("codex-work");

    expect(
      resolveContextWindowModelDisplayName(
        {
          instanceId: selectedInstanceId,
          model: "custom-model",
        },
        new Map(),
      ),
    ).toBe("custom-model");
  });
});

describe("formatContextWindowCompactionMessage", () => {
  it("describes compaction in terms of the selected model", () => {
    expect(formatContextWindowCompactionMessage("GPT-5.6 Sol")).toBe(
      "Context for GPT-5.6 Sol compacts automatically when needed.",
    );
  });

  it("uses neutral copy when the model is unavailable", () => {
    expect(formatContextWindowCompactionMessage(null)).toBe(
      "Context compacts automatically when needed.",
    );
  });
});

describe("resolveContextWindowTokenBreakdown", () => {
  it("uses latest-turn counters and derives cache hit percentage", () => {
    expect(
      resolveContextWindowTokenBreakdown({
        usedTokens: 30_496,
        totalProcessedTokens: 30_670,
        maxTokens: null,
        inputTokens: 30_492,
        cachedInputTokens: 30_214,
        outputTokens: 4,
        reasoningOutputTokens: 0,
        lastUsedTokens: 30_496,
        lastInputTokens: 30_492,
        lastCachedInputTokens: 30_214,
        lastOutputTokens: 4,
        lastReasoningOutputTokens: 0,
        toolUses: null,
        durationMs: null,
        compactsAutomatically: false,
        remainingTokens: null,
        usedPercentage: null,
        remainingPercentage: null,
        updatedAt: "2026-08-24T00:00:00.000Z",
      }),
    ).toEqual({
      inputTokens: 30_492,
      cachedInputTokens: 30_214,
      cachePercentage: (30_214 / 30_492) * 100,
      outputTokens: 4,
      reasoningOutputTokens: 0,
    });
  });
});

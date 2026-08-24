import type { ModelSelection, ProviderInstanceId } from "@t3tools/contracts";
import type { ContextWindowSnapshot } from "~/lib/contextWindow";
import { getTriggerDisplayModelName, type ModelEsque } from "./providerIconUtils";

export function resolveContextWindowModelDisplayName(
  selection: ModelSelection | null | undefined,
  modelOptionsByInstance: ReadonlyMap<ProviderInstanceId, ReadonlyArray<ModelEsque>>,
): string | null {
  if (!selection) {
    return null;
  }

  const selectedModel = modelOptionsByInstance
    .get(selection.instanceId)
    ?.find((model) => model.slug === selection.model);

  return selectedModel ? getTriggerDisplayModelName(selectedModel) : selection.model;
}

export function formatContextWindowCompactionMessage(
  modelDisplayName: string | null | undefined,
): string {
  return modelDisplayName
    ? `Context for ${modelDisplayName} compacts automatically when needed.`
    : "Context compacts automatically when needed.";
}

export type ContextWindowTokenBreakdown = {
  readonly inputTokens: number | null;
  readonly cachedInputTokens: number | null;
  readonly cachePercentage: number | null;
  readonly outputTokens: number | null;
  readonly reasoningOutputTokens: number | null;
};

export function resolveContextWindowTokenBreakdown(
  usage: ContextWindowSnapshot,
): ContextWindowTokenBreakdown {
  const inputTokens = usage.lastInputTokens ?? usage.inputTokens;
  const cachedInputTokens = usage.lastCachedInputTokens ?? usage.cachedInputTokens;
  const outputTokens = usage.lastOutputTokens ?? usage.outputTokens;
  const reasoningOutputTokens = usage.lastReasoningOutputTokens ?? usage.reasoningOutputTokens;
  const cachePercentage =
    inputTokens !== null &&
    inputTokens > 0 &&
    cachedInputTokens !== null &&
    cachedInputTokens >= 0
      ? Math.max(0, Math.min(100, (cachedInputTokens / inputTokens) * 100))
      : null;

  return {
    inputTokens,
    cachedInputTokens,
    cachePercentage,
    outputTokens,
    reasoningOutputTokens,
  };
}

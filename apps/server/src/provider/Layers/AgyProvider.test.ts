import { describe, expect, it } from "@effect/vitest";

import { parseAgyModelsOutput } from "./AgyProvider.ts";

describe("parseAgyModelsOutput", () => {
  it("parses slug and display name columns", () => {
    const models = parseAgyModelsOutput(
      "gemini-3.7-flash-high     Gemini 3.7 Flash (High)\nclaude-sonnet-4-6         Claude Sonnet 4.6 (Thinking)\n",
    );
    expect(models).toHaveLength(2);
    expect(models[0]?.slug).toBe("gemini-3.7-flash-high");
    expect(models[0]?.name).toBe("Gemini 3.7 Flash (High)");
    expect(models[0]?.isCustom).toBe(false);
    expect(models[1]?.slug).toBe("claude-sonnet-4-6");
  });

  it("falls back to the slug as name when the name column is missing", () => {
    const models = parseAgyModelsOutput("bare-slug\n");
    expect(models).toHaveLength(1);
    expect(models[0]?.slug).toBe("bare-slug");
    expect(models[0]?.name).toBe("bare-slug");
  });

  it("ignores blank lines, CRLF, and duplicate slugs", () => {
    const models = parseAgyModelsOutput(
      "\r\ngemini-3.5-flash-medium   Gemini 3.5 Flash (Medium)\r\ngemini-3.5-flash-medium   Again\r\n\r\n",
    );
    expect(models).toHaveLength(1);
    expect(models[0]?.slug).toBe("gemini-3.5-flash-medium");
  });
});

describe("parseAgyModelsOutput returns empty for empty output", () => {
  it("no models discovered yields an empty list without failing", () => {
    expect(parseAgyModelsOutput("")).toHaveLength(0);
    expect(parseAgyModelsOutput("   \n \n")).toHaveLength(0);
  });
});

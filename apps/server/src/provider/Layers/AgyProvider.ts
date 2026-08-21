/**
 * AgyProvider — snapshot + health check for the Antigravity CLI provider.
 *
 * Wraps `agy` headless mode: version probe via `agy --version`, model
 * discovery via `agy models` (two-column output: `<slug>  <name>`).
 *
 * @module provider/Layers/AgyProvider
 */
import {
  type AgySettings,
  type ModelCapabilities,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const AGY_PRESENTATION = {
  displayName: "Antigravity",
  badgeLabel: "Experimental",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: false,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const VERSION_PROBE_TIMEOUT_MS = 4_000;
const AGY_MODELS_DISCOVERY_TIMEOUT_MS = 15_000;

export function buildInitialAgyProviderSnapshot(
  agySettings: AgySettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = agyModelsFromSettings(agySettings.customModels, []);

    if (!agySettings.enabled) {
      return buildServerProvider({
        presentation: AGY_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Antigravity is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: AGY_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Antigravity CLI availability...",
      },
    });
  });
}

function agyModelsFromSettings(
  customModels: ReadonlyArray<string> | undefined,
  builtInModels: ReadonlyArray<ServerProviderModel>,
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(builtInModels, customModels ?? [], EMPTY_CAPABILITIES);
}

/**
 * Parse `agy models` output — one model per line as `<slug>  <name>`,
 * with the name column optional. Example:
 *   gemini-3.7-flash-high     Gemini 3.7 Flash (High)
 */
export function parseAgyModelsOutput(output: string): ReadonlyArray<ServerProviderModel> {
  const models: Array<ServerProviderModel> = [];
  const seen = new Set<string>();
  for (const rawLine of output.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) continue;
    const match = line.match(/^(\S+)\s+(.+)$/);
    const slug = match?.[1] ?? line;
    const name = match?.[2]?.trim() || slug;
    if (seen.has(slug)) continue;
    seen.add(slug);
    models.push({ slug, name, isCustom: false, capabilities: EMPTY_CAPABILITIES });
  }
  return models;
}

const runAgyVersionCommand = (
  agySettings: AgySettings,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const command = agySettings.binaryPath || "agy";
    const spawnCommand = yield* resolveSpawnCommand(command, ["--version"], {
      env: environment,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

const runAgyModelsCommand = (
  agySettings: AgySettings,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const command = agySettings.binaryPath || "agy";
    const spawnCommand = yield* resolveSpawnCommand(command, ["models"], {
      env: environment,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

export const checkAgyProviderStatus = Effect.fn("checkAgyProviderStatus")(function* (
  agySettings: AgySettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = agyModelsFromSettings(agySettings.customModels, []);

  if (!agySettings.enabled) {
    return buildServerProvider({
      presentation: AGY_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Antigravity is disabled in T3 Code settings.",
      },
    });
  }

  const versionResult = yield* runAgyVersionCommand(agySettings, environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* Effect.logWarning("Agy CLI health check failed.", {
      errorTag: error._tag,
    });
    return buildServerProvider({
      presentation: AGY_PRESENTATION,
      enabled: agySettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "Antigravity CLI (`agy`) is not installed or not on PATH."
          : "Failed to execute Antigravity CLI health check.",
      },
    });
  }

  if (Option.isNone(versionResult.success)) {
    return buildServerProvider({
      presentation: AGY_PRESENTATION,
      enabled: agySettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Antigravity CLI is installed but timed out while running `agy --version`.",
      },
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    yield* Effect.logWarning("Agy CLI version probe exited with a non-zero status.", {
      exitCode: versionOutput.code,
      stdoutLength: versionOutput.stdout.length,
      stderrLength: versionOutput.stderr.length,
    });
    return buildServerProvider({
      presentation: AGY_PRESENTATION,
      enabled: agySettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "Antigravity CLI is installed but failed to run.",
      },
    });
  }

  const modelsResult = yield* runAgyModelsCommand(agySettings, environment).pipe(
    Effect.timeoutOption(AGY_MODELS_DISCOVERY_TIMEOUT_MS),
    Effect.result,
  );
  const discoveredModels = Result.isSuccess(modelsResult)
    ? Option.getOrElse(modelsResult.success, () => null)
    : null;
  if (discoveredModels === null) {
    if (Result.isFailure(modelsResult)) {
      yield* Effect.logWarning("Agy model discovery failed.", {
        errorTag: modelsResult.failure._tag,
      });
    }
    return buildServerProvider({
      presentation: AGY_PRESENTATION,
      enabled: agySettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "warning",
        auth: { status: "unknown" },
        message:
          "Antigravity CLI is installed but model discovery failed. Falling back to custom models.",
      },
    });
  }

  const models =
    discoveredModels.code === 0 && discoveredModels.stdout.trim().length > 0
      ? agyModelsFromSettings(
          agySettings.customModels,
          parseAgyModelsOutput(discoveredModels.stdout),
        )
      : fallbackModels;

  return buildServerProvider({
    presentation: AGY_PRESENTATION,
    enabled: agySettings.enabled,
    checkedAt,
    models,
    probe: {
      installed: true,
      version,
      status: "ready",
      auth: { status: "unknown" },
    },
  });
});

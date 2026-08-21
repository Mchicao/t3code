/**
 * AgyTextGeneration — commit/PR/branch/title generation via `agy -p`.
 *
 * Runs the Antigravity CLI headless with `--output-format json` plus
 * `--json-schema`, then decodes the envelope's response through the same
 * schema handed to the CLI.
 *
 * ponytail: the prompt travels as argv (agy headless has no documented
 * stdin prompt path), so very large diffs can exceed the OS command-line
 * limit and fail loudly. Ceiling ~30k chars; upgrade path is a prompt-file
 * flag if agy ever ships one.
 *
 * @module textGeneration/AgyTextGeneration
 */
import { type AgySettings, type ModelSelection, TextGenerationError } from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  normalizeCliError,
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
  toJsonSchemaObject,
} from "./TextGenerationUtils.ts";

const AGY_TIMEOUT_MS = 180_000;
const encodeJsonString = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeJsonString = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));

export const makeAgyTextGeneration = Effect.fn("makeAgyTextGeneration")(function* (
  agySettings: AgySettings,
  environment?: NodeJS.ProcessEnv,
) {
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const resolvedEnvironment = environment ?? process.env;

  const readStreamAsString = <E>(
    operation: string,
    stream: Stream.Stream<Uint8Array, E>,
  ): Effect.Effect<string, TextGenerationError> =>
    stream.pipe(
      Stream.decodeText(),
      Stream.runFold(
        () => "",
        (acc, chunk) => acc + chunk,
      ),
      Effect.mapError((cause) =>
        normalizeCliError("agy", operation, cause, "Failed to collect process output"),
      ),
    );

  const runAgyJson = Effect.fn("runAgyJson")(function* <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchema,
    modelSelection,
  }: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchema: S;
    modelSelection: ModelSelection;
  }): Effect.fn.Return<S["Type"], TextGenerationError, S["DecodingServices"]> {
    const schemaJson = yield* encodeJsonString(toJsonSchemaObject(outputSchema)).pipe(
      Effect.mapError(
        (cause) =>
          new TextGenerationError({
            operation,
            detail: "Failed to encode structured output schema.",
            cause,
          }),
      ),
    );
    const runAndCollect = Effect.fn("runAgyJson.runAndCollect")(function* () {
      const spawnCommand = yield* resolveSpawnCommand(
        agySettings.binaryPath || "agy",
        [
          "-p",
          prompt,
          "--output-format",
          "json",
          "--print-timeout",
          "3m",
          "--json-schema",
          schemaJson,
          ...(modelSelection.model?.trim() ? ["--model", modelSelection.model.trim()] : []),
        ],
        { env: resolvedEnvironment },
      );
      const child = yield* commandSpawner
        .spawn(
          ChildProcess.make(spawnCommand.command, spawnCommand.args, {
            env: resolvedEnvironment,
            cwd,
            shell: spawnCommand.shell,
          }),
        )
        .pipe(
          Effect.mapError((cause) =>
            normalizeCliError("agy", operation, cause, "Failed to spawn Agy CLI process"),
          ),
        );

      return yield* Effect.all(
        [
          readStreamAsString(operation, child.stdout),
          readStreamAsString(operation, child.stderr),
          child.exitCode.pipe(
            Effect.mapError((cause) =>
              normalizeCliError("agy", operation, cause, "Failed to read Agy CLI exit code"),
            ),
          ),
        ],
        { concurrency: "unbounded" },
      );
    });

    const [stdout, stderr, exitCode] = yield* runAndCollect().pipe(
      Effect.scoped,
      Effect.timeoutOption(AGY_TIMEOUT_MS),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              new TextGenerationError({ operation, detail: "Agy CLI request timed out." }),
            ),
          onSome: (value) => Effect.succeed(value),
        }),
      ),
    );
    if (exitCode !== 0) {
      const stderrDetail = stderr.trim();
      const stdoutDetail = stdout.trim();
      const detail = stderrDetail.length > 0 ? stderrDetail : stdoutDetail;
      return yield* new TextGenerationError({
        operation,
        detail:
          detail.length > 0
            ? `Agy CLI command failed: ${detail}`
            : `Agy CLI command failed with code ${exitCode}.`,
      });
    }

    const envelope = yield* readEnvelope(operation, stdout);
    if (envelope.status !== "SUCCESS") {
      return yield* new TextGenerationError({
        operation,
        detail:
          typeof envelope.error === "string" && envelope.error.trim()
            ? envelope.error.trim()
            : `Agy run failed with status ${String(envelope.status)}.`,
      });
    }

    const response =
      typeof envelope.response === "string" && envelope.response.trim()
        ? envelope.response
        : yield* encodeJsonString(envelope.structured_output ?? null).pipe(
            Effect.mapError(
              (cause) =>
                new TextGenerationError({
                  operation,
                  detail: "Failed to encode Agy structured output.",
                  cause,
                }),
            ),
          );
    const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(outputSchema));
    return yield* decodeOutput(response).pipe(
      Effect.catchTags({
        SchemaError: (cause) =>
          Effect.fail(
            new TextGenerationError({
              operation,
              detail: "Agy returned invalid structured output.",
              cause,
            }),
          ),
      }),
    );
  });

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("AgyTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });

      const generated = yield* runAgyJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("AgyTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });

      const generated = yield* runAgyJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("AgyTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });

      const generated = yield* runAgyJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        branch: sanitizeBranchFragment(generated.branch),
      };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("AgyTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });

      const generated = yield* runAgyJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizeThreadTitle(generated.title),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});

type AgyTextGenerationOperation =
  | "generateCommitMessage"
  | "generatePrContent"
  | "generateBranchName"
  | "generateThreadTitle";

/** Decode the agy `--output-format json` envelope, tolerating stray
 * diagnostics around the JSON object on stdout. */
function readEnvelope(
  operation: AgyTextGenerationOperation,
  raw: string,
): Effect.Effect<Record<string, unknown>, TextGenerationError> {
  const trimmed = raw.trim();
  const start = trimmed.indexOf("{");
  const end = trimmed.lastIndexOf("}");
  if (start < 0 || end <= start) {
    return Effect.fail(
      new TextGenerationError({ operation, detail: "Agy returned an unreadable JSON envelope." }),
    );
  }
  return decodeJsonString(trimmed.slice(start, end + 1)).pipe(
    Effect.flatMap((parsed) =>
      typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
        ? Effect.succeed(parsed as Record<string, unknown>)
        : Effect.fail(
            new TextGenerationError({
              operation,
              detail: "Agy returned an unreadable JSON envelope.",
            }),
          ),
    ),
    Effect.catchTag("SchemaError", () =>
      Effect.fail(
        new TextGenerationError({
          operation,
          detail: "Agy returned an unreadable JSON envelope.",
        }),
      ),
    ),
  );
}

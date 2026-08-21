/**
 * AgyAdapter — Antigravity CLI provider adapter over a persistent stream-json
 * session, the documented interface behind Antigravity's queued-message
 * steering in the official IDE extensions.
 *
 * One long-lived `agy --input-format stream-json --output-format stream-json`
 * process per thread; prompts travel as NDJSON `user` events on stdin and the
 * NDJSON event stream (init → step_update* → result per turn) is mapped onto
 * `ProviderRuntimeEvent`s. Conversation continuity comes from the run's
 * `conversation_id`: stored as the session resume cursor, replayed via
 * `--conversation <id>` when the process is respawned (model switch, crash,
 * or interrupt).
 *
 * Steering: agy runs one turn per stdin message and expects the previous
 * turn's `result` before the next prompt, so a sendTurn during an active turn
 * queues the prompt at the adapter level and it continues the same T3 turn —
 * matching how the official extensions queue messages mid-task. True mid-turn
 * injection would require Antigravity's undocumented language-service
 * protocol (`agentapi`), which the CLI does not expose as of 1.1.17.
 *
 * Known ceilings (deliberate):
 *   - No interactive approvals: agy headless soft-denies tools that would
 *     ask, so `respondToRequest`/`respondToUserInput` always fail. Grant
 *     tools via `permissions.allow` in ~/.gemini/antigravity-cli/settings.json.
 *   - No watchdog on hung turns: print-mode's `--print-timeout` does not
 *     apply to streaming sessions; use interruptTurn.
 *
 * @module provider/Layers/AgyAdapter
 */
import {
  type AgySettings,
  EventId,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  type ThreadId,
  ThreadTokenUsageSnapshot,
  TurnId,
} from "@t3tools/contracts";
import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { AgyAdapterShape } from "../Services/AgyAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = ProviderDriverKind.make("agy");
const AGY_RESUME_VERSION = 1 as const;
const decodeThreadTokenUsageSnapshot = Schema.decodeUnknownSync(ThreadTokenUsageSnapshot);

export interface AgyAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly instanceId?: ProviderInstanceId;
}

interface AgyActiveTurn {
  readonly turnId: TurnId;
  /** Steering prompts queued while an agy turn is in flight; each continues
   * the same T3 turn once agy reports the previous result. */
  pendingSteers: Array<string>;
  /** Set by interruptTurn before the kill so settlement picks "cancelled". */
  interrupted: boolean;
  settled: boolean;
  assistantItemId: RuntimeItemId | undefined;
  assistantItemCompleted: boolean;
}

/** The long-lived `agy --input-format stream-json` process backing a thread. */
interface AgySessionProc {
  readonly scope: Scope.Closeable;
  readonly child: ChildProcessSpawner.ChildProcessHandle;
  /** Model the process was spawned with; a model switch respawns it. */
  readonly model: string | undefined;
}

interface AgySessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly cwd: string;
  conversationId: string | undefined;
  sessionProc: AgySessionProc | undefined;
  activeTurn: AgyActiveTurn | undefined;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  stopped: boolean;
}

export function parseAgyResume(raw: unknown): { conversationId: string } | undefined {
  if (typeof raw !== "object" || raw === null) return undefined;
  const record = raw as Record<string, unknown>;
  if (record.schemaVersion !== AGY_RESUME_VERSION) return undefined;
  if (typeof record.conversationId !== "string" || !record.conversationId.trim()) return undefined;
  return { conversationId: record.conversationId.trim() };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function canonicalItemTypeFromAgyToolName(toolName: string | undefined) {
  switch (toolName) {
    case "run_command":
      return "command_execution" as const;
    case "write_to_file":
    case "replace_file_content":
    case "multi_replace_file_content":
      return "file_change" as const;
    case "web_search":
    case "url_context":
      return "web_search" as const;
    default:
      return "dynamic_tool_call" as const;
  }
}

function toolTitleFromInfo(
  toolInfo: Record<string, unknown>,
  toolName: string | undefined,
): string {
  const parameters = isRecord(toolInfo.parameters) ? toolInfo.parameters : undefined;
  const command = typeof parameters?.CommandLine === "string" ? parameters.CommandLine : undefined;
  const targetPath = typeof parameters?.Path === "string" ? parameters.Path : undefined;
  if (command) return command;
  if (targetPath) return targetPath;
  return toolName ?? "tool";
}

/** Map an agy `usage` record onto the canonical token-usage snapshot, or
 * `undefined` when it does not fit (usage is best-effort diagnostics). */
export function usageFromAgy(raw: unknown): ThreadTokenUsageSnapshot | undefined {
  if (!isRecord(raw)) return undefined;
  const pick = (key: string): number | undefined => {
    const value = raw[key];
    return typeof value === "number" && Number.isFinite(value) && value >= 0
      ? Math.round(value)
      : undefined;
  };
  const inputTokens = pick("input_tokens");
  const outputTokens = pick("output_tokens");
  const reasoningOutputTokens = pick("thinking_tokens");
  const cachedInputTokens = pick("cache_read_tokens");
  const candidate = {
    usedTokens: pick("total_tokens") ?? 0,
    ...(inputTokens !== undefined ? { inputTokens } : {}),
    ...(outputTokens !== undefined ? { outputTokens } : {}),
    ...(reasoningOutputTokens !== undefined ? { reasoningOutputTokens } : {}),
    ...(cachedInputTokens !== undefined ? { cachedInputTokens } : {}),
  };
  try {
    return decodeThreadTokenUsageSnapshot(candidate);
  } catch {
    return undefined;
  }
}

/** Serialize a prompt as the NDJSON `user` event agy's stream-json input
 * mode consumes (one turn per line, text blocks only). */
export function agyUserEventLine(prompt: string): string {
  return JSON.stringify({ event: "user", message: { content: prompt } });
}

export function makeAgyAdapter(agySettings: AgySettings, options?: AgyAdapterLiveOptions) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("agy");
    const path = yield* Path.Path;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const crypto = yield* Crypto.Crypto;
    const nativeEventLogger =
      options?.nativeEventLogger ??
      (options?.nativeEventLogPath !== undefined
        ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, { stream: "native" })
        : undefined);

    const sessions = new Map<ThreadId, AgySessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(Effect.orDie);
    const nextEventId = Effect.map(randomUUIDv4, (id) => EventId.make(id));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const logNative = (threadId: ThreadId, method: string, payload: unknown) =>
      Effect.gen(function* () {
        if (!nativeEventLogger) return;
        const observedAt = yield* nowIso;
        yield* nativeEventLogger.write(
          {
            observedAt,
            event: {
              id: EventId.make(yield* randomUUIDv4),
              kind: "notification",
              provider: PROVIDER,
              createdAt: observedAt,
              method,
              threadId,
              payload,
            },
          },
          threadId,
        );
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to write native Agy notification log.", {
            cause,
            threadId,
            method,
          }),
        ),
      );

    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing = current.get(threadId);
        if (existing) {
          return Effect.succeed([existing, current] as const);
        }
        return Semaphore.make(1).pipe(
          Effect.map((semaphore) => {
            const next = new Map(current);
            next.set(threadId, semaphore);
            return [semaphore, next] as const;
          }),
        );
      });

    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<AgySessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
        );
      }
      return Effect.succeed(ctx);
    };

    const resumeCursorFor = (ctx: AgySessionContext) =>
      ctx.conversationId
        ? { schemaVersion: AGY_RESUME_VERSION, conversationId: ctx.conversationId }
        : undefined;

    const settleTurn = (
      ctx: AgySessionContext,
      turn: AgyActiveTurn,
      outcome: {
        readonly state: "completed" | "failed" | "cancelled" | "interrupted";
        readonly errorMessage?: string;
        readonly usage?: unknown;
      },
    ) =>
      Effect.gen(function* () {
        if (turn.settled) return;
        turn.settled = true;
        if (ctx.activeTurn === turn) {
          ctx.activeTurn = undefined;
        }
        const updatedAt = yield* nowIso;
        const { activeTurnId: _activeTurnId, ...sessionWithoutTurn } = ctx.session;
        ctx.session = {
          ...(sessionWithoutTurn as Omit<ProviderSession, "activeTurnId">),
          status: ctx.stopped ? "closed" : "ready",
          updatedAt,
        };
        yield* offerRuntimeEvent({
          type: "turn.completed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: ctx.threadId,
          turnId: turn.turnId,
          payload: {
            state: outcome.state,
            stopReason: null,
            ...(outcome.usage !== undefined ? { usage: outcome.usage } : {}),
            ...(outcome.errorMessage !== undefined ? { errorMessage: outcome.errorMessage } : {}),
          },
        });
      });

    const decodeAgyJsonLine = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));

    const processAgyLine = (ctx: AgySessionContext, line: string) =>
      Effect.gen(function* () {
        const trimmed = line.trim();
        if (!trimmed) return;
        const parsed = yield* decodeAgyJsonLine(trimmed).pipe(
          Effect.catch(() =>
            Effect.logWarning("Agy session emitted a non-JSON stdout line.", {
              threadId: ctx.threadId,
              lineLength: trimmed.length,
            }).pipe(Effect.as(null)),
          ),
        );
        if (parsed === null || !isRecord(parsed)) return;
        yield* logNative(ctx.threadId, "stdout", parsed);

        if (parsed.event === "init") {
          const conversationId =
            typeof parsed.conversation_id === "string" ? parsed.conversation_id.trim() : undefined;
          if (conversationId && !ctx.conversationId) {
            ctx.conversationId = conversationId;
            ctx.session = { ...ctx.session, resumeCursor: resumeCursorFor(ctx) };
          }
          return;
        }

        if (parsed.event !== "step_update" && parsed.event !== "result") return;
        const turn = ctx.activeTurn;
        if (!turn || turn.settled) return;

        if (parsed.event === "result") {
          const result = isRecord(parsed.result) ? parsed.result : undefined;
          const conversationId =
            typeof result?.conversation_id === "string" ? result.conversation_id.trim() : undefined;
          if (conversationId) {
            ctx.conversationId = conversationId;
            ctx.session = { ...ctx.session, resumeCursor: resumeCursorFor(ctx) };
          }
          const usage = usageFromAgy(result?.usage);
          if (usage !== undefined) {
            yield* offerRuntimeEvent({
              type: "thread.token-usage.updated",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              threadId: ctx.threadId,
              turnId: turn.turnId,
              payload: { usage },
            });
          }
          const status = typeof result?.status === "string" ? result.status : "ERROR";
          const errorDetail =
            typeof result?.error === "string" && result.error.trim()
              ? result.error.trim()
              : undefined;

          // A queued steering prompt continues the same T3 turn once agy has
          // finished its current run; a fresh agy turn starts under the same
          // turn id with reset assistant-item state.
          const steeredPrompt = status === "SUCCESS" ? turn.pendingSteers.shift() : undefined;
          if (steeredPrompt !== undefined) {
            const proc = ctx.sessionProc;
            if (proc) {
              turn.assistantItemId = undefined;
              turn.assistantItemCompleted = false;
              yield* writeToAgyStdin(ctx, proc.child, steeredPrompt).pipe(
                Effect.catch((error: ProviderAdapterRequestError) =>
                  Effect.gen(function* () {
                    turn.pendingSteers.length = 0;
                    yield* settleTurn(ctx, turn, {
                      state: "failed",
                      errorMessage: error.detail,
                      usage,
                    });
                  }),
                ),
              );
              return;
            }
            turn.pendingSteers.length = 0;
          }

          if (status === "SUCCESS") {
            yield* settleTurn(ctx, turn, { state: "completed", usage });
          } else if (status === "CANCELED" || status === "INTERRUPTED") {
            yield* settleTurn(ctx, turn, {
              state: status === "CANCELED" ? "cancelled" : "interrupted",
              usage,
            });
          } else if (status === "WAITING" || status === "RUNNING") {
            // Non-terminal result. Streaming sessions should not emit these;
            // leave the turn open for the next event or the process exit.
            yield* Effect.logWarning("Agy session emitted a non-terminal result.", {
              threadId: ctx.threadId,
              status,
            });
          } else {
            if (turn.pendingSteers.length > 0) {
              yield* Effect.logWarning(
                "Dropping steering prompts queued behind a failed Agy turn.",
                { threadId: ctx.threadId, dropped: turn.pendingSteers.length },
              );
              turn.pendingSteers.length = 0;
            }
            yield* settleTurn(ctx, turn, {
              state: "failed",
              errorMessage: errorDetail ?? `Agy run failed with status ${status}.`,
              usage,
            });
          }
          return;
        }

        const step = parsed.step_update;
        if (!isRecord(step)) return;
        const stepType = typeof step.step_type === "string" ? step.step_type : "";
        const state = typeof step.state === "string" ? step.state : "";

        if (stepType === "agent_response") {
          const textDelta = typeof step.text_delta === "string" ? step.text_delta : "";
          if (textDelta.length === 0) return;
          if (turn.assistantItemId === undefined) {
            turn.assistantItemId = RuntimeItemId.make(`agy-assistant-${turn.turnId}`);
            yield* offerRuntimeEvent({
              type: "item.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              threadId: ctx.threadId,
              turnId: turn.turnId,
              itemId: turn.assistantItemId,
              payload: { itemType: "assistant_message", status: "inProgress" },
            });
          }
          yield* offerRuntimeEvent({
            type: "content.delta",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: ctx.threadId,
            turnId: turn.turnId,
            itemId: turn.assistantItemId,
            payload: { streamKind: "assistant_text", delta: textDelta },
          });
          if (state === "DONE" && !turn.assistantItemCompleted) {
            turn.assistantItemCompleted = true;
            yield* offerRuntimeEvent({
              type: "item.completed",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              threadId: ctx.threadId,
              turnId: turn.turnId,
              itemId: turn.assistantItemId,
              payload: { itemType: "assistant_message", status: "completed" },
            });
          }
          return;
        }

        if (stepType === "tool" && state === "DONE" && isRecord(step.tool_info)) {
          const toolInfo = step.tool_info;
          const toolName = typeof toolInfo.name === "string" ? toolInfo.name : undefined;
          const itemType = canonicalItemTypeFromAgyToolName(toolName);
          const stepIndex = typeof step.step_index === "number" ? step.step_index : 0;
          const itemId = RuntimeItemId.make(`agy-tool-${turn.turnId}-${stepIndex}`);
          const failed = isRecord(toolInfo.error);
          const title = toolTitleFromInfo(toolInfo, toolName);
          const data = {
            parameters: toolInfo.parameters ?? null,
            output: typeof toolInfo.output === "string" ? toolInfo.output : null,
          };
          yield* offerRuntimeEvent({
            type: "item.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: ctx.threadId,
            turnId: turn.turnId,
            itemId,
            payload: { itemType, status: "inProgress", title },
          });
          yield* offerRuntimeEvent({
            type: "item.completed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: ctx.threadId,
            turnId: turn.turnId,
            itemId,
            payload: {
              itemType,
              status: failed ? "failed" : "completed",
              title,
              data,
            },
          });
          return;
        }
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logError("Failed to process Agy event.", {
            cause,
            threadId: ctx.threadId,
          }),
        ),
      );

    /** Write one NDJSON user event to the session process's stdin. */
    const writeToAgyStdin = (
      ctx: AgySessionContext,
      child: ChildProcessSpawner.ChildProcessHandle,
      prompt: string,
    ) =>
      Stream.run(Stream.encodeText(Stream.make(`${agyUserEventLine(prompt)}\n`)), child.stdin).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "sendTurn",
              detail: `Failed to write a prompt to the Agy session process stdin: ${cause.message}`,
              cause,
            }),
        ),
      );

    const killProc = (proc: AgySessionProc): Effect.Effect<void> =>
      proc.child
        .kill({ killSignal: "SIGTERM", forceKillAfter: "1 second" })
        .pipe(Effect.catchCause(() => Effect.void));

    const spawnAgySessionProc = (ctx: AgySessionContext, model: string | undefined) =>
      Effect.gen(function* () {
        const procScope = yield* Scope.make("sequential");
        const args = [
          "--input-format",
          "stream-json",
          "--output-format",
          "stream-json",
          ...(model ? ["--model", model] : []),
          ...(ctx.conversationId ? ["--conversation", ctx.conversationId] : []),
          ...tokenizeCliArgs(agySettings.launchArgs),
        ];
        const spawnCommand = yield* resolveSpawnCommand(
          agySettings.binaryPath || "agy",
          args,
          options?.environment ? { env: options.environment } : {},
        );
        const child = yield* childProcessSpawner
          .spawn(
            ChildProcess.make(spawnCommand.command, spawnCommand.args, {
              cwd: ctx.cwd,
              shell: spawnCommand.shell,
              ...(options?.environment ? { env: options.environment } : {}),
            }),
          )
          .pipe(
            Effect.provideService(Scope.Scope, procScope),
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "spawn",
                  detail: `Failed to spawn the Agy session process: ${cause.message}`,
                  cause,
                }),
            ),
          );
        const proc: AgySessionProc = { scope: procScope, child, model };
        ctx.sessionProc = proc;

        const stdoutRemainderRef = yield* Ref.make("");
        yield* child.stdout.pipe(
          Stream.decodeText(),
          Stream.mapEffect((chunk) =>
            Ref.modify(stdoutRemainderRef, (current) => {
              const combined = current + chunk;
              const lines = combined.split("\n");
              const remainder = lines.pop() ?? "";
              return [lines.map((line) => line.replace(/\r$/, "")), remainder] as const;
            }),
          ),
          Stream.flatMap((lines) => Stream.fromIterable(lines)),
          Stream.mapEffect((line) => processAgyLine(ctx, line)),
          Stream.runDrain,
          Effect.catchCause((cause) =>
            Effect.logError("Agy stdout processing failed.", { cause, threadId: ctx.threadId }),
          ),
          Effect.forkIn(procScope),
        );

        // Settle an in-flight turn from the process exit when no terminal
        // result arrived (interrupt kill, crash, model-switch respawn).
        // Detached from procScope so closing the scope cannot interrupt the
        // closer.
        yield* child.exitCode.pipe(
          Effect.flatMap((exitCode) =>
            Effect.gen(function* () {
              if (ctx.sessionProc !== proc) return;
              ctx.sessionProc = undefined;
              const turn = ctx.activeTurn;
              if (!turn || turn.settled || ctx.stopped) return;
              turn.pendingSteers.length = 0;
              yield* settleTurn(ctx, turn, {
                state: turn.interrupted ? "cancelled" : "failed",
                ...(turn.interrupted
                  ? {}
                  : {
                      errorMessage: `The Agy session process exited with code ${exitCode} before producing a result.`,
                    }),
              });
            }),
          ),
          Effect.flatMap(() => Scope.close(procScope, Exit.void)),
          Effect.catchCause((cause) =>
            Effect.logError("Agy process exit handling failed.", {
              cause,
              threadId: ctx.threadId,
            }),
          ),
          Effect.forkChild,
        );

        return proc;
      });

    const ensureAgySessionProc = (ctx: AgySessionContext, model: string | undefined) =>
      Effect.gen(function* () {
        const existing = ctx.sessionProc;
        if (existing) {
          // agy pins the model per process; a switch respawns the idle
          // process and resumes the same conversation via `--conversation`.
          if (existing.model === model) return existing;
          ctx.sessionProc = undefined;
          yield* killProc(existing);
        }
        return yield* spawnAgySessionProc(ctx, model);
      });

    const stopSessionInternal = (ctx: AgySessionContext): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        const activeTurn = ctx.activeTurn;
        if (activeTurn) {
          activeTurn.interrupted = true;
        }
        const proc = ctx.sessionProc;
        if (proc) {
          yield* killProc(proc);
        }
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const startSession: AgyAdapterShape["startSession"] = (input) =>
      Effect.gen(function* () {
        if (input.provider !== undefined && input.provider !== PROVIDER) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
          });
        }
        if (!input.cwd?.trim()) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "cwd is required and must be non-empty.",
          });
        }

        const cwd = path.resolve(input.cwd.trim());
        const existing = sessions.get(input.threadId);
        if (existing && !existing.stopped) {
          yield* stopSessionInternal(existing);
        }

        const modelSelection =
          input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
        const resumeConversationId = parseAgyResume(input.resumeCursor)?.conversationId;
        const now = yield* nowIso;
        const session: ProviderSession = {
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          status: "ready",
          runtimeMode: input.runtimeMode,
          cwd,
          ...(modelSelection?.model ? { model: modelSelection.model } : {}),
          threadId: input.threadId,
          ...(resumeConversationId
            ? {
                resumeCursor: {
                  schemaVersion: AGY_RESUME_VERSION,
                  conversationId: resumeConversationId,
                },
              }
            : {}),
          createdAt: now,
          updatedAt: now,
        };
        const ctx: AgySessionContext = {
          threadId: input.threadId,
          session,
          cwd,
          conversationId: resumeConversationId,
          sessionProc: undefined,
          activeTurn: undefined,
          turns: [],
          stopped: false,
        };
        sessions.set(input.threadId, ctx);

        yield* offerRuntimeEvent({
          type: "session.started",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: input.threadId,
          payload: { resume: resumeConversationId !== undefined },
        });
        yield* offerRuntimeEvent({
          type: "session.state.changed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: input.threadId,
          payload: { state: "ready", reason: "Agy session ready" },
        });
        yield* offerRuntimeEvent({
          type: "thread.started",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: input.threadId,
          payload: resumeConversationId ? { providerThreadId: resumeConversationId } : {},
        });
        return session;
      });

    const beginTurn = (ctx: AgySessionContext, input: ProviderSendTurnInput) =>
      Effect.gen(function* () {
        const prompt = input.input?.trim();
        if (!prompt) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Turn requires non-empty text input.",
          });
        }
        if (input.attachments && input.attachments.length > 0) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Attachments are not supported by the Agy adapter yet.",
          });
        }

        const modelSelection =
          input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
        const model = modelSelection?.model?.trim() || undefined;

        // Steering: agy consumes one turn per stdin message and needs the
        // previous result first, so queue the prompt behind the active turn.
        // It continues the same T3 turn when agy reports its next result —
        // the same queued-message behavior as the official IDE extensions.
        if (ctx.activeTurn && !ctx.activeTurn.settled) {
          const activeTurn = ctx.activeTurn;
          activeTurn.pendingSteers.push(prompt);
          ctx.session = { ...ctx.session, updatedAt: yield* nowIso };
          return {
            threadId: ctx.threadId,
            turnId: activeTurn.turnId,
            resumeCursor: resumeCursorFor(ctx),
          };
        }

        const turnId = TurnId.make(yield* randomUUIDv4);
        const turn: AgyActiveTurn = {
          turnId,
          pendingSteers: [],
          interrupted: false,
          settled: false,
          assistantItemId: undefined,
          assistantItemCompleted: false,
        };
        ctx.activeTurn = turn;

        const proc = yield* ensureAgySessionProc(ctx, model).pipe(
          Effect.tapError(() =>
            Effect.sync(() => {
              ctx.activeTurn = undefined;
            }),
          ),
        );
        yield* writeToAgyStdin(ctx, proc.child, prompt).pipe(
          Effect.tapError(() =>
            Effect.sync(() => {
              ctx.activeTurn = undefined;
            }),
          ),
        );

        ctx.session = {
          ...ctx.session,
          status: "running",
          activeTurnId: turnId,
          updatedAt: yield* nowIso,
          ...(model ? { model } : {}),
        };
        yield* offerRuntimeEvent({
          type: "turn.started",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: ctx.threadId,
          turnId,
          payload: model ? { model } : {},
        });

        ctx.turns.push({ id: turnId, items: [] });
        return { threadId: ctx.threadId, turnId, resumeCursor: resumeCursorFor(ctx) };
      });

    const sendTurn: AgyAdapterShape["sendTurn"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(input.threadId);
          return yield* beginTurn(ctx, input);
        }),
      );

    const interruptTurn: AgyAdapterShape["interruptTurn"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = sessions.get(threadId);
          if (!ctx || ctx.stopped) return;
          const turn = ctx.activeTurn;
          if (!turn || turn.settled) return;
          turn.interrupted = true;
          turn.pendingSteers.length = 0;
          const proc = ctx.sessionProc;
          if (proc) {
            // Killing the process ends the in-flight agy turn; the exit
            // handler settles the T3 turn as cancelled. The next send
            // respawns and resumes via `--conversation`.
            yield* killProc(proc);
          } else {
            yield* settleTurn(ctx, turn, { state: "cancelled" });
          }
        }),
      );

    const stopSession: AgyAdapterShape["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = sessions.get(threadId);
          if (!ctx) return;
          yield* stopSessionInternal(ctx);
        }),
      );

    const listSessions: AgyAdapterShape["listSessions"] = () =>
      Effect.sync(() =>
        Array.from(sessions.values())
          .filter((ctx) => !ctx.stopped)
          .map((ctx) => ctx.session),
      );

    const hasSession: AgyAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const ctx = sessions.get(threadId);
        return ctx !== undefined && !ctx.stopped;
      });

    const readThread: AgyAdapterShape["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return {
          threadId,
          turns: ctx.turns.map((turn) => ({ id: turn.id, items: turn.items })),
        };
      });

    const rollbackThread: AgyAdapterShape["rollbackThread"] = () =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "rollbackThread",
          detail:
            "Rollback is not supported by the Agy adapter; agy conversations are resumed server-side and cannot be rewound from the CLI.",
        }),
      );

    const stopAll: AgyAdapterShape["stopAll"] = () =>
      Effect.forEach(Array.from(sessions.keys()), (threadId) => stopSession(threadId), {
        discard: true,
        concurrency: "unbounded",
      });

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session" },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest: () =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "respondToRequest",
            detail:
              "Interactive approvals are not available over the Agy CLI interface; tools that require approval are soft-denied by the CLI. Grant tools via permissions.allow in ~/.gemini/antigravity-cli/settings.json.",
          }),
        ),
      respondToUserInput: () =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "respondToUserInput",
            detail: "Structured user input is not available over the Agy CLI interface.",
          }),
        ),
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents: Stream.fromPubSub(runtimeEventPubSub),
    } satisfies AgyAdapterShape;
  });
}

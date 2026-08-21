/**
 * AgyAdapter — Antigravity CLI provider adapter over documented headless mode.
 *
 * One `agy -p <prompt> --output-format stream-json` process per turn; the
 * NDJSON event stream (init → step_update* → result) is mapped onto
 * `ProviderRuntimeEvent`s. Conversation continuity across turns comes from
 * the run's `conversation_id`, stored as the session resume cursor and
 * replayed via `--conversation <id>` on the next spawn.
 *
 * Known ceilings (deliberate, headless-mode imposed):
 *   - No steering: a sendTurn while a turn is in flight is rejected. Interrupt
 *     + resend is the workaround. Upgrade path: the harness WebSocket
 *     protocol the Python SDK uses supports mid-turn input.
 *   - No interactive approvals: agy headless soft-denies tools that would
 *     ask, so `respondToRequest`/`respondToUserInput` always fail. Grant
 *     tools via `permissions.allow` in ~/.gemini/antigravity-cli/settings.json.
 *   - Prompts travel as argv, so inputs beyond ~30k chars are rejected
 *     (Windows CreateProcess command-line limit).
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
/** agy receives the prompt as a command-line argument; keep well under the
 * ~32k Windows CreateProcess limit. */
const AGY_MAX_PROMPT_ARG_CHARS = 30_000;
/** agy kills a headless run itself after `--print-timeout`; keep it generous. */
const AGY_PRINT_TIMEOUT = "2h";

export interface AgyAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly instanceId?: ProviderInstanceId;
}

interface AgyActiveTurn {
  readonly turnId: TurnId;
  readonly scope: Scope.Closeable;
  readonly child: ChildProcessSpawner.ChildProcessHandle;
  /** Set by interruptTurn before the kill so settlement picks "cancelled". */
  interrupted: boolean;
  assistantItemId: RuntimeItemId | undefined;
  assistantItemCompleted: boolean;
  settled: boolean;
}

interface AgySessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly cwd: string;
  conversationId: string | undefined;
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

    const processAgyLine = (ctx: AgySessionContext, turn: AgyActiveTurn, line: string) =>
      Effect.gen(function* () {
        const trimmed = line.trim();
        if (!trimmed) return;
        const parsed = yield* decodeAgyJsonLine(trimmed).pipe(
          Effect.catch(() =>
            Effect.logWarning("Agy headless emitted a non-JSON stdout line.", {
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
          if (status === "SUCCESS") {
            yield* settleTurn(ctx, turn, { state: "completed", usage });
          } else if (status === "CANCELED" || status === "INTERRUPTED") {
            yield* settleTurn(ctx, turn, {
              state: status === "CANCELED" ? "cancelled" : "interrupted",
              usage,
            });
          } else if (status === "WAITING" || status === "RUNNING") {
            // Non-terminal result (headless hit its print timeout or is still
            // flushing). The exit-code path settles the turn.
            yield* Effect.logWarning("Agy headless run ended in a non-terminal state.", {
              threadId: ctx.threadId,
              status,
            });
          } else {
            yield* settleTurn(ctx, turn, {
              state: "failed",
              errorMessage: errorDetail ?? `Agy run failed with status ${status}.`,
              usage,
            });
          }
          return;
        }

        if (parsed.event !== "step_update" || !isRecord(parsed.step_update)) return;
        const step = parsed.step_update;
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
          Effect.logError("Failed to process Agy headless event.", {
            cause,
            threadId: ctx.threadId,
          }),
        ),
      );

    const runTurn = (ctx: AgySessionContext, input: ProviderSendTurnInput) =>
      Effect.gen(function* () {
        const prompt = input.input?.trim();
        if (!prompt) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Turn requires non-empty text input.",
          });
        }
        if (prompt.length > AGY_MAX_PROMPT_ARG_CHARS) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: `Prompt of ${prompt.length} characters exceeds the ${AGY_MAX_PROMPT_ARG_CHARS} character limit for Agy headless mode (the prompt travels as a command-line argument).`,
          });
        }
        if (input.attachments && input.attachments.length > 0) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Attachments are not supported by the Agy headless adapter yet.",
          });
        }
        if (ctx.activeTurn) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "sendTurn",
            detail:
              "An Agy turn is already in progress for this thread. Interrupt the active turn before sending a new prompt (steering is not supported by agy headless mode).",
          });
        }

        const modelSelection =
          input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
        const model = modelSelection?.model?.trim() || undefined;

        const turnId = TurnId.make(yield* randomUUIDv4);
        const turnScope = yield* Scope.make("sequential");
        const args = [
          "-p",
          prompt,
          "--output-format",
          "stream-json",
          "--print-timeout",
          AGY_PRINT_TIMEOUT,
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
            Effect.provideService(Scope.Scope, turnScope),
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "spawn",
                  detail: `Failed to spawn Agy headless process: ${cause.message}`,
                  cause,
                }),
            ),
          );

        const turn: AgyActiveTurn = {
          turnId,
          scope: turnScope,
          child,
          interrupted: false,
          assistantItemId: undefined,
          assistantItemCompleted: false,
          settled: false,
        };
        ctx.activeTurn = turn;
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
          Stream.mapEffect((line) => processAgyLine(ctx, turn, line)),
          Stream.runDrain,
          Effect.catchCause((cause) =>
            Effect.logError("Agy stdout processing failed.", { cause, threadId: ctx.threadId }),
          ),
          Effect.forkIn(turnScope),
        );

        // Settle from the process exit when the stream produced no terminal
        // result (hard kill, crash). Detached from turnScope so closing the
        // scope from here cannot interrupt the closer.
        yield* child.exitCode.pipe(
          Effect.flatMap((exitCode) =>
            settleTurn(ctx, turn, {
              state: turn.interrupted ? "cancelled" : "failed",
              ...(turn.interrupted
                ? {}
                : {
                    errorMessage: `Agy headless process exited with code ${exitCode} before producing a result.`,
                  }),
            }),
          ),
          Effect.flatMap(() => Scope.close(turnScope, Exit.void)),
          Effect.catchCause((cause) =>
            Effect.logError("Agy exit handling failed.", { cause, threadId: ctx.threadId }),
          ),
          Effect.forkChild,
        );

        ctx.turns.push({ id: turnId, items: [] });
        return { threadId: ctx.threadId, turnId, resumeCursor: resumeCursorFor(ctx) };
      });

    const killActiveTurnChild = (turn: AgyActiveTurn): Effect.Effect<void> =>
      turn.child
        .kill({ killSignal: "SIGTERM", forceKillAfter: "1 second" })
        .pipe(Effect.catchCause(() => Effect.void));

    const stopSessionInternal = (ctx: AgySessionContext): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        const activeTurn = ctx.activeTurn;
        if (activeTurn) {
          activeTurn.interrupted = true;
          yield* killActiveTurnChild(activeTurn);
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
          payload: { state: "ready", reason: "Agy headless session ready" },
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

    const sendTurn: AgyAdapterShape["sendTurn"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(input.threadId);
          return yield* runTurn(ctx, input);
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
          yield* killActiveTurnChild(turn);
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
            "Rollback is not supported by the Agy headless adapter; agy conversations are resumed server-side and cannot be rewound from the CLI.",
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
              "Interactive approvals are not available in agy headless mode; tools that require approval are soft-denied by the CLI. Grant tools via permissions.allow in ~/.gemini/antigravity-cli/settings.json.",
          }),
        ),
      respondToUserInput: () =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "respondToUserInput",
            detail: "Structured user input is not available in agy headless mode.",
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

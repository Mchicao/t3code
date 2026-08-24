/**
 * ProjectionTurnRepository - Projection repository interface for unified turn state.
 *
 * Owns persistence operations for pending starts, running/completed turn lifecycle,
 * and checkpoint metadata in a single projection table.
 *
 * @module ProjectionTurnRepository
 */
import {
  CheckpointRef,
  IsoDateTime,
  MessageId,
  NonNegativeInt,
  OrchestrationProposedPlanId,
  OrchestrationCheckpointFile,
  OrchestrationCheckpointStatus,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { ProjectionRepositoryError } from "../Errors.ts";

export const ProjectionTurnState = Schema.Literals([
  "pending",
  "running",
  "interrupted",
  "completed",
  "error",
]);
export type ProjectionTurnState = typeof ProjectionTurnState.Type;

export const ProjectionTurn = Schema.Struct({
  threadId: ThreadId,
  turnId: Schema.NullOr(TurnId),
  pendingMessageId: Schema.NullOr(MessageId),
  sourceProposedPlanThreadId: Schema.NullOr(ThreadId),
  sourceProposedPlanId: Schema.NullOr(OrchestrationProposedPlanId),
  assistantMessageId: Schema.NullOr(MessageId),
  state: ProjectionTurnState,
  requestedAt: IsoDateTime,
  startedAt: Schema.NullOr(IsoDateTime),
  completedAt: Schema.NullOr(IsoDateTime),
  checkpointTurnCount: Schema.NullOr(NonNegativeInt),
  checkpointRef: Schema.NullOr(CheckpointRef),
  checkpointStatus: Schema.NullOr(OrchestrationCheckpointStatus),
  checkpointFiles: Schema.Array(OrchestrationCheckpointFile),
});
export type ProjectionTurn = typeof ProjectionTurn.Type;

export const ProjectionTurnById = Schema.Struct({
  threadId: ThreadId,
  turnId: TurnId,
  pendingMessageId: Schema.NullOr(MessageId),
  sourceProposedPlanThreadId: Schema.NullOr(ThreadId),
  sourceProposedPlanId: Schema.NullOr(OrchestrationProposedPlanId),
  assistantMessageId: Schema.NullOr(MessageId),
  state: ProjectionTurnState,
  requestedAt: IsoDateTime,
  startedAt: Schema.NullOr(IsoDateTime),
  completedAt: Schema.NullOr(IsoDateTime),
  checkpointTurnCount: Schema.NullOr(NonNegativeInt),
  checkpointRef: Schema.NullOr(CheckpointRef),
  checkpointStatus: Schema.NullOr(OrchestrationCheckpointStatus),
  checkpointFiles: Schema.Array(OrchestrationCheckpointFile),
});
export type ProjectionTurnById = typeof ProjectionTurnById.Type;

export const ProjectionPendingTurnStart = Schema.Struct({
  threadId: ThreadId,
  messageId: MessageId,
  sourceProposedPlanThreadId: Schema.NullOr(ThreadId),
  sourceProposedPlanId: Schema.NullOr(OrchestrationProposedPlanId),
  requestedAt: IsoDateTime,
});
export type ProjectionPendingTurnStart = typeof ProjectionPendingTurnStart.Type;

export const ListProjectionTurnsByThreadInput = Schema.Struct({
  threadId: ThreadId,
});
export type ListProjectionTurnsByThreadInput = typeof ListProjectionTurnsByThreadInput.Type;

export const GetProjectionTurnByTurnIdInput = Schema.Struct({
  threadId: ThreadId,
  turnId: TurnId,
});
export type GetProjectionTurnByTurnIdInput = typeof GetProjectionTurnByTurnIdInput.Type;

export const GetProjectionPendingTurnStartInput = Schema.Struct({
  threadId: ThreadId,
});
export type GetProjectionPendingTurnStartInput = typeof GetProjectionPendingTurnStartInput.Type;

export const DeleteProjectionPendingTurnStartByMessageIdInput = Schema.Struct({
  threadId: ThreadId,
  messageId: MessageId,
});
export type DeleteProjectionPendingTurnStartByMessageIdInput =
  typeof DeleteProjectionPendingTurnStartByMessageIdInput.Type;

export const DeleteProjectionTurnsByThreadInput = Schema.Struct({
  threadId: ThreadId,
});
export type DeleteProjectionTurnsByThreadInput = typeof DeleteProjectionTurnsByThreadInput.Type;

export const ClearCheckpointTurnConflictInput = Schema.Struct({
  threadId: ThreadId,
  turnId: TurnId,
  checkpointTurnCount: NonNegativeInt,
});
export type ClearCheckpointTurnConflictInput = typeof ClearCheckpointTurnConflictInput.Type;

export interface ProjectionTurnRepositoryShape {
  readonly upsertByTurnId: (
    row: ProjectionTurnById,
  ) => Effect.Effect<void, ProjectionRepositoryError>;

  /** Appends a pending-start placeholder. Pending starts are consumed FIFO. */
  readonly enqueuePendingTurnStart: (
    row: ProjectionPendingTurnStart,
  ) => Effect.Effect<void, ProjectionRepositoryError>;

  /**
   * Compatibility entry point used by the existing projector. It now appends
   * instead of replacing so multiple follow-up prompts can coexist.
   */
  readonly replacePendingTurnStart: (
    row: ProjectionPendingTurnStart,
  ) => Effect.Effect<void, ProjectionRepositoryError>;

  /** Returns the oldest pending-start placeholder for a thread. */
  readonly getPendingTurnStartByThreadId: (
    input: GetProjectionPendingTurnStartInput,
  ) => Effect.Effect<Option.Option<ProjectionPendingTurnStart>, ProjectionRepositoryError>;

  /**
   * Consumes the oldest pending start while the projected session is running;
   * for terminal/non-running sessions it clears every pending start.
   */
  readonly deletePendingTurnStartByThreadId: (
    input: GetProjectionPendingTurnStartInput,
  ) => Effect.Effect<void, ProjectionRepositoryError>;

  readonly deletePendingTurnStartByMessageId: (
    input: DeleteProjectionPendingTurnStartByMessageIdInput,
  ) => Effect.Effect<void, ProjectionRepositoryError>;

  readonly listByThreadId: (
    input: ListProjectionTurnsByThreadInput,
  ) => Effect.Effect<ReadonlyArray<ProjectionTurn>, ProjectionRepositoryError>;

  readonly getByTurnId: (
    input: GetProjectionTurnByTurnIdInput,
  ) => Effect.Effect<Option.Option<ProjectionTurnById>, ProjectionRepositoryError>;

  readonly clearCheckpointTurnConflict: (
    input: ClearCheckpointTurnConflictInput,
  ) => Effect.Effect<void, ProjectionRepositoryError>;

  readonly deleteByThreadId: (
    input: DeleteProjectionTurnsByThreadInput,
  ) => Effect.Effect<void, ProjectionRepositoryError>;
}

export class ProjectionTurnRepository extends Context.Service<
  ProjectionTurnRepository,
  ProjectionTurnRepositoryShape
>()("t3/persistence/Services/ProjectionTurns/ProjectionTurnRepository") {}

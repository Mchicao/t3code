# T3 Remote SWARMS Implementation Handoff

This handoff was placed in the fork at the user's request so an agent on another PC can implement it. It is not an implementation and must not be treated as permission to modify `C:\Proyectos\SWARMS`.

## Confirmed goal

From a T3 client on PC A, submit and control one SWARMS workflow running on PC B through the existing authenticated T3 Connect environment connection.

The v1 API surface is:

1. Start one workflow for one existing environment-local project.
2. Read the current run and task snapshot.
3. Request and observe cancellation.

## Non-goals

- One T3 thread per SWARMS task.
- A second relay or tunnel protocol.
- Terminal RPC as an execution API.
- Client-supplied executable, arbitrary path, process arguments, or credentials.
- Live steering, visual dashboard, automatic WSL discovery, cross-project dispatch, merge coordination, or automatic resume after T3 restart.

## Existing reusable contracts

### T3 fork

- `packages/client-runtime/src/connection/model.ts`: `RelayConnectionTarget`.
- `packages/client-runtime/src/connection/resolver.ts`: authenticated relay preparation through `ManagedRelay` and DPoP authorization.
- `packages/client-runtime/src/connection/supervisor.ts`: environment-scoped RPC ownership and reconnect policy.
- `packages/client-runtime/src/operations/commands.ts`: typed environment command pattern.
- `packages/contracts/src/orchestration.ts`: existing project/thread command schemas and typed validation.
- `packages/contracts/src/rpc.ts`: `WsRpcGroup` and RPC declarations.
- `apps/server/src/ws.ts`: RPC implementation and authorization-aware server handlers.
- `apps/server/src/auth/RpcAuthorization.ts`: per-RPC scope enforcement.
- `apps/server/src/cloud/ManagedEndpointRuntime.ts` and `apps/server/src/provider/acp/AcpSessionRuntime.ts`: existing `ChildProcessSpawner` patterns.
- `docs/internals/remote-swarm-control.md`: accepted architecture decision.

### SWARMS

- `rust/src/main.rs`: public Rust CLI dispatches `run`.
- `rust/src/cli.rs`: `--plan`, `--run-id`, `--workspace-root`, `--router-config`, and concurrency overrides.
- `rust/src/runtime.rs`: scheduler, ACP/CLI execution, state writes, and reports.
- `rust/src/acp.rs`: ACP session launch, prompt, streamed updates, and cancellation.
- `rust/src/telemetry.rs`: task state and report projection.
- `docs/STATE_CONTRACT.md`: `workflow.json`, `tasks/*.json`, `events.jsonl`, results, and report files.
- `docs/RUST_RUNTIME.md`: Rust is the sole public runtime; Python is legacy compatibility tooling.

Do not import the SWARMS Rust crate into T3 for v1. Invoke the configured SWARMS executable and consume its documented state contract.

## Implementation slices

### 1. Contracts

Add a focused contract module, preferably `packages/contracts/src/remoteSwarm.ts`, and export it through the existing contracts entry point.

Define:

- `start` input: `projectId`, bounded opaque `planJson`, and normal command metadata;
- `start` result: generated safe `runId` plus accepted timestamp/status;
- `get` input: `runId`;
- `get` result: run identity, project identity, lifecycle status, task summaries, report summary, and public error state;
- `cancel` input/result with idempotent semantics;
- explicit schemas for malformed/unavailable state and cancellation failure.

Use a separate remote-SWARM namespace. Do not overload `thread.*` commands or expose SWARMS' entire JSON files as an untyped public response.

Add unary RPC methods to `packages/contracts/src/rpc.ts`. Use existing `orchestration:read` and `orchestration:operate` scopes unless implementation evidence shows a separate scope is required.

### 2. Server-side run service

Add a server service under `apps/server/src/remoteSwarm/` that:

- validates the project id and resolves its workspace root on PC B;
- obtains the server-configured SWARMS root, executable, and router configuration;
- writes the plan to a server-owned file;
- launches `swarms-rs run` with fixed arguments equivalent to `--plan <owned-plan> --workspace-root <project-root> --run-id <safe-id> --router-config <configured-router> --force`;
- retains the child handle and ties it to the server lifecycle;
- reads only the documented SWARMS snapshots and reports;
- returns explicit unavailable/error states for missing, malformed, or racing files;
- terminates the owned process tree on cancel and records whether termination was confirmed.

Use `ChildProcessSpawner`, not an ad-hoc `node:child_process` call. The runner executable and root must come from server configuration or a controlled environment variable, never from the RPC payload.

### 3. RPC and client wiring

Wire the service into `apps/server/src/ws.ts`, startup/layer composition, and `RpcAuthorization.ts`. Preserve the existing environment-scoped authorization and `EnvironmentSupervisor` ownership model.

Add a typed client operation following `packages/client-runtime/src/operations/commands.ts`. It must resolve the current environment service at call time and must not construct a raw WebSocket client.

A UI is not required for v1. The caller must be able to target a saved relay environment, receive `runId`, and call `get` or `cancel` against that same environment.

### 4. State projection

Keep SWARMS as the source of truth. Map its durable files into the narrow T3 contract:

- `workflow.json` for run identity, project, and lifecycle metadata;
- `tasks/*.json` for bounded task summaries;
- `report.json` or `report-rs.json` for terminal summary;
- `events.jsonl` only if needed to diagnose lifecycle transitions, never as raw log passthrough.

Do not expose prompts, provider credentials, `.env` data, or raw worker logs in the first snapshot.

## Acceptance tests

- Contract schemas reject missing project ids, empty plans, oversized plans, unsafe run ids, and malformed cancellation payloads.
- Start resolves a project root on the remote environment and never accepts an arbitrary client path.
- A fake child-process layer verifies the fixed argument boundary and proves credentials are not passed to the child.
- Start returns an accepted run id without waiting for workflow completion.
- `get` maps pending, running, completed, failed, malformed, and unavailable SWARMS state deterministically.
- `cancel` is idempotent and does not report success before process termination is observed.
- RPC authorization rejects read/write calls without the appropriate existing scope.
- A relay-target client operation uses the existing environment supervisor and does not open a second tunnel.
- Existing focused orchestration, connection, and auth tests remain green.

## Guardrails

- Do not touch unrelated dirty files in `C:\Proyectos\SWARMS`.
- Do not start a live provider or use production credentials in tests.
- Do not run browsers or GUI automation for this API-first change.
- Do not add a second scheduler, provider adapter registry, or tunnel implementation to T3.
- Do not claim cancellation from a signal dispatch alone; report tree-termination failure explicitly.

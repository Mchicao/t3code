# Project Memory

## 2026-08-21

- The fork must operate as a separate frontend, not as a second T3 server with a second database.
- `apps/web` already supports a remote primary backend through Vite's single-origin proxy when `T3CODE_PORT` points at the backend and `T3CODE_SINGLE_ORIGIN_DEV=1` keeps the client on the frontend origin.
- Added `pnpm dev:front`, implemented by `scripts/frontend-runner.ts`. It runs only `@t3tools/web`, proxies to the configured backend port, defaults to LAN binding, and allowlists local IPv4 interfaces for other PCs and phones.
- The official Nightly backend remains on port `3773`; the fork frontend is on port `5733`; the fork backend on `13773` was stopped. Official userdata remains at `C:\Users\matia\.t3\userdata` and is not shared with the fork.
- Verified HTTP serving, API proxying, LAN access, and a real one-time pairing exchange through `5733`; the resulting session reported `authenticated: true` and cookie name `t3_session_3773`.
- Validation: `pnpm --filter @t3tools/scripts typecheck` and `pnpm --filter @t3tools/web typecheck` pass. The scripts typecheck reports one pre-existing suggestion in `build-desktop-artifact.ts`.

## 2026-08-22

- Confirmed remote-agent v1 boundary: PC A submits one SWARMS workflow to an existing project on PC B through the existing authenticated T3 Connect environment connection; T3 exposes start/get/cancel, while SWARMS owns scheduling, providers, concurrency, ACP/CLI transport, task state, and reports.
- Deliberately rejected one T3 thread per SWARMS task, terminal RPC as an execution API, a second tunnel, and client-supplied paths, executables, process arguments, or credentials.
- Recorded the decision in `docs/internals/remote-swarm-control.md`, added remote workflow terms to `docs/internals/glossary.md`, and prepared the implementation handoff at `C:\Proyectos\output\t3code-remote-swarm-handoff.md`.
- SWARMS is consumed through its configured executable and documented state contract; its unrelated dirty worktree changes must remain untouched.
- Implementation was explicitly not authorized in this phase; the handoff was copied into `docs/internals/remote-swarm-control-handoff.md` for an agent running on another PC.
- A delegated implementation task was cancelled after it had already created partial contract/service files; those files were removed and the final fork diff contains documentation only.

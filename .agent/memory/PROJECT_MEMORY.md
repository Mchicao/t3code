# Project Memory

## 2026-08-21

- The fork must operate as a separate frontend, not as a second T3 server with a second database.
- `apps/web` already supports a remote primary backend through Vite's single-origin proxy when `T3CODE_PORT` points at the backend and `T3CODE_SINGLE_ORIGIN_DEV=1` keeps the client on the frontend origin.
- Added `pnpm dev:front`, implemented by `scripts/frontend-runner.ts`. It runs only `@t3tools/web`, proxies to the configured backend port, defaults to LAN binding, and allowlists local IPv4 interfaces for other PCs and phones.
- The official Nightly backend remains on port `3773`; the fork frontend is on port `5733`; the fork backend on `13773` was stopped. Official userdata remains at `C:\Users\matia\.t3\userdata` and is not shared with the fork.
- Verified HTTP serving, API proxying, LAN access, and a real one-time pairing exchange through `5733`; the resulting session reported `authenticated: true` and cookie name `t3_session_3773`.
- Validation: `pnpm --filter @t3tools/scripts typecheck` and `pnpm --filter @t3tools/web typecheck` pass. The scripts typecheck reports one pre-existing suggestion in `build-desktop-artifact.ts`.

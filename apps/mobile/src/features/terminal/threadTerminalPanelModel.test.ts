import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  buildThreadTerminalAttachInput,
  type ThreadTerminalSubscriptionIdentity,
} from "./threadTerminalPanelModel";

const identity: ThreadTerminalSubscriptionIdentity = {
  environmentId: EnvironmentId.make("env-1"),
  threadId: ThreadId.make("thread-1"),
  terminalId: "default",
  cwd: "/repo",
  worktreePath: "/repo",
};

describe("buildThreadTerminalAttachInput", () => {
  it("preserves identity while updating terminal dimensions", () => {
    const initialAttach = buildThreadTerminalAttachInput(identity, { cols: 80, rows: 24 });
    const resizedAttach = buildThreadTerminalAttachInput(identity, { cols: 132, rows: 40 });
    const { environmentId: _environmentId, ...attachIdentity } = identity;

    expect(initialAttach).not.toEqual(resizedAttach);
    expect(resizedAttach).toMatchObject(attachIdentity);
    expect(resizedAttach).toMatchObject({ cols: 132, rows: 40 });
  });

  it.each([
    ["thread", { threadId: ThreadId.make("thread-2") }],
    ["terminal", { terminalId: "term-2" }],
    ["cwd", { cwd: "/repo/packages/app" }],
    ["worktree", { worktreePath: "/repo/worktrees/feature" }],
  ])("changes when the %s identity changes", (_label, update) => {
    expect(
      buildThreadTerminalAttachInput({ ...identity, ...update }, { cols: 80, rows: 24 }),
    ).not.toEqual(buildThreadTerminalAttachInput(identity, { cols: 80, rows: 24 }));
  });
});

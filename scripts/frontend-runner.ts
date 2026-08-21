// @effect-diagnostics nodeBuiltinImport:off
import { spawn } from "node:child_process";
import { networkInterfaces } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const args = process.argv.slice(2);

function readOption(name: string, fallback: string): string {
  const index = args.indexOf(name);
  if (index === -1) return fallback;
  const value = args[index + 1]?.trim();
  if (!value || value.startsWith("--")) {
    throw new Error(`${name} requires a value.`);
  }
  return value;
}

function readPort(name: string, fallback: string): string {
  const value = readOption(name, fallback);
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error(`${name} must be a valid TCP port.`);
  }
  return String(port);
}

function localNetworkHosts(): ReadonlyArray<string> {
  return Object.values(networkInterfaces())
    .flatMap((entries) => entries ?? [])
    .filter((entry) => entry.family === "IPv4" && !entry.internal)
    .map((entry) => entry.address);
}

const backendPort = readPort("--backend-port", process.env.T3CODE_FRONTEND_BACKEND_PORT ?? "3773");
const webPort = readPort("--port", process.env.T3CODE_FRONTEND_PORT ?? "5733");
const host = readOption("--host", process.env.T3CODE_FRONTEND_HOST ?? "0.0.0.0");
const defaultAllowedHosts = ["localhost", "127.0.0.1", ...localNetworkHosts()].filter(
  (value, index, values) => values.indexOf(value) === index,
);
const allowedHosts = readOption(
  "--allowed-hosts",
  process.env.T3CODE_FRONTEND_ALLOWED_HOSTS ?? defaultAllowedHosts.join(","),
);

const child = spawn("vp", ["run", "--filter=@t3tools/web", "dev"], {
  cwd: repoRoot,
  env: {
    ...process.env,
    PORT: webPort,
    HOST: host,
    T3CODE_PORT: backendPort,
    T3CODE_MODE: "web",
    T3CODE_NO_BROWSER: "1",
    T3CODE_SINGLE_ORIGIN_DEV: "1",
    T3CODE_DEV_ALLOWED_HOSTS: allowedHosts,
  },
  shell: process.platform === "win32",
  stdio: "inherit",
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exitCode = code ?? 1;
});

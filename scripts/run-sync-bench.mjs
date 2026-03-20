import { spawn } from "node:child_process";

import { repoRootFromImportMeta } from "./repo-root.mjs";

const repoRoot = repoRootFromImportMeta(import.meta.url, 1);
const LOCAL_POSTGRES_URL = "postgres://postgres:postgres@127.0.0.1:5432/postgres";

function normalizeTarget(raw) {
  const value = raw.trim();
  if (value === "direct") return "direct";
  if (
    value === "local" ||
    value === "local-server" ||
    value === "local-postgres" ||
    value === "local-postgres-sync-server"
  ) {
    return "local-postgres-sync-server";
  }
  if (
    value === "remote" ||
    value === "remote-server" ||
    value === "remote-sync" ||
    value === "remote-sync-server"
  ) {
    return "remote-sync-server";
  }
  return null;
}

function parseCsv(raw) {
  if (!raw) return [];
  return raw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
}

function extractFlagValue(args, flag) {
  const prefix = `${flag}=`;
  const found = args.find((arg) => arg.startsWith(prefix));
  return found ? found.slice(prefix.length).trim() : undefined;
}

function parseRequestedTargets(args) {
  const forwardedTargets =
    extractFlagValue(args, "--targets") ?? extractFlagValue(args, "--target");
  if (forwardedTargets) {
    const normalized = parseCsv(forwardedTargets)
      .map((value) => normalizeTarget(value))
      .filter(Boolean);
    if (normalized.length > 0) return normalized;
  }

  const positional = args.find((arg) => !arg.startsWith("--"));
  if (positional) {
    const normalized = normalizeTarget(positional);
    if (normalized) return [normalized];
  }

  return ["direct"];
}

function stripPositionalTargetArg(args) {
  let removed = false;
  return args.filter((arg) => {
    if (removed || arg.startsWith("--")) return true;
    const normalized = normalizeTarget(arg);
    if (!normalized) return true;
    removed = true;
    return false;
  });
}

function hasFlag(args, flag) {
  return args.some((arg) => arg.startsWith(`${flag}=`));
}

async function runPnpm(args, env) {
  await new Promise((resolve, reject) => {
    const child = spawn("pnpm", args, {
      cwd: repoRoot,
      env,
      stdio: "inherit",
    });
    child.on("exit", (code, signal) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(
        new Error(
          `pnpm ${args.join(" ")} failed with ${signal ? `signal ${signal}` : `exit code ${code}`}`
        )
      );
    });
    child.on("error", reject);
  });
}

function ensureRemoteUrl(args, env) {
  if (hasFlag(args, "--sync-server-url")) return;
  if (env.TREECRDT_SYNC_SERVER_URL?.trim()) return;
  throw new Error(
    "remote sync bench requires TREECRDT_SYNC_SERVER_URL or --sync-server-url=ws://host/sync (use wss:// for public TLS deployments)"
  );
}

function effectiveEnv(targets, args) {
  const env = { ...process.env };
  if (
    targets.includes("local-postgres-sync-server") &&
    !hasFlag(args, "--postgres-url") &&
    !(env.TREECRDT_POSTGRES_URL && env.TREECRDT_POSTGRES_URL.trim())
  ) {
    env.TREECRDT_POSTGRES_URL = LOCAL_POSTGRES_URL;
  }
  if (targets.includes("remote-sync-server")) {
    ensureRemoteUrl(args, env);
  }
  return env;
}

async function main() {
  const rawArgs = process.argv.slice(2);
  if (rawArgs.includes("--help")) {
    console.log(`Usage:
  pnpm benchmark:sync
  pnpm benchmark:sync:direct -- --workloads=sync-balanced-children-payloads-cold-start --counts=10000,50000 --fanout=10
  pnpm benchmark:sync:local -- --workloads=sync-balanced-children-cold-start --count=10000
  pnpm benchmark:sync:local -- --workloads=sync-balanced-children-payloads-cold-start --count=10000 --first-view
  TREECRDT_SYNC_SERVER_URL=wss://host/sync pnpm benchmark:sync:remote -- --workloads=sync-balanced-children-payloads-cold-start

Notes:
  - local sync benches default TREECRDT_POSTGRES_URL to ${LOCAL_POSTGRES_URL}
  - remote sync benches never hardcode a server URL; pass TREECRDT_SYNC_SERVER_URL or --sync-server-url=...
  - use wss:// for public HTTPS/TLS deployments and ws:// for local/plain HTTP servers
  - use --fanout=20 to model broader trees; default fanout is 10
  - add --first-view to include the immediate local read after sync in the measured duration
  - extra args are forwarded to packages/treecrdt-sqlite-node/scripts/bench-sync.ts`);
    return;
  }

  const targets = parseRequestedTargets(rawArgs);
  const forwardedArgs = stripPositionalTargetArg(rawArgs);
  const env = effectiveEnv(targets, forwardedArgs);

  const buildCommands = [
    ["-C", "packages/treecrdt-benchmark", "run", "build"],
    ["-C", "packages/sync/protocol", "run", "build"],
    ["-C", "packages/sync/material/sqlite", "run", "build"],
    ["-C", "packages/sync/server/postgres-node", "run", "build"],
    ["-C", "packages/treecrdt-sqlite-node", "run", "build"],
  ];
  if (targets.includes("local-postgres-sync-server")) {
    buildCommands.splice(3, 0, [
      "-C",
      "packages/treecrdt-postgres-napi",
      "run",
      "build",
    ]);
  }

  for (const command of buildCommands) {
    await runPnpm(command, env);
  }

  const targetFlagPresent = hasFlag(forwardedArgs, "--targets") || hasFlag(forwardedArgs, "--target");
  const benchArgs = [
    "-C",
    "packages/treecrdt-sqlite-node",
    "exec",
    "tsx",
    "./scripts/bench-sync.ts",
    ...(targetFlagPresent ? [] : [`--targets=${targets.join(",")}`]),
    ...forwardedArgs,
  ];
  await runPnpm(benchArgs, env);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});

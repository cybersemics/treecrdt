import net from "node:net";
import { spawn } from "node:child_process";

const DEFAULT_POSTGRES_URL = "postgres://postgres:postgres@127.0.0.1:5432/postgres";

function canConnect(host, port, timeoutMs = 1000) {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host, port });
    const cleanup = (result) => {
      socket.removeAllListeners();
      socket.destroy();
      resolve(result);
    };
    socket.setTimeout(timeoutMs);
    socket.on("connect", () => cleanup(true));
    socket.on("timeout", () => cleanup(false));
    socket.on("error", () => cleanup(false));
  });
}

if (!process.env.TREECRDT_POSTGRES_URL || process.env.TREECRDT_POSTGRES_URL.trim().length === 0) {
  process.env.TREECRDT_POSTGRES_URL = DEFAULT_POSTGRES_URL;
  console.log(`[sync-server:postgres:local] using default TREECRDT_POSTGRES_URL=${DEFAULT_POSTGRES_URL}`);
}

const postgresUrl = new URL(process.env.TREECRDT_POSTGRES_URL);
const postgresHost = postgresUrl.hostname;
const postgresPort = Number(postgresUrl.port || "5432");

if (!(await canConnect(postgresHost, postgresPort))) {
  console.error(`[sync-server:postgres:local] no Postgres is reachable at ${postgresHost}:${postgresPort}`);
  console.error("[sync-server:postgres:local] start a disposable local database with: pnpm sync-server:postgres:db:start");
  console.error("[sync-server:postgres:local] or point TREECRDT_POSTGRES_URL at an existing Postgres instance");
  process.exit(1);
}

const child = spawn("pnpm", ["--filter", "@treecrdt/sync-server-postgres-node", "dev"], {
  stdio: "inherit",
  env: process.env,
  shell: process.platform === "win32",
});

child.on("error", (error) => {
  console.error(error);
  process.exitCode = 1;
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exitCode = code ?? 1;
});

import { spawnSync } from "node:child_process";
import net from "node:net";

const CONTAINER_NAME = "treecrdt-postgres-dev";
const POSTGRES_HOST = "127.0.0.1";
const POSTGRES_PORT = 5432;
const POSTGRES_IMAGE = "postgres:16";
const POSTGRES_USER = "postgres";
const POSTGRES_PASSWORD = "postgres";
const POSTGRES_DB = "postgres";
const LOCAL_POSTGRES_URL = `postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}`;

function run(command, args, options = {}) {
  return spawnSync(command, args, {
    stdio: "pipe",
    encoding: "utf8",
    ...options,
  });
}

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

async function waitForLocalPostgres(host, port, timeoutMs = 30_000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (await canConnect(host, port, 500)) return true;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  return false;
}

const dockerCheck = run("docker", ["--version"]);
if (dockerCheck.status !== 0) {
  console.error("[sync-server:postgres:db:start] docker is required for the disposable local Postgres helper");
  process.exit(1);
}

if (await canConnect(POSTGRES_HOST, POSTGRES_PORT)) {
  console.log(`[sync-server:postgres:db:start] Postgres is already reachable at ${POSTGRES_HOST}:${POSTGRES_PORT}`);
  console.log(`[sync-server:postgres:db:start] connection string: ${LOCAL_POSTGRES_URL}`);
  process.exit(0);
}

const inspect = run("docker", ["inspect", "-f", "{{.State.Running}}", CONTAINER_NAME]);
if (inspect.status === 0) {
  const running = inspect.stdout.trim() === "true";
  const result = run("docker", running ? ["restart", CONTAINER_NAME] : ["start", CONTAINER_NAME], { stdio: "inherit" });
  if (result.status !== 0) process.exit(result.status ?? 1);
} else {
  const result = run(
    "docker",
    [
      "run",
      "--name",
      CONTAINER_NAME,
      "-e",
      `POSTGRES_USER=${POSTGRES_USER}`,
      "-e",
      `POSTGRES_PASSWORD=${POSTGRES_PASSWORD}`,
      "-e",
      `POSTGRES_DB=${POSTGRES_DB}`,
      "-p",
      `${POSTGRES_PORT}:5432`,
      "-d",
      POSTGRES_IMAGE,
    ],
    { stdio: "inherit" }
  );
  if (result.status !== 0) process.exit(result.status ?? 1);
}

if (!(await waitForLocalPostgres(POSTGRES_HOST, POSTGRES_PORT))) {
  console.error(`[sync-server:postgres:db:start] Postgres did not become ready at ${POSTGRES_HOST}:${POSTGRES_PORT}`);
  process.exit(1);
}

console.log(`[sync-server:postgres:db:start] local Postgres is ready at ${POSTGRES_HOST}:${POSTGRES_PORT}`);
console.log(`[sync-server:postgres:db:start] connection string: ${LOCAL_POSTGRES_URL}`);
console.log(`[sync-server:postgres:db:start] stop it later with: pnpm sync-server:postgres:db:stop`);

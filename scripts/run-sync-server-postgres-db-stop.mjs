import { spawnSync } from "node:child_process";

const CONTAINER_NAME = "treecrdt-postgres-dev";

const result = spawnSync("docker", ["rm", "-f", CONTAINER_NAME], {
  stdio: "pipe",
  encoding: "utf8",
});

if (result.status !== 0) {
  const stderr = result.stderr.trim();
  if (stderr.includes("No such container")) {
    console.log(`[sync-server:postgres:db:stop] container ${CONTAINER_NAME} is not running`);
    process.exit(0);
  }
  process.stderr.write(result.stderr);
  process.exit(result.status ?? 1);
}

console.log(`[sync-server:postgres:db:stop] removed ${CONTAINER_NAME}`);

#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "../../..");

const source = path.join(repoRoot, "vendor/wa-sqlite/src/examples/OPFSCoopSyncVFS.js");
const targets = [
  path.join(repoRoot, "packages/treecrdt-wa-sqlite/src/vendor/OPFSCoopSyncVFS.js"),
  path.join(repoRoot, "packages/treecrdt-wa-sqlite/dist/vendor/OPFSCoopSyncVFS.js"),
];

async function copyFile(from, to) {
  await fs.mkdir(path.dirname(to), { recursive: true });
  await fs.copyFile(from, to);
}

async function main() {
  try {
    await Promise.all(targets.map((t) => copyFile(source, t)));
    console.info(`[copy-opfs-vfs] copied OPFSCoopSyncVFS.js to ${targets.length} target(s)`);
  } catch (err) {
    console.error("[copy-opfs-vfs] failed to copy OPFSCoopSyncVFS.js", err);
    process.exit(1);
  }
}

await main();

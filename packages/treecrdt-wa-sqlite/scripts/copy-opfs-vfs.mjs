#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "../../..");

const sources = [
  {
    src: path.join(repoRoot, "vendor/wa-sqlite/src/examples/OPFSCoopSyncVFS.js"),
    name: "OPFSCoopSyncVFS.js",
    transform: (content) =>
      content
        .replace("../FacadeVFS.js", "./FacadeVFS.js")
        .replace("../VFS.js", "./VFS.js"),
  },
  {
    src: path.join(repoRoot, "vendor/wa-sqlite/src/FacadeVFS.js"),
    name: "FacadeVFS.js",
  },
  {
    src: path.join(repoRoot, "vendor/wa-sqlite/src/VFS.js"),
    name: "VFS.js",
  },
  {
    src: path.join(repoRoot, "vendor/wa-sqlite/src/sqlite-constants.js"),
    name: "sqlite-constants.js",
  },
];

const targetDirs = [
  path.join(repoRoot, "packages/treecrdt-wa-sqlite/src/vendor"),
  path.join(repoRoot, "packages/treecrdt-wa-sqlite/dist/vendor"),
];

async function copyFile(from, to, transform) {
  await fs.mkdir(path.dirname(to), { recursive: true });
  const content = await fs.readFile(from, "utf8");
  const data = transform ? transform(content) : content;
  await fs.writeFile(to, data, "utf8");
}

async function main() {
  try {
    // Clean target vendor folders so stale copies are never reused.
    for (const dir of targetDirs) {
      await fs.rm(dir, { recursive: true, force: true });
    }

    for (const { src, name, transform } of sources) {
      await Promise.all(
        targetDirs.map((dir) => copyFile(src, path.join(dir, name), transform)),
      );
    }
    console.info(
      `[copy-opfs-vfs] copied ${sources.length} files to ${targetDirs.length} target directories`,
    );
  } catch (err) {
    console.error("[copy-opfs-vfs] failed to copy OPFS VFS artifacts", err);
    process.exit(1);
  }
}

await main();

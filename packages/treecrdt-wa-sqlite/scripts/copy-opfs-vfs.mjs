#!/usr/bin/env node
import fs from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { repoRootFromImportMeta } from "../../../scripts/repo-root.mjs";

const repoRoot = repoRootFromImportMeta(import.meta.url, 3);

const vendorRoot = (() => {
  try {
    const require = createRequire(import.meta.url);
    const pkgJson = require.resolve("@treecrdt/wa-sqlite-vendor/package.json");
    return path.join(path.dirname(pkgJson), "wa-sqlite");
  } catch {
    return path.join(repoRoot, "packages/treecrdt-wa-sqlite-vendor/wa-sqlite");
  }
})();

const sources = [
  {
    src: path.join(vendorRoot, "src/examples/OPFSCoopSyncVFS.js"),
    name: "OPFSCoopSyncVFS.js",
    transform: (content) =>
      content
        .replace("../FacadeVFS.js", "./FacadeVFS.js")
        .replace("../VFS.js", "./VFS.js"),
  },
  {
    src: path.join(vendorRoot, "src/FacadeVFS.js"),
    name: "FacadeVFS.js",
  },
  {
    src: path.join(vendorRoot, "src/VFS.js"),
    name: "VFS.js",
  },
  {
    src: path.join(vendorRoot, "src/sqlite-constants.js"),
    name: "sqlite-constants.js",
  },
];

const targetDirs = [
  path.join(repoRoot, "packages/treecrdt-wa-sqlite/src/vendor"),
  path.join(repoRoot, "packages/treecrdt-wa-sqlite/dist/vendor"),
];

async function copyFile(from, to, transform) {
  const content = await fs.readFile(from, "utf8");
  const data = transform ? transform(content) : content;

  try {
    const existing = await fs.readFile(to, "utf8");
    if (existing === data) return false;
  } catch {
    // fall through: file doesn't exist or isn't readable
  }

  await fs.mkdir(path.dirname(to), { recursive: true });
  await fs.writeFile(to, data, "utf8");
  return true;
}

async function main() {
  try {
    let changed = 0;
    for (const { src, name, transform } of sources) {
      const results = await Promise.all(
        targetDirs.map((dir) =>
          copyFile(src, path.join(dir, name), transform),
        ),
      );
      changed += results.filter(Boolean).length;
    }
    console.info(
      `[copy-opfs-vfs] synced ${sources.length} files to ${targetDirs.length} dirs (${changed} changed)`,
    );
  } catch (err) {
    console.error("[copy-opfs-vfs] failed to copy OPFS VFS artifacts", err);
    process.exit(1);
  }
}

await main();

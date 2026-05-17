#!/usr/bin/env node
import fs from 'node:fs/promises';
import { createRequire } from 'node:module';
import path from 'node:path';
import { repoRootFromImportMeta } from '../../../scripts/repo-root.mjs';

const repoRoot = repoRootFromImportMeta(import.meta.url, 3);
const sourcesOnly = process.argv.includes('--sources-only');
const assetsOnly = process.argv.includes('--assets-only');

if (sourcesOnly && assetsOnly) {
  console.error('[copy-opfs-vfs] --sources-only and --assets-only are mutually exclusive');
  process.exit(1);
}

const vendorRoot = (() => {
  try {
    const require = createRequire(import.meta.url);
    const pkgJson = require.resolve('@treecrdt/wa-sqlite-vendor/package.json');
    return path.join(path.dirname(pkgJson), 'wa-sqlite');
  } catch {
    return path.join(repoRoot, 'packages/treecrdt-wa-sqlite-vendor/wa-sqlite');
  }
})();
const vendorDistRoot = (() => {
  try {
    const require = createRequire(import.meta.url);
    const pkgJson = require.resolve('@treecrdt/wa-sqlite-vendor/package.json');
    return path.join(path.dirname(pkgJson), 'dist');
  } catch {
    return path.join(repoRoot, 'packages/treecrdt-wa-sqlite-vendor/dist');
  }
})();

const sources = [
  {
    src: path.join(vendorRoot, 'src/examples/OPFSCoopSyncVFS.js'),
    name: 'OPFSCoopSyncVFS.js',
    transform: (content) =>
      content.replace('../FacadeVFS.js', './FacadeVFS.js').replace('../VFS.js', './VFS.js'),
  },
  {
    src: path.join(vendorRoot, 'src/examples/OPFSAnyContextVFS.js'),
    name: 'OPFSAnyContextVFS.js',
    transform: (content) =>
      content
        .replace('../FacadeVFS.js', './FacadeVFS.js')
        .replace('../VFS.js', './VFS.js')
        .replace('../WebLocksMixin.js', './WebLocksMixin.js'),
  },
  {
    src: path.join(vendorRoot, 'src/FacadeVFS.js'),
    name: 'FacadeVFS.js',
  },
  {
    src: path.join(vendorRoot, 'src/VFS.js'),
    name: 'VFS.js',
  },
  {
    src: path.join(vendorRoot, 'src/WebLocksMixin.js'),
    name: 'WebLocksMixin.js',
  },
  {
    src: path.join(vendorRoot, 'src/sqlite-constants.js'),
    name: 'sqlite-constants.js',
  },
];
const assetSources = [
  {
    src: path.join(vendorDistRoot, 'wa-sqlite.mjs'),
    name: 'wa-sqlite.mjs',
    binary: true,
  },
  {
    src: path.join(vendorDistRoot, 'wa-sqlite.wasm'),
    name: 'wa-sqlite.wasm',
    binary: true,
  },
  {
    src: path.join(vendorDistRoot, 'wa-sqlite-async.mjs'),
    name: 'wa-sqlite-async.mjs',
    binary: true,
  },
  {
    src: path.join(vendorDistRoot, 'wa-sqlite-async.wasm'),
    name: 'wa-sqlite-async.wasm',
    binary: true,
  },
  {
    src: path.join(vendorRoot, 'src/sqlite-api.js'),
    name: 'sqlite-api.js',
  },
  {
    src: path.join(vendorRoot, 'src/sqlite-constants.js'),
    name: 'sqlite-constants.js',
  },
];

const targetDirs = [
  path.join(repoRoot, 'packages/treecrdt-wa-sqlite/src/vendor'),
  path.join(repoRoot, 'packages/treecrdt-wa-sqlite/dist/vendor'),
];
const assetTargetDir = path.join(repoRoot, 'packages/treecrdt-wa-sqlite/dist/wa-sqlite');

async function copyFile(from, to, transform) {
  const content = await fs.readFile(from, 'utf8');
  const data = transform ? transform(content) : content;

  try {
    const existing = await fs.readFile(to, 'utf8');
    if (existing === data) return false;
  } catch {
    // fall through: file doesn't exist or isn't readable
  }

  await fs.mkdir(path.dirname(to), { recursive: true });
  await fs.writeFile(to, data, 'utf8');
  return true;
}

async function copyBinaryFile(from, to) {
  try {
    const [fromStat, toStat] = await Promise.all([fs.stat(from), fs.stat(to)]);
    if (fromStat.size === toStat.size && fromStat.mtimeMs <= toStat.mtimeMs) return false;
  } catch {
    // destination missing or stat failed; copy below
  }

  await fs.mkdir(path.dirname(to), { recursive: true });
  await fs.copyFile(from, to);
  return true;
}

async function main() {
  try {
    let changed = 0;
    if (!assetsOnly) {
      for (const { src, name, transform } of sources) {
        const results = await Promise.all(
          targetDirs.map((dir) => copyFile(src, path.join(dir, name), transform)),
        );
        changed += results.filter(Boolean).length;
      }
    }
    if (!sourcesOnly) {
      for (const { src, name, transform, binary } of assetSources) {
        const to = path.join(assetTargetDir, name);
        changed += binary ? await copyBinaryFile(src, to) : await copyFile(src, to, transform);
      }
    }
    const copiedSources = assetsOnly ? 0 : sources.length;
    const copiedAssets = sourcesOnly ? 0 : assetSources.length;
    const copiedTargetDirs = assetsOnly ? 0 : targetDirs.length;
    console.info(
      `[copy-opfs-vfs] synced ${copiedSources} OPFS files to ${copiedTargetDirs} dirs and ${copiedAssets} wa-sqlite assets (${changed} changed)`,
    );
  } catch (err) {
    console.error('[copy-opfs-vfs] failed to copy OPFS VFS artifacts', err);
    process.exit(1);
  }
}

await main();

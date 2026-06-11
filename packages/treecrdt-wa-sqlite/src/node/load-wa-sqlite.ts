import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import type { LoadWaSqliteResult } from '../load-wa-sqlite.browser.js';

function vendorRoots(): { dist: string; waSqlite: string } | null {
  try {
    const require = createRequire(import.meta.url);
    const pkgJson = require.resolve('@treecrdt/wa-sqlite-vendor/package.json');
    const root = path.dirname(pkgJson);
    return { dist: path.join(root, 'dist'), waSqlite: path.join(root, 'wa-sqlite') };
  } catch {
    return null;
  }
}

function resolveAssetsDir(assetsDir?: string): string {
  if (assetsDir) {
    return assetsDir.startsWith('file://') ? fileURLToPath(assetsDir) : assetsDir;
  }

  const packaged = path.join(path.dirname(fileURLToPath(import.meta.url)), '../wa-sqlite');
  if (fs.existsSync(path.join(packaged, 'wa-sqlite-async.wasm'))) {
    return packaged;
  }

  const vendor = vendorRoots();
  if (vendor && fs.existsSync(path.join(vendor.dist, 'wa-sqlite-async.wasm'))) {
    return vendor.dist;
  }

  throw new Error(
    'wa-sqlite assets not found; run `pnpm --filter @treecrdt/wa-sqlite build` or install @treecrdt/wa-sqlite-vendor',
  );
}

function resolveSqliteApiPath(dir: string): string {
  const packagedApi = path.join(dir, 'sqlite-api.js');
  if (fs.existsSync(packagedApi)) return packagedApi;

  const vendor = vendorRoots();
  if (vendor) {
    const vendorApi = path.join(vendor.waSqlite, 'src', 'sqlite-api.js');
    if (fs.existsSync(vendorApi)) return vendorApi;
  }

  throw new Error(`sqlite-api.js not found for wa-sqlite assets in ${dir}`);
}

export async function loadWaSqliteNode(assetsDir?: string): Promise<LoadWaSqliteResult> {
  const dir = resolveAssetsDir(assetsDir);
  const wasmPath = path.join(dir, 'wa-sqlite-async.wasm');
  const mjsPath = path.join(dir, 'wa-sqlite-async.mjs');
  const apiPath = resolveSqliteApiPath(dir);

  const wasmBinary = fs.readFileSync(wasmPath);
  const mod = await import(/* @vite-ignore */ pathToFileURL(mjsPath).href);
  const sqliteApi = await import(/* @vite-ignore */ pathToFileURL(apiPath).href);
  const module = await mod.default({
    wasmBinary,
    locateFile: (f: string) => (f.endsWith('.wasm') ? wasmPath : f),
  });
  return { sqlite3: sqliteApi.Factory(module), module };
}

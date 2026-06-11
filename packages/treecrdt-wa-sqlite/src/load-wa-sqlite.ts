import { isNode } from './platform.js';

export type LoadWaSqliteOptions = {
  /** Browser: public URL prefix. Node: optional filesystem directory containing wa-sqlite assets. */
  assetsDir?: string;
};

export type LoadWaSqliteResult = {
  sqlite3: any;
  module: any;
};

async function loadWaSqliteBrowser(baseUrl: string): Promise<LoadWaSqliteResult> {
  const normalized = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;
  const sqliteModule = await import(/* @vite-ignore */ `${normalized}wa-sqlite/wa-sqlite-async.mjs`);
  const sqliteApi = await import(/* @vite-ignore */ `${normalized}wa-sqlite/sqlite-api.js`);
  const module = await sqliteModule.default({
    locateFile: (file: string) =>
      file.endsWith('.wasm') ? `${normalized}wa-sqlite/wa-sqlite-async.wasm` : file,
  });
  return { sqlite3: sqliteApi.Factory(module), module };
}

async function loadWaSqliteNode(assetsDir?: string): Promise<LoadWaSqliteResult> {
  const fs = await import('node:fs');
  const path = await import('node:path');
  const { pathToFileURL, fileURLToPath } = await import('node:url');
  const { createRequire } = await import('node:module');

  const vendorRoots = (): { dist: string; waSqlite: string } | null => {
    try {
      const require = createRequire(import.meta.url);
      const pkgJson = require.resolve('@treecrdt/wa-sqlite-vendor/package.json');
      const root = path.dirname(pkgJson);
      return { dist: path.join(root, 'dist'), waSqlite: path.join(root, 'wa-sqlite') };
    } catch {
      return null;
    }
  };

  const resolveAssetsDir = (): string => {
    if (assetsDir) {
      return assetsDir.startsWith('file://') ? fileURLToPath(assetsDir) : assetsDir;
    }

    const packaged = path.join(path.dirname(fileURLToPath(import.meta.url)), 'wa-sqlite');
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
  };

  const resolveSqliteApiPath = (dir: string): string => {
    const packagedApi = path.join(dir, 'sqlite-api.js');
    if (fs.existsSync(packagedApi)) return packagedApi;

    const vendor = vendorRoots();
    if (vendor) {
      const vendorApi = path.join(vendor.waSqlite, 'src', 'sqlite-api.js');
      if (fs.existsSync(vendorApi)) return vendorApi;
    }

    throw new Error(`sqlite-api.js not found for wa-sqlite assets in ${dir}`);
  };

  const dir = resolveAssetsDir();
  const wasmPath = path.join(dir, 'wa-sqlite-async.wasm');
  const mjsPath = path.join(dir, 'wa-sqlite-async.mjs');
  const apiPath = resolveSqliteApiPath(dir);

  const wasmBinary = fs.readFileSync(wasmPath);
  const mod = await import(pathToFileURL(mjsPath).href);
  const sqliteApi = await import(pathToFileURL(apiPath).href);
  const module = await mod.default({
    wasmBinary,
    locateFile: (f: string) => (f.endsWith('.wasm') ? wasmPath : f),
  });
  return { sqlite3: sqliteApi.Factory(module), module };
}

function defaultBrowserBaseUrl(): string {
  const env = (import.meta as unknown as { env?: { BASE_URL?: string } }).env;
  return env?.BASE_URL ?? '/';
}

export async function loadWaSqlite(opts: LoadWaSqliteOptions = {}): Promise<LoadWaSqliteResult> {
  if (isNode()) {
    return loadWaSqliteNode(opts.assetsDir);
  }
  const baseUrl = opts.assetsDir ?? defaultBrowserBaseUrl();
  return loadWaSqliteBrowser(baseUrl);
}

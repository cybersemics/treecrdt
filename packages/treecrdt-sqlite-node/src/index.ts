import path from "node:path";
import { fileURLToPath } from "node:url";
import { createTreecrdtSqliteAdapter, type SqliteRunner } from "@treecrdt/interface/sqlite";
import type { TreecrdtAdapter } from "@treecrdt/interface";

export type LoadOptions = {
  extensionPath?: string;
  entrypoint?: string;
};

// Minimal shape we need from better-sqlite3 Database.
export type LoadableDatabase = {
  loadExtension: (path: string, entryPoint?: string) => void;
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function platformExt(): ".dylib" | ".so" | ".dll" {
  switch (process.platform) {
    case "darwin":
      return ".dylib";
    case "win32":
      return ".dll";
    default:
      return ".so";
  }
}

/**
 * Resolve the bundled TreeCRDT SQLite extension for this platform.
 * Falls back to the `native/` directory within this package.
 */
export function defaultExtensionPath(): string {
  const ext = platformExt();
  const base =
    ext === ".dll" ? "treecrdt_sqlite_ext" : "libtreecrdt_sqlite_ext";
  return path.resolve(__dirname, "../native", `${base}${ext}`);
}

/**
 * Load the TreeCRDT SQLite extension into a better-sqlite3 Database.
 */
export function loadTreecrdtExtension(
  db: LoadableDatabase,
  opts: LoadOptions = {}
): string {
  const path = opts.extensionPath ?? defaultExtensionPath();
  const entrypoint = opts.entrypoint ?? "sqlite3_treecrdt_init";
  db.loadExtension(path, entrypoint);
  return path;
}

export function createSqliteNodeAdapter(db: any): TreecrdtAdapter {
  const stmtCache = new Map<string, any>();
  const prepare = (sql: string) => {
    const cached = stmtCache.get(sql);
    if (cached) return cached;
    const stmt = db.prepare(sql);
    stmtCache.set(sql, stmt);
    return stmt;
  };

  const toBindings = (params: unknown[]) =>
    params.reduce<Record<number, unknown>>((acc, val, idx) => {
      acc[idx + 1] = val;
      return acc;
    }, {});

  const runner: SqliteRunner = {
    exec: (sql) => db.exec(sql),
    getText: (sql, params = []) => {
      const row = prepare(sql).get(toBindings(params));
      if (row === undefined || row === null) return null;
      const val = Object.values(row as Record<string, unknown>)[0];
      if (val === undefined || val === null) return null;
      return String(val);
    },
  };

  return createTreecrdtSqliteAdapter(runner);
}

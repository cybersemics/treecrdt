import path from "node:path";
import { fileURLToPath } from "node:url";

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

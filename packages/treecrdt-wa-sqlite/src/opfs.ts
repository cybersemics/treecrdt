import type { Database } from "./index.js";

export type OpfsSupport = {
  available: boolean;
  reason?: string;
};

/**
 * Feature check for OPFS + sync access handles + cross-origin isolation.
 */
export function detectOpfsSupport(): OpfsSupport {
  const hasWindow = typeof window !== "undefined";
  if (!hasWindow) return { available: false, reason: "No window" };
  const hasSyncHandle = "FileSystemHandle" in window && "FileSystemSyncAccessHandle" in window;
  const hasOpfs = typeof navigator?.storage?.getDirectory === "function";
  const isolated = (window as typeof window & { crossOriginIsolated?: boolean }).crossOriginIsolated === true;
  const ok = hasSyncHandle && hasOpfs && isolated;
  return ok
    ? { available: true }
    : {
        available: false,
        reason: !hasOpfs
          ? "navigator.storage.getDirectory unavailable"
          : !hasSyncHandle
            ? "Sync access handles unsupported"
            : "cross-origin isolation required",
      };
}

export type OpfsVfsOptions = {
  name?: string;
};

/**
 * Create the OPFS cooperative sync VFS bound to the provided wa-sqlite Module.
 * Uses a local copy of wa-sqlite's OPFSCoopSyncVFS example to avoid reaching into vendor paths.
 */
export async function createOpfsVfs(module: any, opts: OpfsVfsOptions = {}): Promise<any> {
  const name = opts.name ?? "opfs";
  // @ts-ignore vendored module lacks type declarations
  const { OPFSCoopSyncVFS } = await import("./vendor/OPFSCoopSyncVFS.js");
  return OPFSCoopSyncVFS.create(name, module);
}

export type OpenOptions = {
  moduleFactory: () => Promise<any>;
  filename?: string;
  storage: "memory" | "opfs";
  sqliteApi: { Factory: (module: any) => any };
};

/**
 * Convenience: open a wa-sqlite handle with CRDT extension ready, using OPFS when requested.
 */
export async function openWithStorage(opts: OpenOptions): Promise<{ db: Database; close?: () => Promise<void> }> {
  const { moduleFactory, filename = ":memory:", sqliteApi, storage } = opts;
  let module = await moduleFactory();
  const sqlite3 = sqliteApi.Factory(module);

  let file = filename;
  if (storage === "opfs") {
    const support = detectOpfsSupport();
    if (!support.available) {
      throw new Error(`OPFS unsupported: ${support.reason ?? "unknown reason"}`);
    }
    const vfs = await createOpfsVfs(module, { name: "opfs" });
    sqlite3.vfs_register(vfs, true);
    file = filename === ":memory:" ? "/treecrdt.db" : filename;
  }

  const handle = await sqlite3.open_v2(file);
  const db = makeDbAdapter(sqlite3, handle);
  return {
    db,
    close: async () => {
      try {
        await sqlite3.close(handle);
      } catch {
        /* ignore */
      }
    },
  };
}

function makeDbAdapter(sqlite3: any, handle: number): Database {
  const prepare = async (sql: string) => {
    const iter = sqlite3.statements(handle, sql, { unscoped: true });
    const { value } = await iter.next();
    if (!value) {
      throw new Error(`Failed to prepare statement: ${sql}`);
    }
    return value;
  };

  return {
    prepare,
    bind: async (stmt: number, index: number, value: unknown) => sqlite3.bind(stmt, index, value),
    step: async (stmt: number) => sqlite3.step(stmt),
    column_text: async (stmt: number, index: number) => sqlite3.column_text(stmt, index),
    finalize: async (stmt: number) => sqlite3.finalize(stmt),
    exec: async (sql: string) => sqlite3.exec(handle, sql),
    close: async () => sqlite3.close(handle),
  } as unknown as Database;
}

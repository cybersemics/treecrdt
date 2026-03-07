import type { Database } from "./index.js";
import { makeDbAdapter } from "./db.js";

export type OpfsSupport = {
  available: boolean;
  reason?: string;
};

/**
 * Feature check for OPFS + cross-origin isolation.
 * We require getDirectory + crossOriginIsolated. createSyncAccessHandle is only
 * available in Web Workers, so we cannot reliably detect it from the main thread;
 * the OPFS VFS runs in a worker and will fail at init if unsupported (we fall
 * back to memory).
 */
export function detectOpfsSupport(): OpfsSupport {
  const hasWindow = typeof window !== "undefined";
  if (!hasWindow) return { available: false, reason: "No window" };
  const hasOpfs = typeof navigator?.storage?.getDirectory === "function";
  const isolated = (window as typeof window & { crossOriginIsolated?: boolean }).crossOriginIsolated === true;
  const ok = hasOpfs && isolated;
  return ok
    ? { available: true }
    : {
        available: false,
        reason: !hasOpfs
          ? "navigator.storage.getDirectory unavailable"
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

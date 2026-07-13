import type { Database } from './types.js';
import { makeDbAdapter } from './db.js';
import { initializeTreecrdtExtension } from './extension.js';

export type OpfsSupport = {
  available: boolean;
  reason?: string;
};

function hasOpfsGetDirectory(): boolean {
  return typeof navigator !== 'undefined' && typeof navigator.storage?.getDirectory === 'function';
}

/**
 * Feature check for OPFS + cross-origin isolation.
 * We require getDirectory + crossOriginIsolated. createSyncAccessHandle is only
 * available in Web Workers, so we cannot reliably detect it from the main thread;
 * the OPFS VFS runs in a worker and will fail at init if unsupported (we fall
 * back to memory).
 */
export function detectOpfsSupport(): OpfsSupport {
  const hasWindow = typeof window !== 'undefined';
  if (!hasWindow) return { available: false, reason: 'No window' };
  const hasOpfs = hasOpfsGetDirectory();
  const isolated =
    (window as typeof window & { crossOriginIsolated?: boolean }).crossOriginIsolated === true;
  const ok = hasOpfs && isolated;
  return ok
    ? { available: true }
    : {
        available: false,
        reason: !hasOpfs
          ? 'navigator.storage.getDirectory unavailable'
          : 'cross-origin isolation required',
      };
}

const DB_RELATED_FILE_SUFFIXES = ['', '-journal', '-wal', '-wa0', '-wa1'];

export type ClearOpfsStorageOptions = {
  vfsKind?: OpfsVfsKind;
  vfsName?: string;
};

export async function accessHandlePoolVfsNameForFilename(filename: string): Promise<string> {
  const normalized = new URL(filename, 'file://localhost/').pathname;
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(normalized));
  const suffix = Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');
  return `opfs-ahp-${suffix}`;
}

async function clearAccessHandlePoolStorage(vfsName: string): Promise<void> {
  try {
    const root = await navigator.storage.getDirectory();
    await root.removeEntry(vfsName, { recursive: true });
  } catch (err) {
    if ((err as Error)?.name !== 'NotFoundError') throw err;
  }
}

/**
 * Remove database and related files (journal, wal) from OPFS storage.
 * Call only after the database handle is closed.
 *
 * @param filename - Path used when opening (e.g. /treecrdt.db or /treecrdt-playground.db)
 */
export async function clearOpfsStorage(
  filename: string,
  opts: ClearOpfsStorageOptions = {},
): Promise<void> {
  if (!hasOpfsGetDirectory()) return;
  if (opts.vfsKind === 'access-handle-pool') {
    await clearAccessHandlePoolStorage(
      opts.vfsName ?? (await accessHandlePoolVfsNameForFilename(filename)),
    );
    return;
  }

  const path = filename.startsWith('/') ? filename.slice(1) : filename;
  const parts = path.split('/').filter(Boolean);
  const base = parts.pop() ?? 'treecrdt.db';
  const dirPath = parts;

  let dirHandle: FileSystemDirectoryHandle;
  try {
    const root = await navigator.storage.getDirectory();
    if (dirPath.length === 0) {
      dirHandle = root;
    } else {
      let current = root;
      for (const segment of dirPath) {
        current = await current.getDirectoryHandle(segment);
      }
      dirHandle = current;
    }
  } catch {
    return;
  }

  for (const suffix of DB_RELATED_FILE_SUFFIXES) {
    const name = base + suffix;
    try {
      await dirHandle.removeEntry(name);
    } catch (e) {
      if ((e as Error)?.name !== 'NotFoundError') throw e;
    }
  }
}

/**
 * Check whether any DB-related files exist in OPFS for the given filename.
 *
 * @param filename - Path used when opening (e.g. /treecrdt.db or /drop-test-xxx.db)
 */
export async function opfsStorageExists(filename: string): Promise<boolean> {
  if (!hasOpfsGetDirectory()) return false;

  const path = filename.startsWith('/') ? filename.slice(1) : filename;
  const parts = path.split('/').filter(Boolean);
  const base = parts.pop() ?? 'treecrdt.db';
  const dirPath = parts;

  let dirHandle: FileSystemDirectoryHandle;
  try {
    const root = await navigator.storage.getDirectory();
    if (dirPath.length === 0) {
      dirHandle = root;
    } else {
      let current = root;
      for (const segment of dirPath) {
        current = await current.getDirectoryHandle(segment);
      }
      dirHandle = current;
    }
  } catch {
    return false;
  }

  for (const suffix of DB_RELATED_FILE_SUFFIXES) {
    const name = base + suffix;
    try {
      await dirHandle.getFileHandle(name);
      return true;
    } catch (e) {
      if ((e as Error)?.name !== 'NotFoundError') throw e;
    }
  }
  return false;
}

export type OpfsVfsKind = 'coop-sync' | 'any-context' | 'access-handle-pool' | 'write-ahead';

export type OpfsVfsOptions = {
  name?: string;
  kind?: OpfsVfsKind;
};

/**
 * Create an OPFS VFS bound to the provided wa-sqlite Module.
 * Uses local copies of wa-sqlite's example VFS implementations to avoid reaching into vendor paths.
 */
export async function createOpfsVfs(module: any, opts: OpfsVfsOptions = {}): Promise<any> {
  const name = opts.name ?? 'opfs';
  if (opts.kind === 'access-handle-pool') {
    // @ts-ignore vendored module lacks type declarations
    const { AccessHandlePoolVFS } = await import('./vendor/AccessHandlePoolVFS.js');
    return AccessHandlePoolVFS.create(name, module);
  }

  if (opts.kind === 'write-ahead') {
    // @ts-ignore vendored module lacks type declarations
    const { OPFSWriteAheadVFS } = await import('./vendor/OPFSWriteAheadVFS.js');
    return OPFSWriteAheadVFS.create(name, module);
  }

  if (opts.kind === 'any-context') {
    // @ts-ignore vendored module lacks type declarations
    const { OPFSAnyContextVFS } = await import('./vendor/OPFSAnyContextVFS.js');
    return OPFSAnyContextVFS.create(name, module, { lockPolicy: 'exclusive' });
  }

  // @ts-ignore vendored module lacks type declarations
  const { OPFSCoopSyncVFS } = await import('./vendor/OPFSCoopSyncVFS.js');
  return OPFSCoopSyncVFS.create(name, module);
}

export type OpenOptions = {
  moduleFactory: () => Promise<any>;
  filename?: string;
  storage: 'memory' | 'opfs';
  sqliteApi: { Factory: (module: any) => any };
  opfsVfs?: OpfsVfsKind;
};

/**
 * Convenience: open a wa-sqlite handle with CRDT extension ready, using OPFS when requested.
 */
export async function openWithStorage(
  opts: OpenOptions,
): Promise<{ db: Database; close?: () => Promise<void> }> {
  const { moduleFactory, filename = ':memory:', sqliteApi, storage } = opts;
  const module = await moduleFactory();
  const sqlite3 = sqliteApi.Factory(module);

  let file = filename;
  let vfs: { close?: () => Promise<void> | void } | undefined;
  let vfsName: string | undefined;
  let handle: number | undefined;
  try {
    if (storage === 'opfs') {
      const support = detectOpfsSupport();
      if (!support.available) {
        throw new Error(`OPFS unsupported: ${support.reason ?? 'unknown reason'}`);
      }
      vfsName = 'opfs';
      vfs = await createOpfsVfs(module, { name: vfsName, kind: opts.opfsVfs });
      sqlite3.vfs_register(vfs, false);
      file = filename === ':memory:' ? '/treecrdt.db' : filename;
    }

    const openedHandle = vfsName
      ? await sqlite3.open_v2(file, undefined, vfsName)
      : await sqlite3.open_v2(file);
    handle = openedHandle;
    const db = makeDbAdapter(sqlite3, openedHandle);
    await initializeTreecrdtExtension(module, openedHandle);
    let closePromise: Promise<void> | undefined;
    return {
      db,
      close: () =>
        (closePromise ??= (async () => {
          try {
            await db.close?.();
          } finally {
            await vfs?.close?.();
          }
        })()),
    };
  } catch (err) {
    try {
      if (handle !== undefined) await sqlite3.close(handle);
    } catch {
      // Preserve the initialization error.
    }
    try {
      await vfs?.close?.();
    } catch {
      // Preserve the initialization error.
    }
    throw err;
  }
}

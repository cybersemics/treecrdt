import { createOpfsVfs, type OpfsVfsKind } from './opfs.js';
import { createWaSqliteApi } from './adapter.js';
import type { Database } from './types.js';
import { makeDbAdapter } from './db.js';
import type { TreecrdtAdapter } from '@treecrdt/interface';
import type { MaterializationEvent } from '@treecrdt/interface/engine';
import { initializeTreecrdtExtension } from './extension.js';

export type OpenTreecrdtDbOptions = {
  baseUrl?: string;
  filename?: string;
  storage: 'memory' | 'opfs';
  docId: string;
  requireOpfs?: boolean;
  onMaterialized?: (event: MaterializationEvent) => void;
  opfsVfs?: OpfsVfsKind;
};

export type OpenTreecrdtDbResult = {
  db: Database;
  api: TreecrdtAdapter;
  storage: 'memory' | 'opfs';
  filename: string;
  opfsError?: string;
};

const OPFS_VFS_NAME = 'opfs';

async function closeIgnoringErrors(close: (() => Promise<void> | void) | undefined): Promise<void> {
  if (!close) return;
  try {
    await close();
  } catch {
    // Preserve the initialization error.
  }
}

function closeDatabaseWithVfs(db: Database, vfs: { close?: () => Promise<void> | void }): Database {
  if (!vfs.close) return db;
  let closePromise: Promise<void> | undefined;
  return {
    ...db,
    close: () =>
      (closePromise ??= (async () => {
        try {
          await db.close?.();
        } finally {
          await vfs.close?.();
        }
      })()),
  };
}

export async function openTreecrdtDbFromLoaded(
  opts: OpenTreecrdtDbOptions,
  loaded: { sqlite3: any; module: any },
): Promise<OpenTreecrdtDbResult> {
  const { sqlite3, module } = loaded;

  let storage: 'memory' | 'opfs' = opts.storage === 'opfs' ? 'opfs' : 'memory';
  let opfsError: string | undefined;
  let vfs: { close?: () => Promise<void> | void } | undefined;

  if (storage === 'opfs') {
    try {
      vfs = await createOpfsVfs(module, { name: OPFS_VFS_NAME, kind: opts.opfsVfs });
      sqlite3.vfs_register(vfs, false);
    } catch (err) {
      opfsError = err instanceof Error ? err.message : String(err);
      await closeIgnoringErrors(vfs?.close ? () => vfs!.close!() : undefined);
      vfs = undefined;
      if (opts.requireOpfs) {
        throw new Error(`OPFS requested but could not be initialized: ${opfsError}`);
      }
      storage = 'memory';
    }
  }

  const filename = storage === 'opfs' ? (opts.filename ?? '/treecrdt.db') : ':memory:';
  let db: Database | undefined;
  try {
    const handle =
      storage === 'opfs'
        ? await sqlite3.open_v2(filename, undefined, OPFS_VFS_NAME)
        : await sqlite3.open_v2(filename);
    db = makeDbAdapter(sqlite3, handle);
    await initializeTreecrdtExtension(module, handle);
    const api = createWaSqliteApi(db, { onMaterialized: opts.onMaterialized });
    await api.setDocId(opts.docId);
    const resultDb = vfs ? closeDatabaseWithVfs(db, vfs) : db;

    return opfsError
      ? { db: resultDb, api, storage, filename, opfsError }
      : { db: resultDb, api, storage, filename };
  } catch (err) {
    await closeIgnoringErrors(db?.close ? () => db!.close!() : undefined);
    await closeIgnoringErrors(vfs?.close ? () => vfs!.close!() : undefined);
    throw err;
  }
}

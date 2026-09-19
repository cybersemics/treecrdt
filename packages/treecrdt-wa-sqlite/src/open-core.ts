import { createOpfsVfs, type OpfsVfsKind } from './opfs.js';
import { createDatabase } from './db.js';
import type { Database } from './types.js';
import { initializeTreecrdtExtension } from './extension.js';

export type OpenTreecrdtDbOptions = {
  baseUrl?: string;
  filename?: string;
  storage: 'memory' | 'opfs';
  docId: string;
  opfsVfs?: OpfsVfsKind;
};

export type OpenTreecrdtDbResult = {
  db: Database;
  storage: 'memory' | 'opfs';
  filename: string;
};

const OPFS_VFS_NAME = 'opfs';

/** Open handle, register extension, and pin docId — no TreecrdtAdapter here. */
async function initializeOpenedDatabase(
  sqlite3: any,
  module: any,
  handle: number,
  opts: OpenTreecrdtDbOptions,
): Promise<Database> {
  let db: Database | undefined;
  try {
    db = createDatabase(sqlite3, handle);
    await initializeTreecrdtExtension(module, handle);
    // Doc id must be set before CRDT ops; session owns the TreecrdtAdapter separately.
    await db.getText('SELECT treecrdt_set_doc_id(?1)', [opts.docId]);
    return db;
  } catch (error) {
    try {
      if (db?.close) await db.close();
      else await sqlite3.close(handle);
    } catch {
      // Preserve the initialization error.
    }
    throw error;
  }
}

type OpenedOpfsHandle = {
  handle: number;
  vfs: { close?: () => Promise<void> | void };
};

async function openOpfsHandle(
  sqlite3: any,
  module: any,
  filename: string,
  kind: OpfsVfsKind | undefined,
): Promise<OpenedOpfsHandle> {
  let vfs: OpenedOpfsHandle['vfs'] | undefined;
  try {
    const initializedVfs = await createOpfsVfs(module, { name: OPFS_VFS_NAME, kind });
    vfs = initializedVfs;
    // Keep SQLite's default VFS unchanged; open_v2 selects OPFS explicitly by name.
    sqlite3.vfs_register(initializedVfs, false);
    const handle = await sqlite3.open_v2(filename, undefined, OPFS_VFS_NAME);
    return { handle, vfs: initializedVfs };
  } catch (error) {
    try {
      await vfs?.close?.();
    } catch {
      // Preserve the VFS or database-open error.
    }
    throw error;
  }
}

function closeDatabaseWithVfs(db: Database, vfs: { close?: () => Promise<void> | void }): Database {
  if (!vfs.close) return db;
  let closePromise: Promise<void> | null = null;
  return {
    ...db,
    close: () => {
      closePromise ??= (async () => {
        try {
          await db.close?.();
        } finally {
          await vfs.close?.();
        }
      })();
      return closePromise;
    },
  };
}

export async function openTreecrdtDbWithLoader(
  opts: OpenTreecrdtDbOptions,
  load: () => Promise<{ sqlite3: any; module: any }>,
): Promise<OpenTreecrdtDbResult> {
  const { sqlite3, module } = await load();

  if (opts.storage === 'opfs') {
    const filename = opts.filename ?? '/treecrdt.db';
    let openedOpfs: OpenedOpfsHandle;
    try {
      openedOpfs = await openOpfsHandle(sqlite3, module, filename, opts.opfsVfs);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      const openError = new Error(
        `OPFS requested but could not be initialized: ${reason}`,
      ) as Error & { cause?: unknown };
      openError.cause = error;
      throw openError;
    }

    const { handle, vfs } = openedOpfs;
    try {
      const db = await initializeOpenedDatabase(sqlite3, module, handle, opts);
      return { db: closeDatabaseWithVfs(db, vfs), storage: 'opfs', filename };
    } catch (error) {
      try {
        await vfs.close?.();
      } catch {
        // Preserve the database initialization error.
      }
      throw error;
    }
  }

  const handle = await sqlite3.open_v2(':memory:');
  const db = await initializeOpenedDatabase(sqlite3, module, handle, opts);
  return { db, storage: 'memory', filename: ':memory:' };
}

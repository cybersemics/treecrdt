import { createOpfsVfs, type OpfsVfsKind } from './opfs.js';
import { createDatabase } from './db.js';
import type { Database } from './types.js';
import { initializeTreecrdtExtension } from './extension.js';

export type OpenTreecrdtDbOptions = {
  baseUrl?: string;
  filename?: string;
  storage: 'memory' | 'opfs';
  docId: string;
  requireOpfs?: boolean;
  opfsVfs?: OpfsVfsKind;
};

export type OpenTreecrdtDbResult = {
  db: Database;
  storage: 'memory' | 'opfs';
  filename: string;
  opfsError?: string;
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
  loadMemoryFallback: () => Promise<{ sqlite3: any; module: any }> = load,
): Promise<OpenTreecrdtDbResult> {
  const loaded = await load();
  const { sqlite3, module } = loaded;
  let opfsError: string | undefined;
  const requestedFilename = opts.filename ?? '/treecrdt.db';

  if (opts.storage === 'opfs') {
    let openedOpfs: OpenedOpfsHandle | undefined;
    try {
      openedOpfs = await openOpfsHandle(sqlite3, module, requestedFilename, opts.opfsVfs);
    } catch (error) {
      opfsError = error instanceof Error ? error.message : String(error);
      if (opts.requireOpfs) {
        const requiredError = new Error(
          `OPFS requested but could not be initialized: ${opfsError}`,
        ) as Error & { cause?: unknown };
        requiredError.cause = error;
        throw requiredError;
      }
    }

    if (openedOpfs) {
      const { handle, vfs } = openedOpfs;
      try {
        const db = await initializeOpenedDatabase(sqlite3, module, handle, opts);
        return {
          db: closeDatabaseWithVfs(db, vfs),
          storage: 'opfs',
          filename: requestedFilename,
        };
      } catch (error) {
        try {
          await vfs.close?.();
        } catch {
          // Preserve the database initialization error.
        }
        throw error;
      }
    }
  }

  // A failed OPFS attempt leaves its registered VFS and callback state on the module even after
  // the VFS is closed. Isolate the memory fallback in a fresh module instead of reusing that state.
  try {
    const memoryLoaded = opfsError !== undefined ? await loadMemoryFallback() : loaded;
    const handle = await memoryLoaded.sqlite3.open_v2(':memory:');
    const db = await initializeOpenedDatabase(
      memoryLoaded.sqlite3,
      memoryLoaded.module,
      handle,
      opts,
    );
    const result = { db, storage: 'memory' as const, filename: ':memory:' };
    return opfsError !== undefined ? { ...result, opfsError } : result;
  } catch (fallbackFailure) {
    if (opfsError === undefined) throw fallbackFailure;
    const fallbackError =
      fallbackFailure instanceof Error ? fallbackFailure.message : String(fallbackFailure);
    throw new Error(
      `OPFS initialization failed: ${opfsError}; memory fallback failed: ${fallbackError}`,
    );
  }
}

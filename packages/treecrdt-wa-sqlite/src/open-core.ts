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

type LoadedWaSqlite = { sqlite3: any; module: any };
type WaSqliteLoader = () => Promise<LoadedWaSqlite>;

const OPFS_VFS_NAME = 'opfs';

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function memoryFallbackError(opfsFailure: unknown, fallbackFailure: unknown): Error {
  const error = new Error(
    `OPFS initialization failed: ${errorMessage(opfsFailure)}; memory fallback failed: ${errorMessage(fallbackFailure)}`,
  ) as Error & { cause?: unknown; opfsCause?: unknown };
  error.cause = fallbackFailure;
  error.opfsCause = opfsFailure;
  return error;
}

async function closeIgnoringErrors(close: (() => Promise<void> | void) | undefined): Promise<void> {
  if (!close) return;
  try {
    await close();
  } catch {
    // Preserve the error that made initialization fail.
  }
}

async function initializeOpenedDatabase(
  sqlite3: any,
  module: any,
  handle: number,
  opts: OpenTreecrdtDbOptions,
): Promise<{ db: Database; api: TreecrdtAdapter }> {
  let db: Database | undefined;
  try {
    db = makeDbAdapter(sqlite3, handle);
    await initializeTreecrdtExtension(module, handle);
    const api = createWaSqliteApi(db, { onMaterialized: opts.onMaterialized });
    await api.setDocId(opts.docId);
    return { db, api };
  } catch (err) {
    await closeIgnoringErrors(db?.close ? () => db!.close!() : () => sqlite3.close(handle));
    throw err;
  }
}

async function openInitializedDatabase(
  sqlite3: any,
  module: any,
  filename: string,
  opts: OpenTreecrdtDbOptions,
): Promise<{ db: Database; api: TreecrdtAdapter }> {
  const handle = await sqlite3.open_v2(filename);
  return initializeOpenedDatabase(sqlite3, module, handle, opts);
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
  } catch (err) {
    await closeIgnoringErrors(vfs?.close ? () => vfs!.close!() : undefined);
    throw err;
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
          await vfs.close!();
        }
      })();
      return closePromise;
    },
  };
}

export async function openTreecrdtDbWithLoader(
  opts: OpenTreecrdtDbOptions,
  load: WaSqliteLoader,
): Promise<OpenTreecrdtDbResult> {
  const loaded = await load();
  const { sqlite3, module } = loaded;
  let opfsError: string | undefined;
  let opfsFailure: unknown;
  const requestedFilename = opts.filename ?? '/treecrdt.db';

  if (opts.storage === 'opfs') {
    let openedOpfs: OpenedOpfsHandle | undefined;
    try {
      openedOpfs = await openOpfsHandle(sqlite3, module, requestedFilename, opts.opfsVfs);
    } catch (err) {
      opfsFailure = err;
      opfsError = errorMessage(err);
      if (opts.requireOpfs) {
        const requiredError = new Error(
          `OPFS requested but could not be initialized: ${opfsError}`,
        ) as Error & { cause?: unknown };
        requiredError.cause = err;
        throw requiredError;
      }
    }

    if (openedOpfs) {
      const { handle, vfs } = openedOpfs;
      try {
        const opened = await initializeOpenedDatabase(sqlite3, module, handle, opts);
        return {
          ...opened,
          db: closeDatabaseWithVfs(opened.db, vfs),
          storage: 'opfs',
          filename: requestedFilename,
        };
      } catch (err) {
        await closeIgnoringErrors(vfs.close ? () => vfs.close!() : undefined);
        throw err;
      }
    }
  }

  // A failed OPFS attempt leaves its registered VFS and callback state on the module even after
  // the VFS is closed. Isolate the memory fallback in a fresh module instead of reusing that state.
  try {
    const memoryLoaded = opfsError !== undefined ? await load() : loaded;
    const opened = await openInitializedDatabase(
      memoryLoaded.sqlite3,
      memoryLoaded.module,
      ':memory:',
      opts,
    );
    const result = { ...opened, storage: 'memory' as const, filename: ':memory:' };
    return opfsError !== undefined ? { ...result, opfsError } : result;
  } catch (fallbackFailure) {
    if (opfsError === undefined) throw fallbackFailure;
    throw memoryFallbackError(opfsFailure, fallbackFailure);
  }
}

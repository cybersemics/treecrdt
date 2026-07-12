import { accessHandlePoolVfsNameForFilename, createOpfsVfs, type OpfsVfsKind } from './opfs.js';
import { createWaSqliteApi } from './adapter.js';
import type { Database, OpfsWriteMode } from './types.js';
import { makeDbAdapter } from './db.js';
import { dbGetText } from './sql.js';
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
  opfsWriteMode?: OpfsWriteMode;
};

export type OpenTreecrdtDbResult = {
  db: Database;
  api: TreecrdtAdapter;
  storage: 'memory' | 'opfs';
  filename: string;
  opfsVfsKind?: OpfsVfsKind;
  opfsVfsName?: string;
  opfsError?: string;
};

export type LoadedWaSqlite = { sqlite3: any; module: any };
export type LoadFreshWaSqlite = () => Promise<LoadedWaSqlite>;

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

async function openInitializedDatabase(
  sqlite3: any,
  module: any,
  filename: string,
  opts: OpenTreecrdtDbOptions,
  vfsName?: string,
  beforeExtensionInit?: (db: Database) => Promise<void>,
): Promise<{ db: Database; api: TreecrdtAdapter }> {
  let db: Database | undefined;
  try {
    const handle = vfsName
      ? await sqlite3.open_v2(filename, undefined, vfsName)
      : await sqlite3.open_v2(filename);
    db = makeDbAdapter(sqlite3, handle);
    await beforeExtensionInit?.(db);
    await initializeTreecrdtExtension(module, handle);
    const api = createWaSqliteApi(db, { onMaterialized: opts.onMaterialized });
    await api.setDocId(opts.docId);
    return { db, api };
  } catch (err) {
    await closeIgnoringErrors(db?.close ? () => db!.close!() : undefined);
    throw err;
  }
}

async function configureOpfsWriteMode(db: Database, mode: OpfsWriteMode): Promise<void> {
  if (mode === 'default') return;

  const lockingMode = await dbGetText(db, 'PRAGMA locking_mode=EXCLUSIVE');
  const currentJournalMode = await dbGetText(db, 'PRAGMA journal_mode');
  const journalMode =
    currentJournalMode?.toLowerCase() === 'wal'
      ? currentJournalMode
      : await dbGetText(db, 'PRAGMA journal_mode=WAL');
  const confirmedJournalMode = await dbGetText(db, 'PRAGMA journal_mode');
  const confirmedLockingMode = await dbGetText(db, 'PRAGMA locking_mode');

  if (
    confirmedJournalMode?.toLowerCase() !== 'wal' ||
    confirmedLockingMode?.toLowerCase() !== 'exclusive'
  ) {
    throw new Error(
      `OPFS single-owner WAL requested but SQLite reported journal_mode=${confirmedJournalMode ?? journalMode ?? 'null'} locking_mode=${confirmedLockingMode ?? lockingMode ?? 'null'}`,
    );
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

export async function openTreecrdtDbFromLoaded(
  opts: OpenTreecrdtDbOptions,
  loaded: LoadedWaSqlite,
  loadFresh: LoadFreshWaSqlite,
): Promise<OpenTreecrdtDbResult> {
  const { sqlite3, module } = loaded;
  let opfsError: string | undefined;
  let opfsFailure: unknown;
  const requestedFilename = opts.filename ?? '/treecrdt.db';
  const opfsWriteMode = opts.opfsWriteMode ?? 'default';

  if (opts.storage === 'opfs') {
    let vfs: { close?: () => Promise<void> | void } | undefined;
    try {
      const opfsVfsKind: OpfsVfsKind =
        opfsWriteMode === 'single-owner-wal' ? 'access-handle-pool' : (opts.opfsVfs ?? 'coop-sync');
      const opfsVfsName =
        opfsVfsKind === 'access-handle-pool'
          ? await accessHandlePoolVfsNameForFilename(requestedFilename)
          : OPFS_VFS_NAME;
      const initializedVfs = await createOpfsVfs(module, {
        name: opfsVfsName,
        kind: opfsVfsKind,
      });
      vfs = initializedVfs;
      sqlite3.vfs_register(initializedVfs, false);
      const opened = await openInitializedDatabase(
        sqlite3,
        module,
        requestedFilename,
        opts,
        opfsVfsName,
        (db) => configureOpfsWriteMode(db, opfsWriteMode),
      );
      return {
        ...opened,
        db: closeDatabaseWithVfs(opened.db, initializedVfs),
        storage: 'opfs',
        filename: requestedFilename,
        opfsVfsKind,
        opfsVfsName,
      };
    } catch (err) {
      opfsFailure = err;
      opfsError = errorMessage(err);
      await closeIgnoringErrors(vfs?.close ? () => vfs!.close!() : undefined);
      if (opts.requireOpfs) {
        const requiredError = new Error(
          `OPFS requested but could not be initialized: ${opfsError}`,
        ) as Error & { cause?: unknown };
        requiredError.cause = err;
        throw requiredError;
      }
    }
  }

  // A rejected wa-sqlite open_v2 may retain a C database handle that its JavaScript wrapper
  // does not expose. Do not continue on that wrapper after an OPFS attempt fails.
  try {
    const memoryLoaded = opfsError !== undefined ? await loadFresh() : loaded;
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

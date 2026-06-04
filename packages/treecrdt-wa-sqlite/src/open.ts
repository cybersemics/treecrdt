import { accessHandlePoolVfsNameForFilename, createOpfsVfs, type OpfsVfsKind } from './opfs.js';
import { createWaSqliteApi } from './adapter.js';
import { dbGetText } from './sql.js';
import { initializeTreecrdtExtension } from './extension.js';
import type { Database, OpfsWriteMode } from './types.js';
import { makeDbAdapter } from './db.js';
import type { TreecrdtAdapter } from '@treecrdt/interface';
import type { MaterializationEvent } from '@treecrdt/interface/engine';

export type OpenTreecrdtDbOptions = {
  baseUrl: string;
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

export async function openTreecrdtDb(opts: OpenTreecrdtDbOptions): Promise<OpenTreecrdtDbResult> {
  const baseUrl = opts.baseUrl.endsWith('/') ? opts.baseUrl : `${opts.baseUrl}/`;
  const sqliteModule = await import(/* @vite-ignore */ `${baseUrl}wa-sqlite/wa-sqlite-async.mjs`);
  const sqliteApi = await import(/* @vite-ignore */ `${baseUrl}wa-sqlite/sqlite-api.js`);
  const module = await sqliteModule.default({
    locateFile: (file: string) =>
      file.endsWith('.wasm') ? `${baseUrl}wa-sqlite/wa-sqlite-async.wasm` : file,
  });
  const sqlite3 = sqliteApi.Factory(module);

  let storage: 'memory' | 'opfs' = opts.storage === 'opfs' ? 'opfs' : 'memory';
  let opfsError: string | undefined;
  let opfsVfs: { close?: () => Promise<void> | void } | undefined;
  const requestedFilename = opts.filename ?? '/treecrdt.db';
  const opfsVfsKind: OpfsVfsKind =
    opts.opfsWriteMode === 'single-owner-wal'
      ? 'access-handle-pool'
      : (opts.opfsVfs ?? 'coop-sync');
  const opfsVfsName =
    opfsVfsKind === 'access-handle-pool'
      ? accessHandlePoolVfsNameForFilename(requestedFilename)
      : 'opfs';
  if (storage === 'opfs') {
    try {
      opfsVfs = await createOpfsVfs(module, { name: opfsVfsName, kind: opfsVfsKind });
      sqlite3.vfs_register(opfsVfs, true);
    } catch (err) {
      opfsError = err instanceof Error ? err.message : String(err);
      if (opts.requireOpfs) {
        throw new Error(`OPFS requested but could not be initialized: ${opfsError}`);
      }
      storage = 'memory';
    }
  }

  const filename = storage === 'opfs' ? requestedFilename : ':memory:';
  const handle = await sqlite3.open_v2(filename);
  const baseDb = makeDbAdapter(sqlite3, handle);
  const db: Database =
    opfsVfs?.close && baseDb.close
      ? {
          ...baseDb,
          close: async () => {
            try {
              await baseDb.close?.();
            } finally {
              await opfsVfs.close?.();
            }
          },
        }
      : baseDb;
  try {
    if (storage === 'opfs') {
      await configureOpfsWriteMode(db, opts.opfsWriteMode ?? 'default');
    }
    await initializeTreecrdtExtension(module, handle);
    const api = createWaSqliteApi(db, { onMaterialized: opts.onMaterialized });
    await api.setDocId(opts.docId);

    const result = {
      db,
      api,
      storage,
      filename,
      ...(storage === 'opfs' ? { opfsVfsKind, opfsVfsName } : {}),
    };
    return opfsError ? { ...result, opfsError } : result;
  } catch (err) {
    try {
      await db.close?.();
    } catch {
      // Ignore close errors while surfacing the original open/configuration error.
    }
    throw err;
  }
}

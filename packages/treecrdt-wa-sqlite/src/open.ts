import { createOpfsVfs } from './opfs.js';
import { createWaSqliteApi } from './index.js';
import type { Database } from './index.js';
import { makeDbAdapter } from './db.js';
import type { TreecrdtAdapter } from '@treecrdt/interface';

export type OpenTreecrdtDbOptions = {
  baseUrl: string;
  filename?: string;
  storage: 'memory' | 'opfs';
  docId: string;
  requireOpfs?: boolean;
};

export type OpenTreecrdtDbResult = {
  db: Database;
  api: TreecrdtAdapter;
  storage: 'memory' | 'opfs';
  filename: string;
  opfsError?: string;
};

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
  if (storage === 'opfs') {
    try {
      const vfs = await createOpfsVfs(module, { name: 'opfs' });
      sqlite3.vfs_register(vfs, true);
    } catch (err) {
      opfsError = err instanceof Error ? err.message : String(err);
      if (opts.requireOpfs) {
        throw new Error(`OPFS requested but could not be initialized: ${opfsError}`);
      }
      storage = 'memory';
    }
  }

  const filename = storage === 'opfs' ? (opts.filename ?? '/treecrdt.db') : ':memory:';
  const handle = await sqlite3.open_v2(filename);
  const db = makeDbAdapter(sqlite3, handle);
  const api = createWaSqliteApi(db);
  await api.setDocId(opts.docId);

  return opfsError ? { db, api, storage, filename, opfsError } : { db, api, storage, filename };
}

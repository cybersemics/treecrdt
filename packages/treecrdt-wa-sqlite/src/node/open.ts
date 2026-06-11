import { createWaSqliteApi } from '../adapter.js';
import { makeDbAdapter } from '../db.js';
import type { OpenTreecrdtDbOptions, OpenTreecrdtDbResult } from '../open-core.js';
import { loadWaSqliteNode } from './load-wa-sqlite.js';

/** Node entry: loads wa-sqlite WASM from the filesystem (in-memory only). */
export async function openTreecrdtDbNode(opts: OpenTreecrdtDbOptions): Promise<OpenTreecrdtDbResult> {
  if (opts.storage === 'opfs' && opts.requireOpfs) {
    throw new Error('OPFS is not supported in Node');
  }
  const { sqlite3 } = await loadWaSqliteNode(opts.baseUrl);
  const handle = await sqlite3.open_v2(':memory:');
  const db = makeDbAdapter(sqlite3, handle);
  const api = createWaSqliteApi(db, { onMaterialized: opts.onMaterialized });
  await api.setDocId(opts.docId);
  return { db, api, storage: 'memory', filename: ':memory:' };
}

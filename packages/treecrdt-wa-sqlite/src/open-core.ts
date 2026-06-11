import { createOpfsVfs, type OpfsVfsKind } from './opfs.js';
import { createWaSqliteApi } from './adapter.js';
import type { Database } from './types.js';
import { makeDbAdapter } from './db.js';
import type { TreecrdtAdapter } from '@treecrdt/interface';
import type { MaterializationEvent } from '@treecrdt/interface/engine';

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

export async function openTreecrdtDbFromLoaded(
  opts: OpenTreecrdtDbOptions,
  loaded: { sqlite3: any; module: any },
): Promise<OpenTreecrdtDbResult> {
  const { sqlite3, module } = loaded;

  let storage: 'memory' | 'opfs' = opts.storage === 'opfs' ? 'opfs' : 'memory';
  let opfsError: string | undefined;

  if (storage === 'opfs') {
    try {
      const vfs = await createOpfsVfs(module, { name: 'opfs', kind: opts.opfsVfs });
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
  const api = createWaSqliteApi(db, { onMaterialized: opts.onMaterialized });
  await api.setDocId(opts.docId);

  return opfsError ? { db, api, storage, filename, opfsError } : { db, api, storage, filename };
}

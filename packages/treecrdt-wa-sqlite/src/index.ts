import type { TreecrdtAdapter } from '@treecrdt/interface';
import { createTreecrdtSqliteAdapter, type SqliteRunner } from '@treecrdt/interface/sqlite';
import type { MaterializationEvent } from '@treecrdt/interface/engine';
import { dbGetText } from './sql.js';
import type { Database } from './types.js';

export type {
  ClientMode,
  ClientOptions,
  Database,
  RuntimeMode,
  StorageMode,
  TreecrdtAssets,
  TreecrdtClient,
  TreecrdtClientAuthApi,
  TreecrdtRuntime,
  TreecrdtStorage,
} from './types.js';

export type {
  OpfsSupport,
  OpfsVfsKind,
  OpfsVfsOptions,
  OpenOptions,
} from './opfs.js';
export {
  clearOpfsStorage,
  createOpfsVfs,
  detectOpfsSupport,
  openWithStorage,
  opfsStorageExists,
} from './opfs.js';

export { CLIENT_CLOSED_ERROR, createTreecrdtClient } from './client.js';

function createRunner(db: Database): SqliteRunner {
  return {
    exec: (sql) => db.exec(sql),
    getText: (sql, params = []) => dbGetText(db, sql, params ?? []),
  };
}

export function createWaSqliteApi(
  db: Database,
  opts: { maxBulkOps?: number; onMaterialized?: (event: MaterializationEvent) => void } = {},
): TreecrdtAdapter {
  return createTreecrdtSqliteAdapter(createRunner(db), opts);
}

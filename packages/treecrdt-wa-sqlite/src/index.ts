import type { TreecrdtAdapter } from '@justthrowaway/interface';
import { createTreecrdtSqliteAdapter, type SqliteRunner } from '@justthrowaway/interface/sqlite';
import type { MaterializationEvent } from '@justthrowaway/interface/engine';
import { dbGetText } from './sql.js';
import type { Database } from './types.js';

export type { Database } from './types.js';

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

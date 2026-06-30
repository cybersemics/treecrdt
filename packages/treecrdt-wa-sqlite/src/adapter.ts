import type { TreecrdtAdapter } from '@treecrdt/interface';
import { createTreecrdtSqliteAdapter, type SqliteRunner } from '@treecrdt/interface/sqlite';
import type { MaterializationEvent } from '@treecrdt/interface/engine';
import { dbGetText } from './sql.js';
import type { Database, OpfsWriteMode } from './types.js';
import { runOpfsWriteAheadWrite } from './opfs-write-ahead.js';

function createRunner(db: Database, opfsWriteMode?: OpfsWriteMode): SqliteRunner {
  return {
    exec: async (sql) => {
      await runOpfsWriteAheadWrite(
        opfsWriteMode,
        (txSql) => db.exec(txSql),
        sql,
        () => db.exec(sql),
      );
    },
    getText: async (sql, params = []) => {
      return runOpfsWriteAheadWrite(
        opfsWriteMode,
        (txSql) => db.exec(txSql),
        sql,
        () => dbGetText(db, sql, params ?? []),
      );
    },
  };
}

export function createWaSqliteApi(
  db: Database,
  opts: {
    maxBulkOps?: number;
    onMaterialized?: (event: MaterializationEvent) => void;
    opfsWriteMode?: OpfsWriteMode;
  } = {},
): TreecrdtAdapter {
  return createTreecrdtSqliteAdapter(createRunner(db, opts.opfsWriteMode), opts);
}

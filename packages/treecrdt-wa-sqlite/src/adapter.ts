import type { TreecrdtAdapter } from '@treecrdt/interface';
import { createTreecrdtSqliteAdapter, type SqliteRunner } from '@treecrdt/interface/sqlite';
import type { MaterializationEvent } from '@treecrdt/interface/engine';
import { dbGetText } from './sql.js';
import type { Database, OpfsWriteMode } from './types.js';
import { createOpfsWriteAheadExecutor } from './opfs-write-ahead.js';

export function createWaSqliteRunner(db: Database, opfsWriteMode?: OpfsWriteMode): SqliteRunner {
  const direct: SqliteRunner = {
    exec: (sql) => db.exec(sql),
    getText: (sql, params = []) => dbGetText(db, sql, params ?? []),
  };
  if (opfsWriteMode !== 'opfs-write-ahead') return direct;

  const run = createOpfsWriteAheadExecutor({
    exec: direct.exec,
    getAutocommit: () => db.getAutocommit(),
  });
  return {
    exec: async (sql) => {
      await run(sql, () => direct.exec(sql), { allowTransactionControlBatch: true });
    },
    getText: (sql, params = []) => run(sql, () => direct.getText(sql, params)),
  };
}

export function createWaSqliteApiFromRunner(
  runner: SqliteRunner,
  opts: { onMaterialized?: (event: MaterializationEvent) => void } = {},
): TreecrdtAdapter {
  return createTreecrdtSqliteAdapter(runner, opts);
}

export function createWaSqliteApi(
  db: Database,
  opts: {
    onMaterialized?: (event: MaterializationEvent) => void;
    opfsWriteMode?: OpfsWriteMode;
  } = {},
): TreecrdtAdapter {
  return createWaSqliteApiFromRunner(createWaSqliteRunner(db, opts.opfsWriteMode), opts);
}

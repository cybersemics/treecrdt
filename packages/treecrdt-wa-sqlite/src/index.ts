import type { TreecrdtAdapter } from '@treecrdt/interface';
import { createTreecrdtSqliteAdapter, type SqliteRunner } from '@treecrdt/interface/sqlite';

// Minimal wa-sqlite surface needed by the adapter. Exported so consumers
// don't need to import types from wa-sqlite directly.
export type Database = {
  prepare(sql: string): Promise<number> | number;
  bind(stmt: number, index: number, value: unknown): Promise<void> | void;
  step(stmt: number): Promise<number> | number;
  column_text(stmt: number, index: number): Promise<string> | string;
  finalize(stmt: number): Promise<void> | void;
  exec(sql: string): Promise<void> | void;
  close?(): Promise<void> | void;
};

function createRunner(db: Database): SqliteRunner {
  return {
    exec: (sql) => db.exec(sql),
    getText: async (sql, params = []) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const stmt: any = await db.prepare(sql);
      try {
        let idx = 1;
        for (const p of params) {
          await db.bind(stmt, idx++, p);
        }
        const row = await db.step(stmt);
        if (row === 0) return null;
        return await db.column_text(stmt, 0);
      } finally {
        await db.finalize(stmt);
      }
    },
  };
}

export function createWaSqliteApi(
  db: Database,
  opts: { maxBulkOps?: number } = {},
): TreecrdtAdapter {
  return createTreecrdtSqliteAdapter(createRunner(db), opts);
}

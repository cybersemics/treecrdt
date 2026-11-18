import type { SqlCall } from '@treecrdt/interface/sqlite';
import type { Database } from './types.js';

/** SQLITE_ROW = 100. See https://www.sqlite.org/c3ref/step.html */
const SQLITE_ROW = 100;

function normalizeBindValue(value: unknown): unknown {
  if (ArrayBuffer.isView(value)) {
    const view = value as ArrayBufferView;
    return new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
  }

  return value;
}

/** Builds a Database handle over an open wa-sqlite connection. */
export function createDatabase(sqlite3: any, handle: number): Database {
  const prepare = async (sql: string) => {
    const iter = sqlite3.statements(handle, sql, { unscoped: true });
    const { value } = await iter.next();
    if (iter.return) await iter.return();
    if (!value) {
      throw new Error(`Failed to prepare statement: ${sql}`);
    }
    return value;
  };

  const bind = async (stmt: number, index: number, value: unknown) =>
    sqlite3.bind(stmt, index, normalizeBindValue(value));

  const step = async (stmt: number) => sqlite3.step(stmt);
  const column_text = async (stmt: number, index: number) => sqlite3.column_text(stmt, index);
  const finalize = async (stmt: number) => sqlite3.finalize(stmt);

  /** Prepares `sql`, binds `params` from index 1, and finalizes the statement after `use`. */
  const withStatement = async <T>(
    sql: string,
    params: SqlCall['params'],
    use: (stmt: number) => Promise<T>,
  ): Promise<T> => {
    const stmt = await prepare(sql);
    try {
      let idx = 1;
      for (const p of params) {
        await bind(stmt, idx++, p);
      }
      return await use(stmt);
    } finally {
      await finalize(stmt);
    }
  };

  /** Single-row query → first column as text, or null when SQLITE_DONE. */
  const getText = (sql: string, params: SqlCall['params'] = []): Promise<string | null> =>
    withStatement(sql, params, async (stmt) =>
      (await step(stmt)) === SQLITE_ROW ? column_text(stmt, 0) : null,
    );

  /** Steps the statement until SQLITE_DONE, skipping any rows it returns. */
  const run = (sql: string, params: SqlCall['params'] = []): Promise<void> =>
    withStatement(sql, params, async (stmt) => {
      while ((await step(stmt)) === SQLITE_ROW);
    });

  return {
    prepare,
    bind,
    step,
    column_text,
    finalize,
    exec: async (sql: string) => sqlite3.exec(handle, sql),
    run,
    getText,
    close: async () => sqlite3.close(handle),
  };
}

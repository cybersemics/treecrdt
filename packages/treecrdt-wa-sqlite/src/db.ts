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

  /** Single-row query → first column as text, or null when SQLITE_DONE. */
  const getText = async (sql: string, params: SqlCall['params'] = []): Promise<string | null> => {
    const stmt = await prepare(sql);
    try {
      let idx = 1;
      for (const p of params) {
        await bind(stmt, idx++, p);
      }
      const row = await step(stmt);
      if (row !== SQLITE_ROW) return null;
      return await column_text(stmt, 0);
    } finally {
      await finalize(stmt);
    }
  };

  return {
    prepare,
    bind,
    step,
    column_text,
    finalize,
    exec: async (sql: string) => sqlite3.exec(handle, sql),
    getText,
    close: async () => sqlite3.close(handle),
  };
}

import {
  createTreecrdtDb,
  type SqlParam,
  type SqlValue,
} from '@treecrdt/interface/sqlite';
import type { Database } from './types.js';

/** SQLITE_ROW = 100. See https://www.sqlite.org/c3ref/step.html */
const SQLITE_ROW = 100;
const SQLITE_INTEGER = 1;
const SQLITE_FLOAT = 2;
const SQLITE_TEXT = 3;
const SQLITE_BLOB = 4;
const SQLITE_NULL = 5;

function normalizeBindValue(value: unknown): unknown {
  if (ArrayBuffer.isView(value)) {
    const view = value as ArrayBufferView;
    return new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
  }

  return value;
}

function readColumn(sqlite3: any, stmt: number, index: number): SqlValue {
  const type = sqlite3.column_type(stmt, index);
  if (type === SQLITE_NULL) return null;
  if (type === SQLITE_INTEGER) return sqlite3.column_int(stmt, index);
  if (type === SQLITE_FLOAT) return sqlite3.column_double(stmt, index);
  if (type === SQLITE_TEXT) return sqlite3.column_text(stmt, index);
  if (type === SQLITE_BLOB) {
    const blob = sqlite3.column_blob(stmt, index) as Uint8Array | null;
    if (!blob) return null;
    return blob instanceof Uint8Array ? new Uint8Array(blob) : Uint8Array.from(blob);
  }
  return sqlite3.column_text(stmt, index);
}

/** Builds a Database (Kysely + close) over an open wa-sqlite connection. */
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
  const finalize = async (stmt: number) => sqlite3.finalize(stmt);

  const bindAll = async (stmt: number, params: SqlParam[]) => {
    let idx = 1;
    for (const p of params) {
      await bind(stmt, idx++, p);
    }
  };

  const query = async (
    sql: string,
    params: SqlParam[] = [],
  ): Promise<Record<string, SqlValue>[]> => {
    const stmt = await prepare(sql);
    try {
      await bindAll(stmt, params);
      const rows: Record<string, SqlValue>[] = [];
      while ((await step(stmt)) === SQLITE_ROW) {
        const count = sqlite3.column_count(stmt);
        const row: Record<string, SqlValue> = {};
        for (let i = 0; i < count; i += 1) {
          const name = String(sqlite3.column_name(stmt, i) ?? `column${i}`);
          row[name] = readColumn(sqlite3, stmt, i);
        }
        rows.push(row);
      }
      return rows;
    } finally {
      await finalize(stmt);
    }
  };

  const db = createTreecrdtDb({ query });

  return {
    db,
    execBatch: async (sql: string) => {
      await sqlite3.exec(handle, sql);
    },
    close: async () => sqlite3.close(handle),
  };
}

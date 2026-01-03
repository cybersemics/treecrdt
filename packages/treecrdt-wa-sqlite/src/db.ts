import type { Database } from "./index.js";

export function makeDbAdapter(sqlite3: any, handle: number): Database {
  const prepare = async (sql: string) => {
    const iter = sqlite3.statements(handle, sql, { unscoped: true });
    const { value } = await iter.next();
    if (iter.return) await iter.return();
    if (!value) {
      throw new Error(`Failed to prepare statement: ${sql}`);
    }
    return value;
  };

  return {
    prepare,
    bind: async (stmt: number, index: number, value: unknown) => sqlite3.bind(stmt, index, value),
    step: async (stmt: number) => sqlite3.step(stmt),
    column_text: async (stmt: number, index: number) => sqlite3.column_text(stmt, index),
    finalize: async (stmt: number) => sqlite3.finalize(stmt),
    exec: async (sql: string) => sqlite3.exec(handle, sql),
    close: async () => sqlite3.close(handle),
  } as unknown as Database;
}

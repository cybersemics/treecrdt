import type { Database } from "./types.js";

/** SQLITE_ROW = 100. See https://www.sqlite.org/c3ref/step.html */
const SQLITE_ROW = 100;

/**
 * Execute a single-row query and return the first column as text.
 * Returns null when no rows are returned (SQLITE_DONE).
 */
export async function dbGetText(
  db: Database,
  sql: string,
  params: unknown[] = []
): Promise<string | null> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare(sql);
  try {
    let idx = 1;
    for (const p of params) {
      await db.bind(stmt, idx++, p);
    }
    const row = await db.step(stmt);
    if (row !== SQLITE_ROW) return null;
    return await db.column_text(stmt, 0);
  } finally {
    await db.finalize(stmt);
  }
}

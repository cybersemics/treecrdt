import { sql } from '@treecrdt/interface/sqlite';
import type { Database } from './types.js';

type WaSqliteModule = {
  _treecrdt_sqlite_init?: (handle: number) => number;
};

function firstColumnText(row: Record<string, unknown> | undefined): string | null {
  if (!row) return null;
  const value = Object.values(row)[0];
  if (value === undefined || value === null) return null;
  return typeof value === 'string' ? value : String(value);
}

/** Initialize the statically linked TreeCRDT extension on an open wa-sqlite handle. */
export async function initializeTreecrdtExtension(
  module: WaSqliteModule,
  handle: number,
  database: Database,
  docId?: string,
): Promise<void> {
  if (typeof module?._treecrdt_sqlite_init !== 'function') {
    throw new Error('wa-sqlite module does not expose the TreeCRDT extension');
  }

  // Registration performs no I/O; only the SQL below needs asynchronous execution.
  const rc = module._treecrdt_sqlite_init(handle);
  if (rc !== 0) {
    throw new Error(`TreeCRDT SQLite extension init failed (rc=${rc})`);
  }

  const schemaResult = await sql`select treecrdt_schema()`.execute(database.db);
  const schema = firstColumnText(schemaResult.rows[0] as Record<string, unknown> | undefined);
  if (!schema) throw new Error('TreeCRDT extension did not provide its schema');

  // Let wa-sqlite await each statement's VFS work on the already-open handle.
  // One transaction also prevents a failed initialization leaving a partial schema.
  await database.execBatch('BEGIN IMMEDIATE');
  try {
    await database.execBatch(schema);
    // Pin the ID before releasing the write lock to another opening connection.
    if (docId !== undefined) {
      await sql`select treecrdt_set_doc_id(${docId})`.execute(database.db);
    }
    await database.execBatch('COMMIT');
  } catch (error) {
    try {
      await database.execBatch('ROLLBACK');
    } catch {
      // Preserve the initialization error; the caller owns closing the handle.
    }
    throw error;
  }
}

import type { OpfsWriteMode } from './types.js';

export function treecrdtSqlRequiresWriteTransaction(sql: string): boolean {
  const normalized = sql.trim().toLowerCase();
  return (
    normalized.startsWith('select treecrdt_append_') ||
    normalized.startsWith('select treecrdt_ensure_materialized') ||
    normalized.startsWith('select treecrdt_local_') ||
    normalized.startsWith('select treecrdt_set_doc_id')
  );
}

export async function runOpfsWriteAheadWrite<T>(
  mode: OpfsWriteMode | undefined,
  exec: (sql: string) => Promise<void> | void,
  sql: string,
  fn: () => Promise<T> | T,
): Promise<T> {
  if (mode !== 'opfs-write-ahead' || !treecrdtSqlRequiresWriteTransaction(sql)) {
    return await fn();
  }

  // OPFSWriteAheadVFS rejects deferred write transactions. TreeCRDT extension
  // write entrypoints must enter SQLite through BEGIN IMMEDIATE.
  await exec('BEGIN IMMEDIATE');
  try {
    const result = await fn();
    await exec('COMMIT');
    return result;
  } catch (err) {
    await Promise.resolve(exec('ROLLBACK')).catch(() => {});
    throw err;
  }
}

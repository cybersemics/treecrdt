import type { Operation, TreecrdtAdapter } from "@treecrdt/interface";
import {
  createTreecrdtSqliteAdapter,
  treecrdtAppendOp,
  treecrdtOpsSince,
  type SqliteRunner,
} from "@treecrdt/interface/sqlite";

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

export type OpsSinceFilter = {
  lamport: number;
  root?: string; // node id as hex string or other canonical encoding
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

/**
 * Set the document id used by the SQLite extension for v0 sync (`op_ref` derivation).
 *
 * This MUST be stable for the lifetime of the database, since it affects opRef hashes.
 */
export async function setDocId(db: Database, docId: string): Promise<void> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare("SELECT treecrdt_set_doc_id(?1)");
  await db.bind(stmt, 1, docId);
  await db.step(stmt);
  await db.finalize(stmt);
}

/**
 * Fetch all stored opRefs (16-byte values) from the extension.
 * Returns raw JSON-decoded values: `number[][]` (bytes) is the expected shape.
 */
export async function opRefsAll(db: Database): Promise<unknown[]> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare("SELECT treecrdt_oprefs_all()");
  const row = await db.step(stmt);
  let result: unknown[] = [];
  if (row !== 0) {
    const json = await db.column_text(stmt, 0);
    result = JSON.parse(json);
  }
  await db.finalize(stmt);
  return result;
}

/**
 * Fetch opRefs relevant to the `children(parent)` filter from the extension.
 * Returns raw JSON-decoded values: `number[][]` (bytes) is the expected shape.
 */
export async function opRefsChildren(db: Database, parent: Uint8Array): Promise<unknown[]> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare("SELECT treecrdt_oprefs_children(?1)");
  await db.bind(stmt, 1, parent);
  const row = await db.step(stmt);
  let result: unknown[] = [];
  if (row !== 0) {
    const json = await db.column_text(stmt, 0);
    result = JSON.parse(json);
  }
  await db.finalize(stmt);
  return result;
}

/**
 * Fetch operations by opRef (16-byte values) from the extension.
 * Returns raw JSON-decoded operation rows (same shape as `opsSince`).
 */
export async function opsByOpRefs(db: Database, opRefs: Uint8Array[]): Promise<unknown[]> {
  const payload = opRefs.map((r) => Array.from(r));
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare("SELECT treecrdt_ops_by_oprefs(?1)");
  await db.bind(stmt, 1, JSON.stringify(payload));
  const row = await db.step(stmt);
  let result: unknown[] = [];
  if (row !== 0) {
    const json = await db.column_text(stmt, 0);
    result = JSON.parse(json);
  }
  await db.finalize(stmt);
  return result;
}

/**
 * Fetch materialized children for a parent node (16-byte id).
 * Returns raw JSON-decoded values: `number[][]` (bytes) is the expected shape.
 */
export async function treeChildren(db: Database, parent: Uint8Array): Promise<unknown[]> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare("SELECT treecrdt_tree_children(?1)");
  await db.bind(stmt, 1, parent);
  const row = await db.step(stmt);
  let result: unknown[] = [];
  if (row !== 0) {
    const json = await db.column_text(stmt, 0);
    result = JSON.parse(json);
  }
  await db.finalize(stmt);
  return result;
}

/**
 * Dump the full materialized tree state.
 * Returns raw JSON-decoded rows (array of objects with byte fields).
 */
export async function treeDump(db: Database): Promise<unknown[]> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare("SELECT treecrdt_tree_dump()");
  const row = await db.step(stmt);
  let result: unknown[] = [];
  if (row !== 0) {
    const json = await db.column_text(stmt, 0);
    result = JSON.parse(json);
  }
  await db.finalize(stmt);
  return result;
}

/**
 * Count non-tombstoned nodes in the materialized tree (excluding ROOT).
 */
export async function treeNodeCount(db: Database): Promise<number> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare("SELECT treecrdt_tree_node_count()");
  const row = await db.step(stmt);
  let result = 0;
  if (row !== 0) {
    const val = await db.column_text(stmt, 0);
    result = Number(val);
  }
  await db.finalize(stmt);
  return result;
}

/**
 * Fetch the maximum lamport seen in the op log.
 */
export async function headLamport(db: Database): Promise<number> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare("SELECT treecrdt_head_lamport()");
  const row = await db.step(stmt);
  let result = 0;
  if (row !== 0) {
    const val = await db.column_text(stmt, 0);
    result = Number(val);
  }
  await db.finalize(stmt);
  return result;
}

/**
 * Fetch the maximum counter observed for a replica id.
 */
export async function replicaMaxCounter(db: Database, replica: Uint8Array): Promise<number> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare("SELECT treecrdt_replica_max_counter(?1)");
  await db.bind(stmt, 1, replica);
  const row = await db.step(stmt);
  let result = 0;
  if (row !== 0) {
    const val = await db.column_text(stmt, 0);
    result = Number(val);
  }
  await db.finalize(stmt);
  return result;
}

/**
  Append an operation by calling the extension function.
*/
export async function appendOp(
  db: Database,
  op: Operation,
  serializeNodeId: (id: string) => Uint8Array,
  serializeReplica: (id: Operation["meta"]["id"]["replica"]) => Uint8Array
) {
  await treecrdtAppendOp(createRunner(db), op, serializeNodeId, serializeReplica);
}

/**
  Fetch ops since a lamport (optionally filtered by root) and parse to typed operations.
  Currently returns raw JSON from the extension; caller should map to Operation.
*/
export async function opsSince(
  db: Database,
  filter: OpsSinceFilter
): Promise<unknown[]> {
  return treecrdtOpsSince(createRunner(db), filter);
}

export function createWaSqliteAdapter(db: Database): TreecrdtAdapter {
  return createTreecrdtSqliteAdapter(createRunner(db));
}

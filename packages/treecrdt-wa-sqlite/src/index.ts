import type { Operation, TreecrdtAdapter } from "@treecrdt/interface";
import {
  createTreecrdtSqliteAdapter,
  treecrdtAppendOp,
  treecrdtDocId,
  treecrdtHeadLamport,
  treecrdtOpRefsAll,
  treecrdtOpRefsChildren,
  treecrdtOpsByOpRefs,
  treecrdtOpsSince,
  treecrdtReplicaMaxCounter,
  treecrdtSetDocId,
  treecrdtTreeChildren,
  treecrdtTreeDump,
  treecrdtTreeNodeCount,
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
  await treecrdtSetDocId(createRunner(db), docId);
}

/**
 * Fetch all stored opRefs (16-byte values) from the extension.
 * Returns raw JSON-decoded values: `number[][]` (bytes) is the expected shape.
 */
export async function opRefsAll(db: Database): Promise<unknown[]> {
  return treecrdtOpRefsAll(createRunner(db));
}

/**
 * Fetch opRefs relevant to the `children(parent)` filter from the extension.
 * Returns raw JSON-decoded values: `number[][]` (bytes) is the expected shape.
 */
export async function opRefsChildren(db: Database, parent: Uint8Array): Promise<unknown[]> {
  return treecrdtOpRefsChildren(createRunner(db), parent);
}

/**
 * Fetch operations by opRef (16-byte values) from the extension.
 * Returns raw JSON-decoded operation rows (same shape as `opsSince`).
 */
export async function opsByOpRefs(db: Database, opRefs: Uint8Array[]): Promise<unknown[]> {
  return treecrdtOpsByOpRefs(createRunner(db), opRefs);
}

/**
 * Fetch materialized children for a parent node (16-byte id).
 * Returns raw JSON-decoded values: `number[][]` (bytes) is the expected shape.
 */
export async function treeChildren(db: Database, parent: Uint8Array): Promise<unknown[]> {
  return treecrdtTreeChildren(createRunner(db), parent);
}

/**
 * Dump the full materialized tree state.
 * Returns raw JSON-decoded rows (array of objects with byte fields).
 */
export async function treeDump(db: Database): Promise<unknown[]> {
  return treecrdtTreeDump(createRunner(db));
}

/**
 * Count non-tombstoned nodes in the materialized tree (excluding ROOT).
 */
export async function treeNodeCount(db: Database): Promise<number> {
  return treecrdtTreeNodeCount(createRunner(db));
}

/**
 * Fetch the maximum lamport seen in the op log.
 */
export async function headLamport(db: Database): Promise<number> {
  return treecrdtHeadLamport(createRunner(db));
}

/**
 * Fetch the maximum counter observed for a replica id.
 */
export async function replicaMaxCounter(db: Database, replica: Uint8Array): Promise<number> {
  return treecrdtReplicaMaxCounter(createRunner(db), replica);
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

export async function docId(db: Database): Promise<string | null> {
  return treecrdtDocId(createRunner(db));
}

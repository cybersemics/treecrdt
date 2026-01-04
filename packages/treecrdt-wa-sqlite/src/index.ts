import type { Operation, TreecrdtAdapter } from "@treecrdt/interface";
import { buildAppendOp, buildOpsSince } from "@treecrdt/interface/sqlite";
import { nodeIdToBytes16 } from "@treecrdt/interface/ids";

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
  Append an operation by calling the extension function.
*/
export async function appendOp(
  db: Database,
  op: Operation,
  serializeNodeId: (id: string) => Uint8Array,
  serializeReplica: (id: Operation["meta"]["id"]["replica"]) => Uint8Array
) {
  const { meta, kind } = op;
  const { id, lamport } = meta;
  const { replica, counter } = id;

  const { sql, params } = buildAppendOp(kind, {
    replica: serializeReplica(replica),
    counter,
    lamport,
    serializeNodeId,
  });

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare(sql);
  let idx = 1;
  for (const p of params) {
    await db.bind(stmt, idx++, p);
  }
  await db.step(stmt);
  await db.finalize(stmt);
}

/**
  Fetch ops since a lamport (optionally filtered by root) and parse to typed operations.
  Currently returns raw JSON from the extension; caller should map to Operation.
*/
export async function opsSince(
  db: Database,
  filter: OpsSinceFilter
): Promise<unknown[]> {
  const { sql, params } = buildOpsSince({
    lamport: filter.lamport,
    root: filter.root,
    serializeNodeId: nodeIdToBytes16,
  });

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare(sql);
  let idx = 1;
  for (const p of params) {
    await db.bind(stmt, idx++, p);
  }
  const row = await db.step(stmt);
  let result: unknown[] = [];
  if (row === 0) {
    // done
  } else {
    const json = await db.column_text(stmt, 0);
    result = JSON.parse(json);
  }
  await db.finalize(stmt);
  return result;
}

export function createWaSqliteAdapter(db: Database): TreecrdtAdapter {
  return {
    appendOp: (op, serializeNodeId, serializeReplica) =>
      appendOp(db, op, serializeNodeId, serializeReplica),
    appendOps: async (ops, serializeNodeId, serializeReplica) => {
      if (ops.length === 0) return;
      const bulkSql = "SELECT treecrdt_append_ops(?1)";
      const maxBulkOps = 5_000;
      const serialize = (val: string) => Array.from(serializeNodeId(val));

      // Try bulk entrypoint first, chunked to avoid huge JSON payloads.
      let bulkFailedAt: number | null = null;
      for (let start = 0; start < ops.length; start += maxBulkOps) {
        const chunk = ops.slice(start, start + maxBulkOps);
        const payload = chunk.map((op) => {
          const { meta, kind } = op;
          const { id, lamport } = meta;
          const { replica, counter } = id;
          const serReplica = serializeReplica(replica);
          const base = {
            replica: Array.from(serReplica),
            counter,
            lamport,
            kind: kind.type,
            position: "position" in kind ? kind.position ?? null : null,
          };
          if (kind.type === "insert") {
            return { ...base, parent: serialize(kind.parent), node: serialize(kind.node), new_parent: null };
          } else if (kind.type === "move") {
            return { ...base, parent: null, node: serialize(kind.node), new_parent: serialize(kind.newParent) };
          } else if (kind.type === "delete") {
            return { ...base, parent: null, node: serialize(kind.node), new_parent: null };
          }
          return { ...base, parent: null, node: serialize(kind.node), new_parent: null };
        });
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const bulkStmt: any = await db.prepare(bulkSql);
        try {
          await db.bind(bulkStmt, 1, JSON.stringify(payload));
          await db.step(bulkStmt);
          await db.finalize(bulkStmt);
        } catch {
          await db.finalize(bulkStmt);
          bulkFailedAt = start;
          break;
        }
      }
      if (bulkFailedAt === null) return;

      const remaining = ops.slice(bulkFailedAt);
      await db.exec("BEGIN");
      try {
        for (const op of remaining) {
          const { meta, kind } = op;
          const { id, lamport } = meta;
          const { replica, counter } = id;
          const { sql, params } = buildAppendOp(kind, {
            replica: serializeReplica(replica),
            counter,
            lamport,
            serializeNodeId,
          });
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const stmt: any = await db.prepare(sql);
          let idx = 1;
          for (const p of params) {
            await db.bind(stmt, idx++, p);
          }
          await db.step(stmt);
          await db.finalize(stmt);
        }
        await db.exec("COMMIT");
      } catch (err) {
        await db.exec("ROLLBACK");
        throw err;
      }
    },
    opsSince: (lamport, root) => opsSince(db, { lamport, root }),
  };
}

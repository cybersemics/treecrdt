import type { SerializeNodeId, SerializeReplica, TreecrdtAdapter } from "./adapter.js";
import {
  decodeNodeId,
  decodeReplicaId,
  nodeIdToBytes16,
} from "./ids.js";
import type { Operation, OperationKind } from "./index.js";

export type SqlCall = {
  sql: string;
  params: (number | string | Uint8Array | null)[];
};

export type SqliteRunner = {
  exec: (sql: string) => Promise<void> | void;
  getText: (sql: string, params?: SqlCall["params"]) => Promise<string | null> | string | null;
};

async function sqliteGetJson<T>(
  runner: SqliteRunner,
  sql: string,
  params?: SqlCall["params"]
): Promise<T> {
  const text = await runner.getText(sql, params);
  if (!text) throw new Error(`expected JSON result for query: ${sql}`);
  return JSON.parse(text) as T;
}

async function sqliteGetJsonOrEmpty<T>(
  runner: SqliteRunner,
  sql: string,
  params?: SqlCall["params"]
): Promise<T> {
  const text = await runner.getText(sql, params);
  if (!text) return JSON.parse("[]") as T;
  return JSON.parse(text) as T;
}

async function sqliteGetNumber(
  runner: SqliteRunner,
  sql: string,
  params?: SqlCall["params"]
): Promise<number> {
  const text = await runner.getText(sql, params);
  if (!text) return 0;
  const value = Number(text);
  if (!Number.isFinite(value)) throw new Error(`expected numeric result for query: ${sql}`);
  return value;
}

function buildAppendOp(
  kind: OperationKind,
  opts: {
    replica: Uint8Array;
    counter: number;
    lamport: number;
    serializeNodeId: SerializeNodeId;
  }
): SqlCall {
  const base = [opts.replica, opts.counter, opts.lamport] as (
    | number
    | string
    | Uint8Array
    | null
  )[];

  switch (kind.type) {
    case "insert":
      return {
        sql: "SELECT treecrdt_append_op(?1,?2,?3,?4,?5,?6,NULL,NULL)",
        params: [
          ...base,
          "insert",
          opts.serializeNodeId(kind.parent),
          opts.serializeNodeId(kind.node),
        ],
      };
    case "move":
      return {
        sql: "SELECT treecrdt_append_op(?1,?2,?3,?4,NULL,?5,?6,?7)",
        params: [
          ...base,
          "move",
          opts.serializeNodeId(kind.node),
          opts.serializeNodeId(kind.newParent),
          kind.position,
        ],
      };
    case "delete":
      return {
        sql: "SELECT treecrdt_append_op(?1,?2,?3,?4,NULL,?5,NULL,NULL)",
        params: [...base, "delete", opts.serializeNodeId(kind.node)],
      };
    case "tombstone":
      return {
        sql: "SELECT treecrdt_append_op(?1,?2,?3,?4,NULL,?5,NULL,NULL)",
        params: [...base, "tombstone", opts.serializeNodeId(kind.node)],
      };
    default:
      throw new Error("unsupported operation kind");
  }
}

function buildOpsSince(filter: { lamport: number; root?: string; serializeNodeId: SerializeNodeId }): SqlCall {
  if (filter.root === undefined) {
    return { sql: "SELECT treecrdt_ops_since(?1)", params: [filter.lamport] };
  }
  return {
    sql: "SELECT treecrdt_ops_since(?1, ?2)",
    params: [filter.lamport, filter.serializeNodeId(filter.root)],
  };
}

function buildAppendOpsPayload(
  ops: Operation[],
  serializeNodeId: SerializeNodeId,
  serializeReplica: SerializeReplica
): unknown[] {
  const serialize = (val: string) => Array.from(serializeNodeId(val));
  return ops.map((op) => {
    const { meta, kind } = op;
    const { id, lamport } = meta;
    const { replica, counter } = id;
    const serReplica = serializeReplica(replica);
    const knownState = meta.knownState;
    const base = {
      replica: Array.from(serReplica),
      counter,
      lamport,
      kind: kind.type,
      position: "position" in kind ? kind.position ?? null : null,
      ...(knownState && knownState.length > 0 ? { known_state: Array.from(knownState) } : {}),
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
}

async function treecrdtAppendOp(
  runner: SqliteRunner,
  op: Operation,
  serializeNodeId: SerializeNodeId,
  serializeReplica: SerializeReplica
): Promise<void> {
  if (op.meta.knownState && op.meta.knownState.length > 0) {
    const payload = buildAppendOpsPayload([op], serializeNodeId, serializeReplica);
    await runner.getText("SELECT treecrdt_append_ops(?1)", [JSON.stringify(payload)]);
    return;
  }

  const { meta, kind } = op;
  const { id, lamport } = meta;
  const { replica, counter } = id;

  const { sql, params } = buildAppendOp(kind, {
    replica: serializeReplica(replica),
    counter,
    lamport,
    serializeNodeId,
  });

  await runner.getText(sql, params);
}

async function treecrdtAppendOps(
  runner: SqliteRunner,
  ops: Operation[],
  serializeNodeId: SerializeNodeId,
  serializeReplica: SerializeReplica,
  opts: { maxBulkOps?: number } = {}
): Promise<void> {
  if (ops.length === 0) return;

  const maxBulkOps = opts.maxBulkOps ?? 5_000;
  const bulkSql = "SELECT treecrdt_append_ops(?1)";

  // Try bulk entrypoint first, chunked to avoid huge JSON payloads.
  let bulkFailedAt: number | null = null;
  for (let start = 0; start < ops.length; start += maxBulkOps) {
    const chunk = ops.slice(start, start + maxBulkOps);
    const payload = buildAppendOpsPayload(chunk, serializeNodeId, serializeReplica);
    try {
      await runner.getText(bulkSql, [JSON.stringify(payload)]);
    } catch {
      bulkFailedAt = start;
      break;
    }
  }
  if (bulkFailedAt === null) return;

  const remaining = ops.slice(bulkFailedAt);
  await runner.exec("BEGIN");
  try {
    for (const op of remaining) {
      await treecrdtAppendOp(runner, op, serializeNodeId, serializeReplica);
    }
    await runner.exec("COMMIT");
  } catch (err) {
    await runner.exec("ROLLBACK");
    throw err;
  }
}

async function treecrdtOpsSince(
  runner: SqliteRunner,
  filter: { lamport: number; root?: string }
): Promise<unknown[]> {
  const { sql, params } = buildOpsSince({
    lamport: filter.lamport,
    root: filter.root,
    serializeNodeId: nodeIdToBytes16,
  });

  return sqliteGetJsonOrEmpty(runner, sql, params);
}

export function createTreecrdtSqliteAdapter(
  runner: SqliteRunner,
  opts: { maxBulkOps?: number } = {}
): TreecrdtAdapter {
  return {
    setDocId: (docId) => treecrdtSetDocId(runner, docId),
    docId: () => treecrdtDocId(runner),
    opRefsAll: () => treecrdtOpRefsAll(runner),
    opRefsChildren: (parent) => treecrdtOpRefsChildren(runner, parent),
    opsByOpRefs: (opRefs) => treecrdtOpsByOpRefs(runner, opRefs),
    treeChildren: (parent) => treecrdtTreeChildren(runner, parent),
    treeDump: () => treecrdtTreeDump(runner),
    treeNodeCount: () => treecrdtTreeNodeCount(runner),
    headLamport: () => treecrdtHeadLamport(runner),
    replicaMaxCounter: (replica) => treecrdtReplicaMaxCounter(runner, replica),
    appendOp: (op, serializeNodeId, serializeReplica) =>
      treecrdtAppendOp(runner, op, serializeNodeId, serializeReplica),
    appendOps: (ops, serializeNodeId, serializeReplica) =>
      treecrdtAppendOps(runner, ops, serializeNodeId, serializeReplica, opts),
    opsSince: (lamport, root) => treecrdtOpsSince(runner, { lamport, root }),
  };
}

/**
 * Set the document id used by the SQLite extension for v0 sync (`op_ref` derivation).
 *
 * This MUST be stable for the lifetime of the database, since it affects opRef hashes.
 */
async function treecrdtSetDocId(runner: SqliteRunner, docId: string): Promise<void> {
  await runner.getText("SELECT treecrdt_set_doc_id(?1)", [docId]);
}

async function treecrdtDocId(runner: SqliteRunner): Promise<string | null> {
  return runner.getText("SELECT treecrdt_doc_id()");
}

/**
 * Fetch all stored opRefs (16-byte values) from the extension.
 * Returns raw JSON-decoded values: `number[][]` (bytes) is the expected shape.
 */
async function treecrdtOpRefsAll(runner: SqliteRunner): Promise<unknown[]> {
  return sqliteGetJsonOrEmpty(runner, "SELECT treecrdt_oprefs_all()");
}

/**
 * Fetch opRefs relevant to the `children(parent)` filter from the extension.
 * Returns raw JSON-decoded values: `number[][]` (bytes) is the expected shape.
 */
async function treecrdtOpRefsChildren(runner: SqliteRunner, parent: Uint8Array): Promise<unknown[]> {
  return sqliteGetJsonOrEmpty(runner, "SELECT treecrdt_oprefs_children(?1)", [parent]);
}

/**
 * Fetch operations by opRef (16-byte values) from the extension.
 * Returns raw JSON-decoded operation rows (same shape as `treecrdtOpsSince`).
 */
async function treecrdtOpsByOpRefs(runner: SqliteRunner, opRefs: Uint8Array[]): Promise<unknown[]> {
  const payload = opRefs.map((r) => Array.from(r));
  return sqliteGetJsonOrEmpty(runner, "SELECT treecrdt_ops_by_oprefs(?1)", [JSON.stringify(payload)]);
}

/**
 * Fetch materialized children for a parent node (16-byte id).
 * Returns raw JSON-decoded values: `number[][]` (bytes) is the expected shape.
 */
async function treecrdtTreeChildren(runner: SqliteRunner, parent: Uint8Array): Promise<unknown[]> {
  return sqliteGetJsonOrEmpty(runner, "SELECT treecrdt_tree_children(?1)", [parent]);
}

/**
 * Dump the full materialized tree state.
 * Returns raw JSON-decoded rows (array of objects with byte fields).
 */
async function treecrdtTreeDump(runner: SqliteRunner): Promise<unknown[]> {
  return sqliteGetJsonOrEmpty(runner, "SELECT treecrdt_tree_dump()");
}

/**
 * Count non-tombstoned nodes in the materialized tree (excluding ROOT).
 */
async function treecrdtTreeNodeCount(runner: SqliteRunner): Promise<number> {
  return sqliteGetNumber(runner, "SELECT treecrdt_tree_node_count()");
}

/**
 * Fetch the maximum lamport seen in the op log.
 */
async function treecrdtHeadLamport(runner: SqliteRunner): Promise<number> {
  return sqliteGetNumber(runner, "SELECT treecrdt_head_lamport()");
}

/**
 * Fetch the maximum counter observed for a replica id.
 */
async function treecrdtReplicaMaxCounter(runner: SqliteRunner, replica: Uint8Array): Promise<number> {
  return sqliteGetNumber(runner, "SELECT treecrdt_replica_max_counter(?1)", [replica]);
}

// ---- Decoders for extension JSON payloads ----

export function decodeSqliteOpRefs(raw: unknown): Uint8Array[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((val) => (val instanceof Uint8Array ? val : Uint8Array.from(val as any)));
}

export function decodeSqliteNodeIds(raw: unknown): string[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((val) => decodeNodeId(val instanceof Uint8Array ? val : (val as any)));
}

export type SqliteTreeRow = {
  node: string;
  parent: string | null;
  pos: number | null;
  tombstone: boolean;
};

export function decodeSqliteTreeRows(raw: unknown): SqliteTreeRow[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((row: any) => {
    const node = decodeNodeId(row.node);
    const parent = row.parent ? decodeNodeId(row.parent) : null;
    const pos = row.pos === null || row.pos === undefined ? null : Number(row.pos);
    const tombstone = Boolean(row.tombstone);
    return { node, parent, pos, tombstone };
  });
}

export function decodeSqliteOps(raw: unknown): Operation[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((row: any) => {
    const replica = decodeReplicaId(row.replica);
    const counter = Number(row.counter);
    const lamport = Number(row.lamport);
    const knownState =
      row.known_state && (row.known_state instanceof Uint8Array ? row.known_state : Uint8Array.from(row.known_state));
    const base = { meta: { id: { replica, counter }, lamport, ...(knownState ? { knownState } : {}) } } as Operation;
    if (row.kind === "insert") {
      return {
        ...base,
        kind: {
          type: "insert",
          parent: decodeNodeId(row.parent),
          node: decodeNodeId(row.node),
          position: row.position === null || row.position === undefined ? 0 : Number(row.position),
        },
      } as Operation;
    }
    if (row.kind === "move") {
      return {
        ...base,
        kind: {
          type: "move",
          node: decodeNodeId(row.node),
          newParent: decodeNodeId(row.new_parent),
          position: row.position === null || row.position === undefined ? 0 : Number(row.position),
        },
      } as Operation;
    }
    if (row.kind === "delete") {
      return { ...base, kind: { type: "delete", node: decodeNodeId(row.node) } } as Operation;
    }
    return { ...base, kind: { type: "tombstone", node: decodeNodeId(row.node) } } as Operation;
  });
}

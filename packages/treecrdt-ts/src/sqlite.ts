import type { SerializeNodeId, SerializeReplica, TreecrdtAdapter } from "./adapter.js";
import {
  decodeNodeId,
  decodeReplicaId,
  hexToBytes,
  nodeIdToBytes16,
  ROOT_NODE_ID_HEX,
  replicaIdToBytes,
} from "./ids.js";
import type { Operation, OperationKind, ReplicaId } from "./index.js";

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

const ROOT_NODE_BYTES = nodeIdToBytes16(ROOT_NODE_ID_HEX);

function buildAppendOp(
  kind: OperationKind,
  opts: {
    replica: Uint8Array;
    counter: number;
    lamport: number;
    serializeNodeId: SerializeNodeId;
    knownState: Uint8Array | null;
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
      if (kind.payload !== undefined) {
        return {
          sql: "SELECT treecrdt_append_op(?1,?2,?3,?4,?5,?6,NULL,?7,?8)",
          params: [
            ...base,
            "insert",
            opts.serializeNodeId(kind.parent),
            opts.serializeNodeId(kind.node),
            kind.orderKey,
            kind.payload,
          ],
        };
      }
      return {
        sql: "SELECT treecrdt_append_op(?1,?2,?3,?4,?5,?6,NULL,?7,NULL)",
        params: [
          ...base,
          "insert",
          opts.serializeNodeId(kind.parent),
          opts.serializeNodeId(kind.node),
          kind.orderKey,
        ],
      };
    case "move":
      return {
        sql: "SELECT treecrdt_append_op(?1,?2,?3,?4,NULL,?5,?6,?7,NULL)",
        params: [
          ...base,
          "move",
          opts.serializeNodeId(kind.node),
          opts.serializeNodeId(kind.newParent),
          kind.orderKey,
        ],
      };
    case "delete":
      if (!opts.knownState || opts.knownState.length === 0) {
        throw new Error("treecrdt: delete operations require meta.knownState");
      }
      return {
        sql: "SELECT treecrdt_append_op(?1,?2,?3,?4,NULL,?5,NULL,NULL,?6)",
        params: [...base, "delete", opts.serializeNodeId(kind.node), opts.knownState],
      };
    case "tombstone":
      return {
        sql: "SELECT treecrdt_append_op(?1,?2,?3,?4,NULL,?5,NULL,NULL,?6)",
        params: [...base, "tombstone", opts.serializeNodeId(kind.node), opts.knownState],
      };
    case "payload":
      return {
        sql: "SELECT treecrdt_append_op(?1,?2,?3,?4,NULL,?5,NULL,NULL,?6)",
        params: [...base, "payload", opts.serializeNodeId(kind.node), kind.payload],
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
      order_key: "orderKey" in kind ? Array.from(kind.orderKey) : null,
      ...(knownState && knownState.length > 0 ? { known_state: Array.from(knownState) } : {}),
    };
    if (kind.type === "insert") {
      const payload = kind.payload ? Array.from(kind.payload) : undefined;
      return {
        ...base,
        parent: serialize(kind.parent),
        node: serialize(kind.node),
        new_parent: null,
        ...(payload ? { payload } : {}),
      };
    } else if (kind.type === "move") {
      return { ...base, parent: null, node: serialize(kind.node), new_parent: serialize(kind.newParent) };
    } else if (kind.type === "delete") {
      return { ...base, parent: null, node: serialize(kind.node), new_parent: null };
    } else if (kind.type === "payload") {
      return {
        ...base,
        parent: null,
        node: serialize(kind.node),
        new_parent: null,
        payload: kind.payload === null ? null : Array.from(kind.payload),
      };
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
  if (op.kind.type === "delete" && (!op.meta.knownState || op.meta.knownState.length === 0)) {
    throw new Error("treecrdt: delete operations require meta.knownState");
  }
  const { meta, kind } = op;
  const { id, lamport } = meta;
  const { replica, counter } = id;

  const { sql, params } = buildAppendOp(kind, {
    replica: serializeReplica(replica),
    counter,
    lamport,
    serializeNodeId,
    knownState: meta.knownState ?? null,
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

  if (
    ops.some((op) => op.kind.type === "delete" && (!op.meta.knownState || op.meta.knownState.length === 0))
  ) {
    throw new Error("treecrdt: delete operations require meta.knownState");
  }

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
    treeChildrenPage: (parent, cursor, limit) =>
      treecrdtTreeChildrenPage(runner, parent, cursor, limit),
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

async function treecrdtEnsureMaterialized(runner: SqliteRunner): Promise<void> {
  await runner.getText("SELECT treecrdt_ensure_materialized()");
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
 *
 * Implemented as direct SQL over `tree_nodes` (not a SQLite extension UDF), returning a JSON
 * array of canonical node id hex strings (32 chars).
 */
async function treecrdtTreeChildren(runner: SqliteRunner, parent: Uint8Array): Promise<unknown[]> {
  await treecrdtEnsureMaterialized(runner);
  return sqliteGetJsonOrEmpty(
    runner,
    "SELECT COALESCE(json_group_array(node_hex), '[]') FROM (\
     SELECT lower(hex(node)) AS node_hex \
     FROM tree_nodes \
     WHERE parent = ?1 AND tombstone = 0 \
     ORDER BY order_key, node\
     )",
    [parent]
  );
}

/**
 * Fetch a page of materialized children for `parent`, including ordering keys.
 *
 * Use `(order_key, node)` as a keyset pagination cursor.
 */
async function treecrdtTreeChildrenPage(
  runner: SqliteRunner,
  parent: Uint8Array,
  cursor: { orderKey: Uint8Array; node: Uint8Array } | null,
  limit: number
): Promise<unknown[]> {
  await treecrdtEnsureMaterialized(runner);
  const afterOrderKey = cursor?.orderKey ?? null;
  const afterNode = cursor?.node ?? null;
  return sqliteGetJsonOrEmpty(
    runner,
    "SELECT COALESCE(json_group_array(json_object('node', node_hex, 'order_key', order_key_hex)), '[]') \
     FROM (\
       SELECT \
         lower(hex(node)) AS node_hex, \
         CASE WHEN order_key IS NULL THEN NULL ELSE lower(hex(order_key)) END AS order_key_hex, \
         order_key, \
         node \
       FROM tree_nodes \
       WHERE parent = ?1 AND tombstone = 0 \
         AND (?2 IS NULL OR (order_key > ?2 OR (order_key = ?2 AND node > ?3))) \
       ORDER BY order_key, node \
       LIMIT ?4\
     )",
    [parent, afterOrderKey, afterNode, limit]
  );
}

/**
 * Dump the full materialized tree state.
 * Returns raw JSON-decoded rows (array of objects with byte fields).
 */
async function treecrdtTreeDump(runner: SqliteRunner): Promise<unknown[]> {
  await treecrdtEnsureMaterialized(runner);
  return sqliteGetJsonOrEmpty(
    runner,
    "SELECT COALESCE(json_group_array(json_object('node', node_hex, 'parent', parent_hex, 'order_key', order_key_hex, 'tombstone', tombstone)), '[]') \
     FROM (\
       SELECT \
         lower(hex(node)) AS node_hex, \
         CASE WHEN parent IS NULL THEN NULL ELSE lower(hex(parent)) END AS parent_hex, \
         CASE WHEN order_key IS NULL THEN NULL ELSE lower(hex(order_key)) END AS order_key_hex, \
         tombstone \
       FROM tree_nodes \
       ORDER BY node\
     )"
  );
}

/**
 * Count non-tombstoned nodes in the materialized tree (excluding ROOT).
 */
async function treecrdtTreeNodeCount(runner: SqliteRunner): Promise<number> {
  await treecrdtEnsureMaterialized(runner);
  return sqliteGetNumber(
    runner,
    "SELECT COUNT(*) FROM tree_nodes WHERE tombstone = 0 AND node <> ?1",
    [ROOT_NODE_BYTES]
  );
}

/**
 * Fetch the maximum lamport seen in the op log.
 */
async function treecrdtHeadLamport(runner: SqliteRunner): Promise<number> {
  return sqliteGetNumber(runner, "SELECT COALESCE(MAX(lamport), 0) FROM ops");
}

/**
 * Fetch the maximum counter observed for a replica id.
 */
async function treecrdtReplicaMaxCounter(runner: SqliteRunner, replica: Uint8Array): Promise<number> {
  return sqliteGetNumber(runner, "SELECT COALESCE(MAX(counter), 0) FROM ops WHERE replica = ?1", [replica]);
}

export type TreecrdtSqlitePlacement =
  | { type: "first" }
  | { type: "last" }
  | { type: "after"; after: string };

export type TreecrdtSqliteWriter = {
  insert: (
    parent: string,
    node: string,
    placement: TreecrdtSqlitePlacement,
    opts?: { payload?: Uint8Array }
  ) => Promise<Operation>;
  move: (node: string, newParent: string, placement: TreecrdtSqlitePlacement) => Promise<Operation>;
  delete: (node: string) => Promise<Operation>;
  payload: (node: string, payload: Uint8Array | null) => Promise<Operation>;
};

export function createTreecrdtSqliteWriter(runner: SqliteRunner, opts: { replica: ReplicaId }): TreecrdtSqliteWriter {
  const replica = opts.replica;
  const replicaBytes = replicaIdToBytes(replica);

  const getLocalOp = async (sql: string, params: SqlCall["params"]) => {
    const raw = await sqliteGetJson<unknown[]>(runner, sql, params);
    const ops = decodeSqliteOps(raw);
    if (ops.length !== 1) throw new Error(`expected exactly 1 op from query: ${sql}`);
    return ops[0]!;
  };

  const insert = async (
    parent: string,
    node: string,
    placement: TreecrdtSqlitePlacement,
    o: { payload?: Uint8Array } = {}
  ) => {
    const afterNode = placement.type === "after" ? nodeIdToBytes16(placement.after) : null;
    const payload = o.payload ?? null;
    return getLocalOp("SELECT treecrdt_local_insert(?1,?2,?3,?4,?5,?6)", [
      replicaBytes,
      nodeIdToBytes16(parent),
      nodeIdToBytes16(node),
      placement.type,
      afterNode,
      payload,
    ]);
  };

  const move = async (node: string, newParent: string, placement: TreecrdtSqlitePlacement) => {
    const afterNode = placement.type === "after" ? nodeIdToBytes16(placement.after) : null;
    return getLocalOp("SELECT treecrdt_local_move(?1,?2,?3,?4,?5)", [
      replicaBytes,
      nodeIdToBytes16(node),
      nodeIdToBytes16(newParent),
      placement.type,
      afterNode,
    ]);
  };

  const del = async (node: string) => {
    return getLocalOp("SELECT treecrdt_local_delete(?1,?2)", [replicaBytes, nodeIdToBytes16(node)]);
  };

  const payload = async (node: string, next: Uint8Array | null) => {
    return getLocalOp("SELECT treecrdt_local_payload(?1,?2,?3)", [
      replicaBytes,
      nodeIdToBytes16(node),
      next,
    ]);
  };

  return { insert, move, delete: del, payload };
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

export type SqliteTreeChildRow = {
  node: string;
  orderKey: Uint8Array | null;
};

export function decodeSqliteTreeChildRows(raw: unknown): SqliteTreeChildRow[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((row: any) => {
    const node = decodeNodeId(row.node);
    const rawOrderKey = row.order_key;
    const orderKey =
      rawOrderKey === null || rawOrderKey === undefined
        ? null
        : typeof rawOrderKey === "string"
          ? hexToBytes(rawOrderKey)
        : rawOrderKey instanceof Uint8Array
          ? rawOrderKey
          : Uint8Array.from(rawOrderKey as any);
    return { node, orderKey };
  });
}

export type SqliteTreeRow = {
  node: string;
  parent: string | null;
  orderKey: Uint8Array | null;
  tombstone: boolean;
};

export function decodeSqliteTreeRows(raw: unknown): SqliteTreeRow[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((row: any) => {
    const node = decodeNodeId(row.node);
    const parent = row.parent ? decodeNodeId(row.parent) : null;
    const rawOrderKey = row.order_key;
    const orderKey =
      rawOrderKey === null || rawOrderKey === undefined
        ? null
        : typeof rawOrderKey === "string"
          ? hexToBytes(rawOrderKey)
        : rawOrderKey instanceof Uint8Array
          ? rawOrderKey
          : Uint8Array.from(rawOrderKey as any);
    const tombstone = Boolean(row.tombstone);
    return { node, parent, orderKey, tombstone };
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
      const rawPayload = row.payload;
      const payload =
        rawPayload === null || rawPayload === undefined
          ? undefined
          : rawPayload instanceof Uint8Array
            ? rawPayload
            : Uint8Array.from(rawPayload as any);
      const rawOrderKey = row.order_key;
      if (rawOrderKey === null || rawOrderKey === undefined) {
        throw new Error("missing order_key for insert op from sqlite");
      }
      const orderKey = rawOrderKey instanceof Uint8Array ? rawOrderKey : Uint8Array.from(rawOrderKey as any);
      return {
        ...base,
        kind: {
          type: "insert",
          parent: decodeNodeId(row.parent),
          node: decodeNodeId(row.node),
          orderKey,
          ...(payload !== undefined ? { payload } : {}),
        },
      } as Operation;
    }
    if (row.kind === "move") {
      const rawOrderKey = row.order_key;
      if (rawOrderKey === null || rawOrderKey === undefined) {
        throw new Error("missing order_key for move op from sqlite");
      }
      const orderKey = rawOrderKey instanceof Uint8Array ? rawOrderKey : Uint8Array.from(rawOrderKey as any);
      return {
        ...base,
        kind: {
          type: "move",
          node: decodeNodeId(row.node),
          newParent: decodeNodeId(row.new_parent),
          orderKey,
        },
      } as Operation;
    }
    if (row.kind === "delete") {
      return { ...base, kind: { type: "delete", node: decodeNodeId(row.node) } } as Operation;
    }
    if (row.kind === "tombstone") {
      return { ...base, kind: { type: "tombstone", node: decodeNodeId(row.node) } } as Operation;
    }
    if (row.kind === "payload") {
      const rawPayload = row.payload;
      if (rawPayload === null || rawPayload === undefined) {
        return { ...base, kind: { type: "payload", node: decodeNodeId(row.node), payload: null } } as Operation;
      }
      const bytes = rawPayload instanceof Uint8Array ? rawPayload : Uint8Array.from(rawPayload as any);
      return { ...base, kind: { type: "payload", node: decodeNodeId(row.node), payload: bytes } } as Operation;
    }
    throw new Error(`unknown op kind from sqlite: ${String(row.kind)}`);
  });
}

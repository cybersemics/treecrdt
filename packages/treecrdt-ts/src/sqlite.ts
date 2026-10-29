import { encode } from 'cborg';
import type {
  SerializeNodeId,
  SerializeReplica,
  TreecrdtAdapter,
  TreecrdtChangedSinceQuery,
} from './adapter.js';
import { addMaterializationWriteId, emptyMaterializationOutcome } from './engine.js';
import type {
  LocalWriteOptions,
  MaterializationEvent,
  MaterializationOutcome,
  MaterializationSource,
  TreecrdtChangedNode,
  TreecrdtEngineTree,
} from './engine.js';
import {
  decodeNodeId,
  decodeReplicaId,
  hexToBytes,
  nodeIdToBytes16,
  ROOT_NODE_ID_HEX,
  replicaIdToBytes,
} from './ids.js';
import type { Operation, OperationKind, ReplicaId } from './index.js';

export type SqlCall = {
  sql: string;
  params: (number | string | Uint8Array | null)[];
};

export type SqliteRunner = {
  exec: (sql: string) => Promise<void> | void;
  getText: (sql: string, params?: SqlCall['params']) => Promise<string | null> | string | null;
};

async function sqliteGetJson<T>(
  runner: SqliteRunner,
  sql: string,
  params?: SqlCall['params'],
): Promise<T> {
  const text = await runner.getText(sql, params);
  if (!text) throw new Error(`expected JSON result for query: ${sql}`);
  return JSON.parse(text) as T;
}

async function sqliteGetJsonOrEmpty<T>(
  runner: SqliteRunner,
  sql: string,
  params?: SqlCall['params'],
): Promise<T> {
  const text = await runner.getText(sql, params);
  if (!text) return JSON.parse('[]') as T;
  return JSON.parse(text) as T;
}

async function sqliteGetNumber(
  runner: SqliteRunner,
  sql: string,
  params?: SqlCall['params'],
): Promise<number> {
  const text = await runner.getText(sql, params);
  if (!text) return 0;
  const value = Number(text);
  if (!Number.isFinite(value)) throw new Error(`expected numeric result for query: ${sql}`);
  return value;
}

async function sqliteExec(runner: SqliteRunner, sql: string): Promise<void> {
  await runner.exec(sql);
}

let localAuthSavepointCounter = 0;

function decodeLocalOpResult(
  raw: any,
  sql: string,
): { op: Operation; outcome: MaterializationOutcome } {
  const ops = decodeSqliteOps([raw.op]);
  if (ops.length !== 1) throw new Error(`expected exactly 1 op from query: ${sql}`);
  return {
    op: ops[0]!,
    outcome: decodeSqliteMaterializationOutcome(raw.outcome),
  };
}

function emitLocalOutcome(
  outcome: MaterializationOutcome,
  emit?: (event: MaterializationEvent) => void,
  writeId?: string,
): void {
  if (outcome.changes.length > 0) emit?.(addMaterializationWriteId(outcome, writeId));
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
  },
): SqlCall {
  const base = [opts.replica, opts.counter, opts.lamport] as (
    | number
    | string
    | Uint8Array
    | null
  )[];

  switch (kind.type) {
    case 'insert':
      if (kind.payload !== undefined) {
        return {
          sql: 'SELECT treecrdt_append_op(?1,?2,?3,?4,?5,?6,NULL,?7,?8)',
          params: [
            ...base,
            'insert',
            opts.serializeNodeId(kind.parent),
            opts.serializeNodeId(kind.node),
            kind.orderKey,
            kind.payload,
          ],
        };
      }
      return {
        sql: 'SELECT treecrdt_append_op(?1,?2,?3,?4,?5,?6,NULL,?7,NULL)',
        params: [
          ...base,
          'insert',
          opts.serializeNodeId(kind.parent),
          opts.serializeNodeId(kind.node),
          kind.orderKey,
        ],
      };
    case 'move':
      return {
        sql: 'SELECT treecrdt_append_op(?1,?2,?3,?4,NULL,?5,?6,?7,NULL)',
        params: [
          ...base,
          'move',
          opts.serializeNodeId(kind.node),
          opts.serializeNodeId(kind.newParent),
          kind.orderKey,
        ],
      };
    case 'delete':
      if (!opts.knownState || opts.knownState.length === 0) {
        throw new Error('treecrdt: delete operations require meta.knownState');
      }
      return {
        sql: 'SELECT treecrdt_append_op(?1,?2,?3,?4,NULL,?5,NULL,NULL,?6)',
        params: [...base, 'delete', opts.serializeNodeId(kind.node), opts.knownState],
      };
    case 'tombstone':
      return {
        sql: 'SELECT treecrdt_append_op(?1,?2,?3,?4,NULL,?5,NULL,NULL,?6)',
        params: [...base, 'tombstone', opts.serializeNodeId(kind.node), opts.knownState],
      };
    case 'payload':
      return {
        sql: 'SELECT treecrdt_append_op(?1,?2,?3,?4,NULL,?5,NULL,NULL,?6)',
        params: [...base, 'payload', opts.serializeNodeId(kind.node), kind.payload],
      };
    default:
      throw new Error('unsupported operation kind');
  }
}

function buildOpsSince(filter: {
  lamport: number;
  root?: string;
  serializeNodeId: SerializeNodeId;
}): SqlCall {
  if (filter.root === undefined) {
    return { sql: 'SELECT treecrdt_ops_since(?1)', params: [filter.lamport] };
  }
  return {
    sql: 'SELECT treecrdt_ops_since(?1, ?2)',
    params: [filter.lamport, filter.serializeNodeId(filter.root)],
  };
}

function buildAppendOpsPayload(
  ops: Operation[],
  serializeNodeId: SerializeNodeId,
  serializeReplica: SerializeReplica,
): unknown[] {
  const serialize = (val: string) => new Uint8Array(serializeNodeId(val));
  return ops.map((op) => {
    const { meta, kind } = op;
    const { id, lamport } = meta;
    const { replica, counter } = id;
    const knownState = meta.knownState;
    const base = {
      replica: new Uint8Array(serializeReplica(replica)),
      counter,
      lamport,
      kind: kind.type,
      order_key: 'orderKey' in kind ? kind.orderKey : null,
      ...(knownState && knownState.length > 0 ? { known_state: knownState } : {}),
    };
    if (kind.type === 'insert') {
      return {
        ...base,
        parent: serialize(kind.parent),
        node: serialize(kind.node),
        new_parent: null,
        ...(kind.payload ? { payload: kind.payload } : {}),
      };
    } else if (kind.type === 'move') {
      return {
        ...base,
        parent: null,
        node: serialize(kind.node),
        new_parent: serialize(kind.newParent),
      };
    } else if (kind.type === 'delete') {
      return { ...base, parent: null, node: serialize(kind.node), new_parent: null };
    } else if (kind.type === 'payload') {
      if (kind.payload === undefined) {
        throw new Error('treecrdt: payload operations require payload or null');
      }
      return {
        ...base,
        parent: null,
        node: serialize(kind.node),
        new_parent: null,
        payload: kind.payload,
      };
    }
    return { ...base, parent: null, node: serialize(kind.node), new_parent: null };
  });
}

async function treecrdtAppendOp(
  runner: SqliteRunner,
  op: Operation,
  serializeNodeId: SerializeNodeId,
  serializeReplica: SerializeReplica,
): Promise<MaterializationOutcome> {
  if (op.kind.type === 'delete' && (!op.meta.knownState || op.meta.knownState.length === 0)) {
    throw new Error('treecrdt: delete operations require meta.knownState');
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

  return decodeSqliteMaterializationOutcome(await sqliteGetJson<unknown>(runner, sql, params));
}

function mergeMaterializationOutcomes(outcomes: MaterializationOutcome[]): MaterializationOutcome {
  const last = outcomes.length > 0 ? outcomes[outcomes.length - 1] : undefined;
  return {
    headSeq: last?.headSeq ?? 0,
    changes: outcomes.flatMap((outcome) => outcome.changes),
  };
}

async function treecrdtAppendOps(
  runner: SqliteRunner,
  ops: Operation[],
  serializeNodeId: SerializeNodeId,
  serializeReplica: SerializeReplica,
  opts: { maxBulkOps?: number } = {},
): Promise<MaterializationOutcome> {
  if (ops.length === 0) return emptyMaterializationOutcome();

  const maxBulkOps = opts.maxBulkOps ?? 5_000;
  const bulkSql = 'SELECT treecrdt_append_ops(?1)';

  if (
    ops.some(
      (op) => op.kind.type === 'delete' && (!op.meta.knownState || op.meta.knownState.length === 0),
    )
  ) {
    throw new Error('treecrdt: delete operations require meta.knownState');
  }

  // Try bulk entrypoint first, chunked to bound the encoded batch size.
  let bulkFailedAt: number | null = null;
  const outcomes: MaterializationOutcome[] = [];
  for (let start = 0; start < ops.length; start += maxBulkOps) {
    const chunk = ops.slice(start, start + maxBulkOps);
    const payload = buildAppendOpsPayload(chunk, serializeNodeId, serializeReplica);
    try {
      const raw = await sqliteGetJson<unknown>(runner, bulkSql, [encode(payload)]);
      outcomes.push(decodeSqliteMaterializationOutcome(raw));
    } catch {
      bulkFailedAt = start;
      break;
    }
  }
  if (bulkFailedAt === null) return mergeMaterializationOutcomes(outcomes);

  const remaining = ops.slice(bulkFailedAt);
  for (const op of remaining) {
    const payload = buildAppendOpsPayload([op], serializeNodeId, serializeReplica);
    const raw = await sqliteGetJson<unknown>(runner, bulkSql, [encode(payload)]);
    outcomes.push(decodeSqliteMaterializationOutcome(raw));
  }
  return mergeMaterializationOutcomes(outcomes);
}

async function treecrdtOpsSince(
  runner: SqliteRunner,
  filter: { lamport: number; root?: string },
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
  opts: { maxBulkOps?: number; onMaterialized?: (event: MaterializationEvent) => void } = {},
): TreecrdtAdapter {
  const emitOutcome = (outcome: MaterializationOutcome) => {
    if (outcome.changes.length === 0) return;
    opts.onMaterialized?.({ ...outcome });
  };
  return {
    setDocId: (docId) => treecrdtSetDocId(runner, docId),
    docId: () => treecrdtDocId(runner),
    opRefsAll: () => treecrdtOpRefsAll(runner, emitOutcome),
    opRefsChildren: (parent) => treecrdtOpRefsChildren(runner, parent, emitOutcome),
    opsByOpRefs: (opRefs) => treecrdtOpsByOpRefs(runner, opRefs),
    treeChildren: (parent, offset, limit) =>
      treecrdtTreeChildren(runner, parent, offset, limit, emitOutcome),
    treeDump: () => treecrdtTreeDump(runner, emitOutcome),
    treeChangedSince: (query) => treecrdtTreeChangedSince(runner, query, emitOutcome),
    treeNodeCount: () => treecrdtTreeNodeCount(runner, emitOutcome),
    treeParent: (node) => treecrdtTreeParent(runner, node, emitOutcome),
    treeExists: (node) => treecrdtTreeExists(runner, node, emitOutcome),
    treePayload: (node) => treecrdtTreePayload(runner, node, emitOutcome),
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
  await runner.getText('SELECT treecrdt_set_doc_id(?1)', [docId]);
}

async function treecrdtDocId(runner: SqliteRunner): Promise<string | null> {
  return runner.getText('SELECT treecrdt_doc_id()');
}

async function treecrdtEnsureMaterialized(
  runner: SqliteRunner,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<MaterializationOutcome> {
  const outcome = decodeSqliteMaterializationOutcome(
    await sqliteGetJson<unknown>(runner, 'SELECT treecrdt_ensure_materialized()'),
  );
  if (outcome.changes.length > 0) emitOutcome?.(outcome);
  return outcome;
}

/**
 * Fetch all stored opRefs (16-byte values) from the extension.
 * Returns raw JSON-decoded values: `number[][]` (bytes) is the expected shape.
 */
async function treecrdtOpRefsAll(
  runner: SqliteRunner,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<unknown[]> {
  await treecrdtEnsureMaterialized(runner, emitOutcome);
  return sqliteGetJsonOrEmpty(runner, 'SELECT treecrdt_oprefs_all()');
}

/**
 * Fetch opRefs relevant to the `children(parent)` filter from the extension.
 * Returns raw JSON-decoded values: `number[][]` (bytes) is the expected shape.
 */
async function treecrdtOpRefsChildren(
  runner: SqliteRunner,
  parent: Uint8Array,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<unknown[]> {
  await treecrdtEnsureMaterialized(runner, emitOutcome);
  return sqliteGetJsonOrEmpty(runner, 'SELECT treecrdt_oprefs_children(?1)', [parent]);
}

/**
 * Fetch operations by opRef (16-byte values) from the extension.
 * Returns raw JSON-decoded operation rows (same shape as `treecrdtOpsSince`).
 */
async function treecrdtOpsByOpRefs(runner: SqliteRunner, opRefs: Uint8Array[]): Promise<unknown[]> {
  const payload = opRefs.map((r) => Array.from(r));
  return sqliteGetJsonOrEmpty(runner, 'SELECT treecrdt_ops_by_oprefs(?1)', [
    JSON.stringify(payload),
  ]);
}

/**
 * Fetch materialized children for a parent node (16-byte id).
 *
 * Implemented as direct SQL over `tree_nodes` (not a SQLite extension UDF), returning a JSON
 * array of canonical node id hex strings (32 chars).
 */
async function treecrdtTreeChildren(
  runner: SqliteRunner,
  parent: Uint8Array,
  offset: number | null,
  limit: number | null,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<unknown[]> {
  await treecrdtEnsureMaterialized(runner, emitOutcome);
  const baseSelect =
    'SELECT lower(hex(node)) AS node_hex FROM tree_nodes WHERE parent = ?1 AND tombstone = 0 ORDER BY order_key, node';
  if (offset === null && limit === null) {
    return sqliteGetJsonOrEmpty(
      runner,
      "SELECT COALESCE(json_group_array(node_hex), '[]') FROM (\
       SELECT lower(hex(node)) AS node_hex \
       FROM tree_nodes \
       WHERE parent = ?1 AND tombstone = 0 \
       ORDER BY order_key, node\
       )",
      [parent],
    );
  }
  if (limit === null) {
    return sqliteGetJsonOrEmpty(
      runner,
      `SELECT COALESCE(json_group_array(node_hex), '[]') FROM (${baseSelect} LIMIT -1 OFFSET ?2)`,
      [parent, offset],
    );
  }
  return sqliteGetJsonOrEmpty(
    runner,
    `SELECT COALESCE(json_group_array(node_hex), '[]') FROM (${baseSelect} LIMIT ?3 OFFSET ?2)`,
    [parent, offset, limit],
  );
}

/**
 * Dump the full materialized tree state.
 * Returns raw JSON-decoded rows (array of objects with byte fields).
 */
async function treecrdtTreeDump(
  runner: SqliteRunner,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<unknown[]> {
  await treecrdtEnsureMaterialized(runner, emitOutcome);
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
     )",
  );
}

/**
 * Read nodes whose visible state changed in `(since, until]`.
 *
 * The first page fixes `until` at the current head, so a write between pages surfaces in the next read
 * instead of being skipped. A full read (`since = 0`) pages over every materialized node by id; an
 * incremental read pages over `tree_changes` by `(seq, node)` and also reports nodes a rebuild left
 * without a materialized row.
 */
async function treecrdtTreeChangedSince(
  runner: SqliteRunner,
  query: TreecrdtChangedSinceQuery,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<{ headSeq: number; rows: unknown[] }> {
  const outcome = await treecrdtEnsureMaterialized(runner, emitOutcome);
  const headSeq = query.until ?? outcome.headSeq;
  const limit = query.limit ?? -1;
  const payloads = query.payloads ? 1 : 0;
  if (query.since === 0) {
    const rows = await sqliteGetJsonOrEmpty<unknown[]>(
      runner,
      "SELECT COALESCE(json_group_array(json_array(node_hex, parent_hex, order_key_hex, tombstone, seq, payload_hex)), '[]') \
       FROM (\
         SELECT \
           lower(hex(n.node)) AS node_hex, \
           CASE WHEN n.parent IS NULL THEN NULL ELSE lower(hex(n.parent)) END AS parent_hex, \
           CASE WHEN n.order_key IS NULL THEN NULL ELSE lower(hex(n.order_key)) END AS order_key_hex, \
           n.tombstone AS tombstone, \
           COALESCE(c.seq, 0) AS seq, \
           CASE WHEN ?4 = 1 AND n.tombstone = 0 AND p.payload IS NOT NULL THEN lower(hex(p.payload)) END AS payload_hex \
         FROM tree_nodes n \
         LEFT JOIN tree_changes c ON c.node = n.node \
         LEFT JOIN tree_payload p ON p.node = n.node \
         WHERE COALESCE(c.seq, 0) <= ?1 AND (?2 IS NULL OR n.node > ?2) \
         ORDER BY n.node \
         LIMIT ?3\
       )",
      [headSeq, query.afterNode, limit, payloads],
    );
    return { headSeq, rows };
  }
  const rows = await sqliteGetJsonOrEmpty<unknown[]>(
    runner,
    "SELECT COALESCE(json_group_array(json_array(node_hex, parent_hex, order_key_hex, tombstone, seq, payload_hex)), '[]') \
     FROM (\
       SELECT \
         lower(hex(c.node)) AS node_hex, \
         CASE WHEN n.parent IS NULL THEN NULL ELSE lower(hex(n.parent)) END AS parent_hex, \
         CASE WHEN n.order_key IS NULL THEN NULL ELSE lower(hex(n.order_key)) END AS order_key_hex, \
         COALESCE(n.tombstone, 1) AS tombstone, \
         c.seq AS seq, \
         CASE WHEN ?5 = 1 AND n.tombstone = 0 AND p.payload IS NOT NULL THEN lower(hex(p.payload)) END AS payload_hex \
       FROM tree_changes c \
       LEFT JOIN tree_nodes n ON n.node = c.node \
       LEFT JOIN tree_payload p ON p.node = c.node \
       WHERE c.seq <= ?1 AND (c.seq > ?2 OR (?3 IS NOT NULL AND c.seq = ?2 AND c.node > ?3)) \
       ORDER BY c.seq, c.node \
       LIMIT ?4\
     )",
    [headSeq, query.afterSeq, query.afterNode, limit, payloads],
  );
  return { headSeq, rows };
}

/**
 * Count non-tombstoned nodes in the materialized tree (excluding ROOT).
 */
async function treecrdtTreeNodeCount(
  runner: SqliteRunner,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<number> {
  await treecrdtEnsureMaterialized(runner, emitOutcome);
  return sqliteGetNumber(
    runner,
    'SELECT COUNT(*) FROM tree_nodes WHERE tombstone = 0 AND node <> ?1',
    [ROOT_NODE_BYTES],
  );
}

/**
 * Fetch the parent of a node (16-byte id).
 * Returns the live parent node id as bytes, or null if the node or parent is not visible.
 */
async function treecrdtTreeParent(
  runner: SqliteRunner,
  node: Uint8Array,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<Uint8Array | null> {
  await treecrdtEnsureMaterialized(runner, emitOutcome);
  const parentHex = await runner.getText(
    'SELECT lower(hex(parent.node)) \
     FROM tree_nodes AS child \
     JOIN tree_nodes AS parent ON parent.node = child.parent AND parent.tombstone = 0 \
     WHERE child.node = ?1 AND child.tombstone = 0',
    [node],
  );
  if (!parentHex) return null;
  return hexToBytes(parentHex);
}

/**
 * Check if a non-tombstoned node exists (16-byte id).
 */
async function treecrdtTreeExists(
  runner: SqliteRunner,
  node: Uint8Array,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<boolean> {
  await treecrdtEnsureMaterialized(runner, emitOutcome);
  const result = await runner.getText(
    'SELECT 1 FROM tree_nodes WHERE node = ?1 AND tombstone = 0 LIMIT 1',
    [node],
  );
  return result !== null;
}

/**
 * Fetch the payload for a node (16-byte id).
 * Returns the opaque byte payload or null if the node has no payload.
 */
async function treecrdtTreePayload(
  runner: SqliteRunner,
  node: Uint8Array,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<Uint8Array | null> {
  await treecrdtEnsureMaterialized(runner, emitOutcome);
  const hex = await runner.getText(
    'SELECT hex(payload) FROM tree_payload WHERE node = ?1 LIMIT 1',
    [node],
  );
  if (hex === null || hex === undefined || hex === '') return null;
  return hexToBytes(hex);
}

/**
 * Fetch the maximum lamport seen in the op log.
 */
async function treecrdtHeadLamport(runner: SqliteRunner): Promise<number> {
  return sqliteGetNumber(runner, 'SELECT COALESCE(MAX(lamport), 0) FROM ops');
}

/**
 * Fetch the maximum counter observed for a replica id.
 */
async function treecrdtReplicaMaxCounter(
  runner: SqliteRunner,
  replica: Uint8Array,
): Promise<number> {
  return sqliteGetNumber(runner, 'SELECT COALESCE(MAX(counter), 0) FROM ops WHERE replica = ?1', [
    replica,
  ]);
}

export type TreecrdtSqlitePlacement =
  | { type: 'first' }
  | { type: 'last' }
  | { type: 'after'; after: string };

export type TreecrdtSqliteWriter = {
  insert: (
    parent: string,
    node: string,
    placement: TreecrdtSqlitePlacement,
    opts?: { payload?: Uint8Array } & LocalWriteOptions,
  ) => Promise<Operation>;
  move: (
    node: string,
    newParent: string,
    placement: TreecrdtSqlitePlacement,
    opts?: LocalWriteOptions,
  ) => Promise<Operation>;
  delete: (node: string, opts?: LocalWriteOptions) => Promise<Operation>;
  payload: (
    node: string,
    payload: Uint8Array | null,
    opts?: LocalWriteOptions,
  ) => Promise<Operation>;
};

export function createTreecrdtSqliteWriter(
  runner: SqliteRunner,
  opts: {
    replica: ReplicaId;
    onMaterialized?: (event: MaterializationEvent) => void;
  },
): TreecrdtSqliteWriter {
  const replica = opts.replica;
  const replicaBytes = replicaIdToBytes(replica);

  const getLocalOp = async (
    sql: string,
    params: SqlCall['params'],
    writeOpts?: LocalWriteOptions,
  ) => {
    const authSession = writeOpts?.authSession;
    if (!authSession) {
      const { op, outcome } = decodeLocalOpResult(
        await sqliteGetJson<any>(runner, sql, params),
        sql,
      );
      emitLocalOutcome(outcome, opts.onMaterialized, writeOpts?.writeId);
      return op;
    }

    const savepoint = `treecrdt_local_auth_${++localAuthSavepointCounter}`;
    let released = false;
    await sqliteExec(runner, `SAVEPOINT ${savepoint}`);
    try {
      const { op, outcome } = decodeLocalOpResult(
        await sqliteGetJson<any>(runner, sql, params),
        sql,
      );
      await authSession.authorizeLocalOps([op]);
      await sqliteExec(runner, `RELEASE ${savepoint}`);
      released = true;
      emitLocalOutcome(outcome, opts.onMaterialized, writeOpts?.writeId);
      return op;
    } catch (err) {
      if (!released) {
        try {
          await sqliteExec(runner, `ROLLBACK TO ${savepoint}`);
        } finally {
          await sqliteExec(runner, `RELEASE ${savepoint}`);
        }
      }
      throw err;
    }
  };

  const insert = async (
    parent: string,
    node: string,
    placement: TreecrdtSqlitePlacement,
    o: { payload?: Uint8Array } & LocalWriteOptions = {},
  ) => {
    const afterNode = placement.type === 'after' ? nodeIdToBytes16(placement.after) : null;
    const payload = o.payload ?? null;
    return getLocalOp(
      'SELECT treecrdt_local_insert(?1,?2,?3,?4,?5,?6)',
      [
        replicaBytes,
        nodeIdToBytes16(parent),
        nodeIdToBytes16(node),
        placement.type,
        afterNode,
        payload,
      ],
      o,
    );
  };

  const move = async (
    node: string,
    newParent: string,
    placement: TreecrdtSqlitePlacement,
    writeOpts?: LocalWriteOptions,
  ) => {
    const afterNode = placement.type === 'after' ? nodeIdToBytes16(placement.after) : null;
    return getLocalOp(
      'SELECT treecrdt_local_move(?1,?2,?3,?4,?5)',
      [replicaBytes, nodeIdToBytes16(node), nodeIdToBytes16(newParent), placement.type, afterNode],
      writeOpts,
    );
  };

  const del = async (node: string, writeOpts?: LocalWriteOptions) => {
    return getLocalOp(
      'SELECT treecrdt_local_delete(?1,?2)',
      [replicaBytes, nodeIdToBytes16(node)],
      writeOpts,
    );
  };

  const payload = async (node: string, next: Uint8Array | null, writeOpts?: LocalWriteOptions) => {
    return getLocalOp(
      'SELECT treecrdt_local_payload(?1,?2,?3)',
      [replicaBytes, nodeIdToBytes16(node), next],
      writeOpts,
    );
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

function decodeSqlitePayload(raw: unknown): Uint8Array | null {
  if (raw == null) return null;
  if (raw instanceof Uint8Array) return raw;
  if (typeof raw === 'string') return hexToBytes(raw);
  return Uint8Array.from(raw as any);
}

function decodeSqliteMaterializationSource(raw: unknown): MaterializationSource | undefined {
  if (raw == null) return undefined;
  const value = raw as any;
  const operation = value.operation as any;
  const operationId = operation?.id as any;
  const source: MaterializationSource = {
    ...(operation && operationId
      ? {
          operation: {
            id: {
              replica: decodeReplicaId(operationId.replica),
              counter: Number(operationId.counter),
            },
            lamport: Number(operation.lamport),
          },
        }
      : {}),
    ...(Array.isArray(value.writeIds) ? { writeIds: value.writeIds.map(String) } : {}),
    ...(value.signer?.publicKey
      ? { signer: { publicKey: Uint8Array.from(value.signer.publicKey) } }
      : {}),
  };

  // Avoid exposing `source: {}` when a backend emits an empty or unknown source object.
  const hasSource =
    source.operation !== undefined ||
    (source.writeIds?.length ?? 0) > 0 ||
    source.signer !== undefined;
  return hasSource ? source : undefined;
}

export function decodeSqliteMaterializationOutcome(raw: unknown): MaterializationOutcome {
  const value = (raw ?? {}) as any;
  const rawChanges = Array.isArray(value.changes) ? value.changes : [];
  const changes = rawChanges.map((change: any) => {
    const kind = String(change.kind);
    const source = decodeSqliteMaterializationSource(change.source);
    if (kind === 'insert') {
      return {
        kind,
        node: decodeNodeId(change.node),
        parentAfter: decodeNodeId(change.parentAfter),
        payload: decodeSqlitePayload(change.payload),
        ...(source ? { source } : {}),
      };
    }
    if (kind === 'move') {
      return {
        kind,
        node: decodeNodeId(change.node),
        parentBefore: change.parentBefore == null ? null : decodeNodeId(change.parentBefore),
        parentAfter: decodeNodeId(change.parentAfter),
        ...(source ? { source } : {}),
      };
    }
    if (kind === 'delete') {
      return {
        kind,
        node: decodeNodeId(change.node),
        parentBefore: change.parentBefore == null ? null : decodeNodeId(change.parentBefore),
        ...(source ? { source } : {}),
      };
    }
    if (kind === 'restore') {
      return {
        kind,
        node: decodeNodeId(change.node),
        parentAfter: change.parentAfter == null ? null : decodeNodeId(change.parentAfter),
        payload: decodeSqlitePayload(change.payload),
        ...(source ? { source } : {}),
      };
    }
    if (kind === 'payload') {
      return {
        kind,
        node: decodeNodeId(change.node),
        payload: decodeSqlitePayload(change.payload),
        ...(source ? { source } : {}),
      };
    }
    throw new Error(`unknown materialization change kind: ${kind}`);
  });
  return {
    headSeq: Number(value.headSeq ?? 0),
    changes,
  };
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
        : typeof rawOrderKey === 'string'
          ? hexToBytes(rawOrderKey)
          : rawOrderKey instanceof Uint8Array
            ? rawOrderKey
            : Uint8Array.from(rawOrderKey as any);
    const tombstone = Boolean(row.tombstone);
    return { node, parent, orderKey, tombstone };
  });
}

/** Decodes raw `treeChangedSince` rows; `payloads` mirrors the request. */
export function decodeSqliteChangedNodes(raw: unknown, payloads: boolean): TreecrdtChangedNode[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((row) => {
    const [node, parent, orderKey, tombstone, changedSeq, payload] = row as [
      string,
      string | null,
      string | null,
      number,
      number,
      string | null,
    ];
    return {
      node: decodeNodeId(node),
      parent: parent === null ? null : decodeNodeId(parent),
      orderKey: orderKey === null ? null : hexToBytes(orderKey),
      tombstone: Boolean(tombstone),
      changedSeq: Number(changedSeq),
      // Same as treePayload: an empty stored payload reads as null.
      ...(payloads
        ? { payload: payload === null || payload === '' ? null : hexToBytes(payload) }
        : {}),
    };
  });
}

/** Builds `tree.changedSince` over a raw adapter read, handling cursors and decoding. */
export function createTreecrdtSqliteChangedSince(
  read: (query: TreecrdtChangedSinceQuery) => Promise<{ headSeq: number; rows: unknown[] }>,
): NonNullable<TreecrdtEngineTree['changedSince']> {
  return async (since, opts = {}) => {
    const { limit } = opts;
    if (limit !== undefined && (!Number.isInteger(limit) || limit <= 0)) {
      throw new Error(`invalid changedSince limit: ${String(limit)}`);
    }
    const cursor = typeof since === 'number' ? null : since;
    const from = cursor ? cursor.since : (since as number);
    if (!Number.isInteger(from) || from < 0) {
      throw new Error(`invalid changedSince seq: ${String(from)}`);
    }
    const payloads = opts.payloads === true;
    const { headSeq, rows } = await read({
      since: from,
      until: cursor ? cursor.until : null,
      afterSeq: cursor ? cursor.afterSeq : from,
      afterNode: cursor ? nodeIdToBytes16(cursor.afterNode) : null,
      limit: limit ?? null,
      payloads,
    });
    const nodes = decodeSqliteChangedNodes(rows, payloads);
    const last = nodes[nodes.length - 1];
    const next =
      last && nodes.length === limit
        ? { since: from, until: headSeq, afterSeq: last.changedSeq, afterNode: last.node }
        : null;
    return { headSeq, nodes, next };
  };
}

export function decodeSqliteOps(raw: unknown): Operation[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((row: any) => {
    const replica = decodeReplicaId(row.replica);
    const counter = Number(row.counter);
    const lamport = Number(row.lamport);
    const knownState =
      row.known_state &&
      (row.known_state instanceof Uint8Array ? row.known_state : Uint8Array.from(row.known_state));
    const base = {
      meta: { id: { replica, counter }, lamport, ...(knownState ? { knownState } : {}) },
    } as Operation;
    if (row.kind === 'insert') {
      const rawPayload = row.payload;
      const payload =
        rawPayload === null || rawPayload === undefined
          ? undefined
          : rawPayload instanceof Uint8Array
            ? rawPayload
            : Uint8Array.from(rawPayload as any);
      const rawOrderKey = row.order_key;
      if (rawOrderKey === null || rawOrderKey === undefined) {
        throw new Error('missing order_key for insert op from sqlite');
      }
      const orderKey =
        rawOrderKey instanceof Uint8Array ? rawOrderKey : Uint8Array.from(rawOrderKey as any);
      return {
        ...base,
        kind: {
          type: 'insert',
          parent: decodeNodeId(row.parent),
          node: decodeNodeId(row.node),
          orderKey,
          ...(payload !== undefined ? { payload } : {}),
        },
      } as Operation;
    }
    if (row.kind === 'move') {
      const rawOrderKey = row.order_key;
      if (rawOrderKey === null || rawOrderKey === undefined) {
        throw new Error('missing order_key for move op from sqlite');
      }
      const orderKey =
        rawOrderKey instanceof Uint8Array ? rawOrderKey : Uint8Array.from(rawOrderKey as any);
      return {
        ...base,
        kind: {
          type: 'move',
          node: decodeNodeId(row.node),
          newParent: decodeNodeId(row.new_parent),
          orderKey,
        },
      } as Operation;
    }
    if (row.kind === 'delete') {
      return { ...base, kind: { type: 'delete', node: decodeNodeId(row.node) } } as Operation;
    }
    if (row.kind === 'tombstone') {
      return { ...base, kind: { type: 'tombstone', node: decodeNodeId(row.node) } } as Operation;
    }
    if (row.kind === 'payload') {
      const rawPayload = row.payload;
      if (rawPayload === null || rawPayload === undefined) {
        return {
          ...base,
          kind: { type: 'payload', node: decodeNodeId(row.node), payload: null },
        } as Operation;
      }
      const bytes =
        rawPayload instanceof Uint8Array ? rawPayload : Uint8Array.from(rawPayload as any);
      return {
        ...base,
        kind: { type: 'payload', node: decodeNodeId(row.node), payload: bytes },
      } as Operation;
    }
    throw new Error(`unknown op kind from sqlite: ${String(row.kind)}`);
  });
}

import { encode } from 'cborg';
import { CompiledQuery, sql, type Kysely, type RawBuilder } from 'kysely';
export { CompiledQuery, sql };
export type { Kysely };
import type {
  SerializeNodeId,
  SerializeReplica,
  TreecrdtAdapter,
  TreecrdtChangedSinceQuery,
} from './adapter.js';
import { type SqlParam, type SqlValue } from './createTreecrdtDb.js';
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
import type { TreecrdtDb } from './schema.js';

export type { SqlParam, SqlValue } from './createTreecrdtDb.js';
export { createTreecrdtDb, createTreecrdtDbFromRunner } from './createTreecrdtDb.js';
export type { TreecrdtSqlExecutor } from './createTreecrdtDb.js';
export type {
  MetaTable,
  OpsTable,
  TreeMetaTable,
  TreeNodesTable,
  OprefsChildrenTable,
  TreePayloadTable,
  TreecrdtDb,
} from './schema.js';

/**
 * Low-level SQLite executor used only to construct a Kysely dialect.
 * Prefer `createTreecrdtDb` / `client.db` for application SQL.
 */
export type SqliteRunner = {
  exec: (sql: string) => Promise<void> | void;
  getText: (sql: string, params?: SqlParam[]) => Promise<string | null> | string | null;
  query: (
    sql: string,
    params?: SqlParam[],
  ) => Promise<Record<string, SqlValue>[]> | Record<string, SqlValue>[];
};

function firstColumnText(row: Record<string, unknown> | undefined): string | null {
  if (!row) return null;
  const value = Object.values(row)[0];
  if (value === undefined || value === null) return null;
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'bigint') return String(value);
  return null;
}

async function udfJson<T>(
  db: Kysely<TreecrdtDb>,
  query: RawBuilder<unknown>,
  label: string,
): Promise<T> {
  const result = await query.execute(db);
  const text = firstColumnText(result.rows[0] as Record<string, unknown> | undefined);
  if (!text) throw new Error(`expected JSON result for query: ${label}`);
  return JSON.parse(text) as T;
}

async function udfJsonOrEmpty<T>(
  db: Kysely<TreecrdtDb>,
  query: RawBuilder<unknown>,
  label: string,
): Promise<T> {
  const result = await query.execute(db);
  const text = firstColumnText(result.rows[0] as Record<string, unknown> | undefined);
  if (!text) return JSON.parse('[]') as T;
  return JSON.parse(text) as T;
}

let localAuthSavepointCounter = 0;

function decodeLocalOpResult(
  raw: any,
  label: string,
): { op: Operation; outcome: MaterializationOutcome } {
  const ops = decodeSqliteOps([raw.op]);
  if (ops.length !== 1) throw new Error(`expected exactly 1 op from query: ${label}`);
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

function appendOpQuery(
  kind: OperationKind,
  opts: {
    replica: Uint8Array;
    counter: number;
    lamport: number;
    serializeNodeId: SerializeNodeId;
    knownState: Uint8Array | null;
  },
): RawBuilder<unknown> {
  const replica = opts.replica;
  const counter = opts.counter;
  const lamport = opts.lamport;
  const nullLit = sql`null`;

  switch (kind.type) {
    case 'insert':
      if (kind.payload !== undefined) {
        return sql`select treecrdt_append_op(${replica}, ${counter}, ${lamport}, ${'insert'}, ${opts.serializeNodeId(kind.parent)}, ${opts.serializeNodeId(kind.node)}, ${nullLit}, ${kind.orderKey}, ${kind.payload})`;
      }
      return sql`select treecrdt_append_op(${replica}, ${counter}, ${lamport}, ${'insert'}, ${opts.serializeNodeId(kind.parent)}, ${opts.serializeNodeId(kind.node)}, ${nullLit}, ${kind.orderKey}, ${nullLit})`;
    case 'move':
      return sql`select treecrdt_append_op(${replica}, ${counter}, ${lamport}, ${'move'}, ${nullLit}, ${opts.serializeNodeId(kind.node)}, ${opts.serializeNodeId(kind.newParent)}, ${kind.orderKey}, ${nullLit})`;
    case 'delete':
      if (!opts.knownState || opts.knownState.length === 0) {
        throw new Error('treecrdt: delete operations require meta.knownState');
      }
      return sql`select treecrdt_append_op(${replica}, ${counter}, ${lamport}, ${'delete'}, ${nullLit}, ${opts.serializeNodeId(kind.node)}, ${nullLit}, ${nullLit}, ${opts.knownState})`;
    case 'tombstone':
      return sql`select treecrdt_append_op(${replica}, ${counter}, ${lamport}, ${'tombstone'}, ${nullLit}, ${opts.serializeNodeId(kind.node)}, ${nullLit}, ${nullLit}, ${opts.knownState})`;
    case 'payload':
      return sql`select treecrdt_append_op(${replica}, ${counter}, ${lamport}, ${'payload'}, ${nullLit}, ${opts.serializeNodeId(kind.node)}, ${nullLit}, ${nullLit}, ${kind.payload})`;
    default:
      throw new Error('unsupported operation kind');
  }
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
  db: Kysely<TreecrdtDb>,
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

  return decodeSqliteMaterializationOutcome(
    await udfJson(
      db,
      appendOpQuery(kind, {
        replica: serializeReplica(replica),
        counter,
        lamport,
        serializeNodeId,
        knownState: meta.knownState ?? null,
      }),
      'treecrdt_append_op',
    ),
  );
}

function mergeMaterializationOutcomes(outcomes: MaterializationOutcome[]): MaterializationOutcome {
  const last = outcomes.length > 0 ? outcomes[outcomes.length - 1] : undefined;
  return {
    headSeq: last?.headSeq ?? 0,
    changes: outcomes.flatMap((outcome) => outcome.changes),
  };
}

async function treecrdtAppendOps(
  db: Kysely<TreecrdtDb>,
  ops: Operation[],
  serializeNodeId: SerializeNodeId,
  serializeReplica: SerializeReplica,
  opts: { maxBulkOps?: number } = {},
): Promise<MaterializationOutcome> {
  if (ops.length === 0) return emptyMaterializationOutcome();

  const maxBulkOps = opts.maxBulkOps ?? 5_000;

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
    const payload = encode(buildAppendOpsPayload(chunk, serializeNodeId, serializeReplica));
    try {
      const raw = await udfJson(
        db,
        sql`select treecrdt_append_ops(${payload})`,
        'treecrdt_append_ops',
      );
      outcomes.push(decodeSqliteMaterializationOutcome(raw));
    } catch {
      bulkFailedAt = start;
      break;
    }
  }
  if (bulkFailedAt === null) return mergeMaterializationOutcomes(outcomes);

  const remaining = ops.slice(bulkFailedAt);
  for (const op of remaining) {
    const payload = encode(buildAppendOpsPayload([op], serializeNodeId, serializeReplica));
    const raw = await udfJson(
      db,
      sql`select treecrdt_append_ops(${payload})`,
      'treecrdt_append_ops',
    );
    outcomes.push(decodeSqliteMaterializationOutcome(raw));
  }
  return mergeMaterializationOutcomes(outcomes);
}

async function treecrdtOpsSince(
  db: Kysely<TreecrdtDb>,
  filter: { lamport: number; root?: string },
): Promise<unknown[]> {
  if (filter.root === undefined) {
    return udfJsonOrEmpty(
      db,
      sql`select treecrdt_ops_since(${filter.lamport})`,
      'treecrdt_ops_since',
    );
  }
  return udfJsonOrEmpty(
    db,
    sql`select treecrdt_ops_since(${filter.lamport}, ${nodeIdToBytes16(filter.root)})`,
    'treecrdt_ops_since',
  );
}

/** Creates the CRDT adapter over a typed Kysely database. */
export function createTreecrdtSqliteAdapter(
  db: Kysely<TreecrdtDb>,
  opts: { maxBulkOps?: number; onMaterialized?: (event: MaterializationEvent) => void } = {},
): TreecrdtAdapter {
  const emitOutcome = (outcome: MaterializationOutcome) => {
    if (outcome.changes.length === 0) return;
    opts.onMaterialized?.({ ...outcome });
  };
  return {
    setDocId: (docId) => treecrdtSetDocId(db, docId),
    docId: () => treecrdtDocId(db),
    opRefsAll: () => treecrdtOpRefsAll(db, emitOutcome),
    opRefsChildren: (parent) => treecrdtOpRefsChildren(db, parent, emitOutcome),
    opsByOpRefs: (opRefs) => treecrdtOpsByOpRefs(db, opRefs),
    treeChildren: (parent, offset, limit) =>
      treecrdtTreeChildren(db, parent, offset, limit, emitOutcome),
    treeDump: () => treecrdtTreeDump(db, emitOutcome),
    treeChangedSince: (query) => treecrdtTreeChangedSince(db, query, emitOutcome),
    treeNodeCount: () => treecrdtTreeNodeCount(db, emitOutcome),
    treeParent: (node) => treecrdtTreeParent(db, node, emitOutcome),
    treeExists: (node) => treecrdtTreeExists(db, node, emitOutcome),
    treePayload: (node) => treecrdtTreePayload(db, node, emitOutcome),
    headLamport: () => treecrdtHeadLamport(db),
    replicaMaxCounter: (replica) => treecrdtReplicaMaxCounter(db, replica),
    appendOp: (op, serializeNodeId, serializeReplica) =>
      treecrdtAppendOp(db, op, serializeNodeId, serializeReplica),
    appendOps: (ops, serializeNodeId, serializeReplica) =>
      treecrdtAppendOps(db, ops, serializeNodeId, serializeReplica, opts),
    opsSince: (lamport, root) => treecrdtOpsSince(db, { lamport, root }),
  };
}

/**
 * Set the document id used by the SQLite extension for v0 sync (`op_ref` derivation).
 *
 * This MUST be stable for the lifetime of the database, since it affects opRef hashes.
 */
async function treecrdtSetDocId(db: Kysely<TreecrdtDb>, docId: string): Promise<void> {
  await sql`select treecrdt_set_doc_id(${docId})`.execute(db);
}

async function treecrdtDocId(db: Kysely<TreecrdtDb>): Promise<string | null> {
  const result = await sql<{ value: string | null }>`select treecrdt_doc_id() as value`.execute(db);
  return result.rows[0]?.value ?? null;
}

async function treecrdtEnsureMaterialized(
  db: Kysely<TreecrdtDb>,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<MaterializationOutcome> {
  const outcome = decodeSqliteMaterializationOutcome(
    await udfJson(db, sql`select treecrdt_ensure_materialized()`, 'treecrdt_ensure_materialized'),
  );
  if (outcome.changes.length > 0) emitOutcome?.(outcome);
  return outcome;
}

/**
 * Fetch all stored opRefs (16-byte values) from the extension.
 * Returns raw JSON-decoded values: `number[][]` (bytes) is the expected shape.
 */
async function treecrdtOpRefsAll(
  db: Kysely<TreecrdtDb>,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<unknown[]> {
  await treecrdtEnsureMaterialized(db, emitOutcome);
  return udfJsonOrEmpty(db, sql`select treecrdt_oprefs_all()`, 'treecrdt_oprefs_all');
}

/**
 * Fetch opRefs relevant to the `children(parent)` filter from the extension.
 * Returns raw JSON-decoded values: `number[][]` (bytes) is the expected shape.
 */
async function treecrdtOpRefsChildren(
  db: Kysely<TreecrdtDb>,
  parent: Uint8Array,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<unknown[]> {
  await treecrdtEnsureMaterialized(db, emitOutcome);
  return udfJsonOrEmpty(
    db,
    sql`select treecrdt_oprefs_children(${parent})`,
    'treecrdt_oprefs_children',
  );
}

/**
 * Fetch operations by opRef (16-byte values) from the extension.
 * Returns raw JSON-decoded operation rows (same shape as `treecrdtOpsSince`).
 */
async function treecrdtOpsByOpRefs(
  db: Kysely<TreecrdtDb>,
  opRefs: Uint8Array[],
): Promise<unknown[]> {
  const payload = JSON.stringify(opRefs.map((r) => Array.from(r)));
  return udfJsonOrEmpty(
    db,
    sql`select treecrdt_ops_by_oprefs(${payload})`,
    'treecrdt_ops_by_oprefs',
  );
}

/** Root is always visible; any other node needs a live `tree_nodes` row. */
function nodeVisible(node: Uint8Array): RawBuilder<boolean> {
  return sql<boolean>`(${node} = ${ROOT_NODE_BYTES} OR EXISTS (
    SELECT 1 FROM tree_nodes AS visible
    WHERE visible.node = ${node} AND visible.tombstone = 0
  ))`;
}

/**
 * Fetch materialized children for a parent node (16-byte id).
 * Returns node id bytes for decodeSqliteNodeIds.
 */
async function treecrdtTreeChildren(
  db: Kysely<TreecrdtDb>,
  parent: Uint8Array,
  offset: number | null,
  limit: number | null,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<unknown[]> {
  await treecrdtEnsureMaterialized(db, emitOutcome);
  let query = db
    .selectFrom('tree_nodes')
    .select('node')
    .where('parent', '=', parent)
    .where('tombstone', '=', 0)
    .where(nodeVisible(parent))
    .orderBy('order_key')
    .orderBy('node');
  // SQLite requires LIMIT when OFFSET is present; -1 means no upper bound.
  if (offset !== null && limit === null) query = query.limit(-1).offset(offset);
  else if (offset !== null && limit !== null) query = query.limit(limit).offset(offset);
  else if (limit !== null) query = query.limit(limit);
  const rows = await query.execute();
  return rows.map((row) => row.node);
}

/**
 * Dump the full materialized tree state.
 * Returns raw rows for decodeSqliteTreeRows.
 */
async function treecrdtTreeDump(
  db: Kysely<TreecrdtDb>,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<unknown[]> {
  await treecrdtEnsureMaterialized(db, emitOutcome);
  const rows = await db
    .selectFrom('tree_nodes')
    .select(['node', 'parent', 'order_key', 'tombstone'])
    .orderBy('node')
    .execute();
  return rows.map((row) => ({
    node: row.node,
    parent: row.parent,
    order_key: row.order_key,
    tombstone: row.tombstone,
  }));
}

/**
 * Read nodes whose latest visible-change stamp is in `(since, until]`.
 *
 * Updating an unread node replaces its stamp, so it is deferred to the next read, even if it also
 * changed before `until`; pages are not a snapshot. An incremental read scans `tree_changes` in
 * node order, so its pages together cost one scan.
 */
async function treecrdtTreeChangedSince(
  db: Kysely<TreecrdtDb>,
  query: TreecrdtChangedSinceQuery,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<{ headSeq: number; rows: unknown[] }> {
  const outcome = await treecrdtEnsureMaterialized(db, emitOutcome);
  const ahead = Math.max(query.since, query.until ?? 0);
  if (ahead > outcome.headSeq) {
    throw new Error(
      `changedSince seq ${ahead} is ahead of headSeq ${outcome.headSeq}; read from 0 to rebuild`,
    );
  }
  const headSeq = query.until ?? outcome.headSeq;
  const limit = query.limit ?? -1;
  const payloads = query.payloads ? 1 : 0;
  if (query.since === 0) {
    const rows = await udfJsonOrEmpty<unknown[]>(
      db,
      sql`SELECT COALESCE(json_group_array(json_array(node_hex, parent_hex, order_key_hex, tombstone, payload_hex)), '[]')
       FROM (
         SELECT
           lower(hex(n.node)) AS node_hex,
           CASE WHEN n.parent IS NULL THEN NULL ELSE lower(hex(n.parent)) END AS parent_hex,
           CASE WHEN n.order_key IS NULL THEN NULL ELSE lower(hex(n.order_key)) END AS order_key_hex,
           n.tombstone AS tombstone,
           CASE WHEN ${payloads} = 1 AND n.tombstone = 0 AND p.payload IS NOT NULL THEN lower(hex(p.payload)) END AS payload_hex
         FROM tree_nodes n
         LEFT JOIN tree_changes c ON c.node = n.node
         LEFT JOIN tree_payload p ON p.node = n.node
         WHERE COALESCE(c.seq, 0) <= ${headSeq} AND n.node > COALESCE(${query.afterNode}, X'')
         ORDER BY n.node
         LIMIT ${limit}
       )`,
      'tree_changed_since',
    );
    return { headSeq, rows };
  }
  if (query.since >= headSeq) return { headSeq, rows: [] };
  const rows = await udfJsonOrEmpty<unknown[]>(
    db,
    sql`SELECT COALESCE(json_group_array(json_array(node_hex, parent_hex, order_key_hex, tombstone, payload_hex)), '[]')
     FROM (
       SELECT
         lower(hex(c.node)) AS node_hex,
         CASE WHEN n.parent IS NULL THEN NULL ELSE lower(hex(n.parent)) END AS parent_hex,
         CASE WHEN n.order_key IS NULL THEN NULL ELSE lower(hex(n.order_key)) END AS order_key_hex,
         COALESCE(n.tombstone, 1) AS tombstone,
         CASE WHEN ${payloads} = 1 AND n.tombstone = 0 AND p.payload IS NOT NULL THEN lower(hex(p.payload)) END AS payload_hex
       FROM tree_changes c
       LEFT JOIN tree_nodes n ON n.node = c.node
       LEFT JOIN tree_payload p ON p.node = c.node
       WHERE c.seq > ${query.since} AND c.seq <= ${headSeq} AND c.node > COALESCE(${query.afterNode}, X'')
       ORDER BY c.node
       LIMIT ${limit}
     )`,
    'tree_changed_since',
  );
  return { headSeq, rows };
}

/**
 * Count non-tombstoned nodes in the materialized tree (excluding ROOT).
 */
async function treecrdtTreeNodeCount(
  db: Kysely<TreecrdtDb>,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<number> {
  await treecrdtEnsureMaterialized(db, emitOutcome);
  const row = await db
    .selectFrom('tree_nodes')
    .select((eb) => eb.fn.countAll<number>().as('cnt'))
    .where('tombstone', '=', 0)
    .where('node', '<>', ROOT_NODE_BYTES)
    .executeTakeFirst();
  return Number(row?.cnt ?? 0);
}

/**
 * Fetch the parent of a node (16-byte id).
 * Returns the live parent node id as bytes, or null if the node or parent is not visible.
 */
async function treecrdtTreeParent(
  db: Kysely<TreecrdtDb>,
  node: Uint8Array,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<Uint8Array | null> {
  await treecrdtEnsureMaterialized(db, emitOutcome);
  const row = await db
    .selectFrom('tree_nodes as child')
    .innerJoin('tree_nodes as parent', (join) =>
      join.onRef('parent.node', '=', 'child.parent').on('parent.tombstone', '=', 0),
    )
    .select('parent.node as parent_node')
    .where('child.node', '=', node)
    .where('child.tombstone', '=', 0)
    .executeTakeFirst();
  return row?.parent_node ?? null;
}

/**
 * Check if a non-tombstoned node exists (16-byte id).
 */
async function treecrdtTreeExists(
  db: Kysely<TreecrdtDb>,
  node: Uint8Array,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<boolean> {
  await treecrdtEnsureMaterialized(db, emitOutcome);
  const row = await db
    .selectFrom('tree_nodes')
    .select('node')
    .where('node', '=', node)
    .where('tombstone', '=', 0)
    .limit(1)
    .executeTakeFirst();
  return row !== undefined;
}

/**
 * Fetch the payload for a node (16-byte id).
 * Returns the opaque byte payload, or null if the node has no payload or is not visible.
 */
async function treecrdtTreePayload(
  db: Kysely<TreecrdtDb>,
  node: Uint8Array,
  emitOutcome?: (outcome: MaterializationOutcome) => void,
): Promise<Uint8Array | null> {
  await treecrdtEnsureMaterialized(db, emitOutcome);
  const row = await db
    .selectFrom('tree_payload')
    .select('payload')
    .where('node', '=', node)
    .where(nodeVisible(node))
    .limit(1)
    .executeTakeFirst();
  if (!row || row.payload === null || row.payload === undefined) return null;
  const payload = row.payload instanceof Uint8Array ? row.payload : Uint8Array.from(row.payload);
  return payload.byteLength === 0 ? null : payload;
}

/**
 * Fetch the maximum lamport seen in the op log.
 */
async function treecrdtHeadLamport(db: Kysely<TreecrdtDb>): Promise<number> {
  const row = await db
    .selectFrom('ops')
    .select((eb) => eb.fn.max('lamport').as('max_lamport'))
    .executeTakeFirst();
  return Number(row?.max_lamport ?? 0);
}

/**
 * Fetch the maximum counter observed for a replica id.
 */
async function treecrdtReplicaMaxCounter(
  db: Kysely<TreecrdtDb>,
  replica: Uint8Array,
): Promise<number> {
  const row = await db
    .selectFrom('ops')
    .select((eb) => eb.fn.max('counter').as('max_counter'))
    .where('replica', '=', replica)
    .executeTakeFirst();
  return Number(row?.max_counter ?? 0);
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
  db: Kysely<TreecrdtDb>,
  opts: {
    replica: ReplicaId;
    onMaterialized?: (event: MaterializationEvent) => void;
  },
): TreecrdtSqliteWriter {
  const replica = opts.replica;
  const replicaBytes = replicaIdToBytes(replica);

  const getLocalOp = async (
    query: RawBuilder<unknown>,
    label: string,
    writeOpts?: LocalWriteOptions,
  ) => {
    const authSession = writeOpts?.authSession;
    if (!authSession) {
      const { op, outcome } = decodeLocalOpResult(await udfJson<any>(db, query, label), label);
      emitLocalOutcome(outcome, opts.onMaterialized, writeOpts?.writeId);
      return op;
    }

    const savepoint = `treecrdt_local_auth_${++localAuthSavepointCounter}`;
    let released = false;
    await sql.raw(`SAVEPOINT ${savepoint}`).execute(db);
    try {
      const { op, outcome } = decodeLocalOpResult(await udfJson<any>(db, query, label), label);
      await authSession.authorizeLocalOps([op]);
      await sql.raw(`RELEASE ${savepoint}`).execute(db);
      released = true;
      emitLocalOutcome(outcome, opts.onMaterialized, writeOpts?.writeId);
      return op;
    } catch (err) {
      if (!released) {
        try {
          await sql.raw(`ROLLBACK TO ${savepoint}`).execute(db);
        } finally {
          await sql.raw(`RELEASE ${savepoint}`).execute(db);
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
      sql`select treecrdt_local_insert(${replicaBytes}, ${nodeIdToBytes16(parent)}, ${nodeIdToBytes16(node)}, ${placement.type}, ${afterNode}, ${payload})`,
      'treecrdt_local_insert',
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
      sql`select treecrdt_local_move(${replicaBytes}, ${nodeIdToBytes16(node)}, ${nodeIdToBytes16(newParent)}, ${placement.type}, ${afterNode})`,
      'treecrdt_local_move',
      writeOpts,
    );
  };

  const del = async (node: string, writeOpts?: LocalWriteOptions) => {
    return getLocalOp(
      sql`select treecrdt_local_delete(${replicaBytes}, ${nodeIdToBytes16(node)})`,
      'treecrdt_local_delete',
      writeOpts,
    );
  };

  const payload = async (node: string, next: Uint8Array | null, writeOpts?: LocalWriteOptions) => {
    return getLocalOp(
      sql`select treecrdt_local_payload(${replicaBytes}, ${nodeIdToBytes16(node)}, ${next})`,
      'treecrdt_local_payload',
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
    const [node, parent, orderKey, tombstone, payload] = row as [
      string,
      string | null,
      string | null,
      number,
      string | null,
    ];
    return {
      node: decodeNodeId(node),
      parent: parent === null ? null : decodeNodeId(parent),
      orderKey: orderKey === null ? null : hexToBytes(orderKey),
      tombstone: Boolean(tombstone),
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
      afterNode: cursor ? nodeIdToBytes16(cursor.afterNode) : null,
      limit: limit ?? null,
      payloads,
    });
    const nodes = decodeSqliteChangedNodes(rows, payloads);
    const last = nodes[nodes.length - 1];
    const next =
      last && nodes.length === limit ? { since: from, until: headSeq, afterNode: last.node } : null;
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

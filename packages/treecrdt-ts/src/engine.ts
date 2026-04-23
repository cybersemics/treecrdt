import type { Operation, ReplicaId } from './index.js';
import type { SqliteTreeChildRow, SqliteTreeRow, TreecrdtSqlitePlacement } from './sqlite.js';

export type Change =
  | { kind: 'insert'; node: string; parentAfter: string }
  | { kind: 'move'; node: string; parentBefore: string | null; parentAfter: string }
  | { kind: 'delete'; node: string; parentBefore: string | null }
  | { kind: 'restore'; node: string; parentAfter: string | null }
  | { kind: 'payload'; node: string };

/**
 * Coalesced result of advancing materialized state to `headSeq`.
 *
 * This is intentionally not a raw op list. Replays and batched appends collapse multiple writes for
 * the same node into final visible changes before adapters emit events.
 */
export type MaterializationOutcome = {
  headSeq: number;
  changes: Change[];
};

/**
 * Event emitted after write-path materialization, or after read-path recovery advances a pending
 * materialization frontier. `writeIds` echoes optional ids supplied to append APIs.
 */
export type MaterializationEvent = MaterializationOutcome & {
  writeIds?: string[];
};

export type MaterializationListener = (event: MaterializationEvent) => void;

export type WriteOptions = {
  writeId?: string;
};

export type TreecrdtEngineOps = {
  append: (op: Operation, opts?: WriteOptions) => Promise<void>;
  appendMany: (ops: Operation[], opts?: WriteOptions) => Promise<void>;
  all: () => Promise<Operation[]>;
  since: (lamport: number, root?: string) => Promise<Operation[]>;
  children: (parent: string) => Promise<Operation[]>;
  get: (opRefs: Uint8Array[]) => Promise<Operation[]>;
};

export type TreecrdtEngineOpRefs = {
  all: () => Promise<Uint8Array[]>;
  children: (parent: string) => Promise<Uint8Array[]>;
};

export type TreecrdtEngineTree = {
  children: (parent: string) => Promise<string[]>;
  childrenPage?: (
    parent: string,
    cursor: { orderKey: Uint8Array; node: Uint8Array } | null,
    limit: number,
  ) => Promise<SqliteTreeChildRow[]>;
  dump: () => Promise<SqliteTreeRow[]>;
  nodeCount: () => Promise<number>;
  parent: (node: string) => Promise<string | null>;
  exists: (node: string) => Promise<boolean>;
  getPayload: (node: string) => Promise<Uint8Array | null>;
};

export type TreecrdtEngineMeta = {
  headLamport: () => Promise<number>;
  replicaMaxCounter: (replica: ReplicaId) => Promise<number>;
};

export type TreecrdtEngineLocal = {
  insert: (
    replica: ReplicaId,
    parent: string,
    node: string,
    placement: TreecrdtSqlitePlacement,
    payload: Uint8Array | null,
  ) => Promise<Operation>;
  move: (
    replica: ReplicaId,
    node: string,
    newParent: string,
    placement: TreecrdtSqlitePlacement,
  ) => Promise<Operation>;
  delete: (replica: ReplicaId, node: string) => Promise<Operation>;
  payload: (replica: ReplicaId, node: string, payload: Uint8Array | null) => Promise<Operation>;
};

/**
 * Common high-level engine surface shared across the Node and wa-sqlite backends.
 *
 * Note: `mode`/`storage` are intentionally strings (backend-defined) so Node and browsers can both
 * conform without unions drifting.
 */
export type TreecrdtEngine = {
  mode: string;
  storage: string;
  docId: string;
  ops: TreecrdtEngineOps;
  opRefs: TreecrdtEngineOpRefs;
  tree: TreecrdtEngineTree;
  meta: TreecrdtEngineMeta;
  local: TreecrdtEngineLocal;
  onMaterialized: (listener: MaterializationListener) => () => void;
  close: () => Promise<void>;
};

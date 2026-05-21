import type { Operation, ReplicaId } from './index.js';
import type { SqliteTreeChildRow, SqliteTreeRow, TreecrdtSqlitePlacement } from './sqlite.js';

export type MaterializationSigner = {
  publicKey: Uint8Array;
};

export type MaterializationSource = {
  /**
   * Operation that caused the visible change.
   *
   * `replica` is a low-level CRDT replica id, not an auth/user identity.
   */
  operation: {
    id: {
      replica: Uint8Array;
      counter: number;
    };
    /** Lamport timestamp assigned to the operation. */
    lamport: number;
  };
  /** Auth signer metadata for the operation, when available. */
  signer?: MaterializationSigner;
};

type ChangeSource = {
  /**
   * Operation source metadata when the materializer can attribute the visible change to one exact op.
   *
   * Conservative catch-up/rebuild paths may omit this when a visible change is derived from rebuilt
   * state instead of a single operation.
   */
  source?: MaterializationSource;
};

export type Change =
  | ({
      kind: 'insert';
      node: string;
      parentAfter: string;
      payload: Uint8Array | null;
    } & ChangeSource)
  | ({
      kind: 'move';
      node: string;
      parentBefore: string | null;
      parentAfter: string;
    } & ChangeSource)
  | ({ kind: 'delete'; node: string; parentBefore: string | null } & ChangeSource)
  | ({
      kind: 'restore';
      node: string;
      parentAfter: string | null;
      payload: Uint8Array | null;
    } & ChangeSource)
  | ({ kind: 'payload'; node: string; payload: Uint8Array | null } & ChangeSource);

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

export function emptyMaterializationOutcome(headSeq = 0): MaterializationOutcome {
  return { headSeq, changes: [] };
}

/**
 * Event emitted after write-path materialization, or after read-path recovery advances a pending
 * materialization frontier. `writeIds` echoes optional ids supplied to append APIs.
 */
export type MaterializationEvent = MaterializationOutcome & {
  writeIds?: string[];
};

export type MaterializationListener = (event: MaterializationEvent) => void;

export type MaterializationDispatcher = {
  emitEvent: (event: MaterializationEvent) => void;
  emitOutcome: (outcome: MaterializationOutcome, writeId?: string) => void;
  onMaterialized: (listener: MaterializationListener) => () => void;
};

export function createMaterializationDispatcher(): MaterializationDispatcher {
  const listeners = new Set<MaterializationListener>();

  const emitEvent = (event: MaterializationEvent) => {
    if (event.changes.length === 0) return;
    for (const listener of listeners) listener(event);
  };

  const emitOutcome = (outcome: MaterializationOutcome, writeId?: string) => {
    if (outcome.changes.length === 0) return;
    emitEvent({
      ...outcome,
      ...(writeId ? { writeIds: [writeId] } : {}),
    });
  };

  const onMaterialized = (listener: MaterializationListener) => {
    listeners.add(listener);
    return () => {
      listeners.delete(listener);
    };
  };

  return { emitEvent, emitOutcome, onMaterialized };
}

export type WriteOptions = {
  writeId?: string;
};

export type LocalWriteAuthSession = {
  /**
   * Authorizes local ops before they are exposed as committed.
   *
   * If the session has signer metadata available, expose it on `signer` so SQLite writers can
   * annotate materialization events.
   */
  authorizeLocalOps: (ops: readonly Operation[]) => Promise<unknown>;
  signer?: MaterializationSigner;
};

export type LocalWriteOptions = {
  /** Echoed on the materialization event emitted by this local write. */
  writeId?: string;

  /**
   * Authorizes the minted local op before it is exposed to callers as committed.
   *
   * SQLite clients wrap this in a savepoint so auth failures roll back the local
   * op and defer materialization events until auth succeeds.
   */
  authSession?: LocalWriteAuthSession;
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

export type BoundTreecrdtEngineLocal = {
  insert: (
    parent: string,
    node: string,
    placement: TreecrdtSqlitePlacement,
    payload: Uint8Array | null,
    opts?: LocalWriteOptions,
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

export type TreecrdtEngineLocal = {
  insert: (
    replica: ReplicaId,
    parent: string,
    node: string,
    placement: TreecrdtSqlitePlacement,
    payload: Uint8Array | null,
    opts?: LocalWriteOptions,
  ) => Promise<Operation>;
  move: (
    replica: ReplicaId,
    node: string,
    newParent: string,
    placement: TreecrdtSqlitePlacement,
    opts?: LocalWriteOptions,
  ) => Promise<Operation>;
  delete: (replica: ReplicaId, node: string, opts?: LocalWriteOptions) => Promise<Operation>;
  payload: (
    replica: ReplicaId,
    node: string,
    payload: Uint8Array | null,
    opts?: LocalWriteOptions,
  ) => Promise<Operation>;
  forReplica: (replica: ReplicaId, opts?: LocalWriteOptions) => BoundTreecrdtEngineLocal;
};

export type TreecrdtEngineLocalMethods = Omit<TreecrdtEngineLocal, 'forReplica'>;

export function createBoundTreecrdtEngineLocal(
  local: TreecrdtEngineLocalMethods,
  replica: ReplicaId,
  defaults: LocalWriteOptions = {},
): BoundTreecrdtEngineLocal {
  const hasDefaults = Object.keys(defaults).length > 0;
  const mergeOptions = (opts?: LocalWriteOptions): LocalWriteOptions | undefined => {
    if (!hasDefaults) return opts;
    return { ...defaults, ...opts };
  };

  return {
    insert: (parent, node, placement, payload, opts) =>
      local.insert(replica, parent, node, placement, payload, mergeOptions(opts)),
    move: (node, newParent, placement, opts) =>
      local.move(replica, node, newParent, placement, mergeOptions(opts)),
    delete: (node, opts) => local.delete(replica, node, mergeOptions(opts)),
    payload: (node, payload, opts) => local.payload(replica, node, payload, mergeOptions(opts)),
  };
}

export function createTreecrdtEngineLocal(
  methods: TreecrdtEngineLocalMethods,
): TreecrdtEngineLocal {
  return {
    ...methods,
    forReplica: (replica, opts) => createBoundTreecrdtEngineLocal(methods, replica, opts),
  };
}

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

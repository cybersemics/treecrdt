import type { Operation, ReplicaId } from './index.js';
import type { SqliteTreeRow, TreecrdtSqlitePlacement } from './sqlite.js';
import { ROOT_NODE_ID_HEX } from './ids.js';

export type MaterializationSource = {
  /**
   * Operation that caused the visible change.
   *
   * `replica` is a low-level CRDT replica id, not an auth/user identity.
   */
  operation?: {
    id: {
      replica: Uint8Array;
      counter: number;
    };
    /** Lamport timestamp assigned to the operation. */
    lamport: number;
  };
  /** Local write ids supplied to the append/local write API that produced this visible change. */
  writeIds?: string[];
  /** Future auth metadata for the operation signer, when available. */
  signer?: {
    publicKey: Uint8Array;
  };
};

type ChangeSource = {
  /**
   * Source metadata for the visible change.
   *
   * Conservative catch-up/rebuild paths may omit operation metadata when a visible change is derived
   * from rebuilt state instead of a single operation.
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
 * materialization frontier. Local write ids are echoed on each affected change's `source.writeIds`.
 */
export type MaterializationEvent = MaterializationOutcome;

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
    emitEvent(addMaterializationWriteId(outcome, writeId));
  };

  const onMaterialized = (listener: MaterializationListener) => {
    listeners.add(listener);
    return () => {
      listeners.delete(listener);
    };
  };

  return { emitEvent, emitOutcome, onMaterialized };
}

export function addMaterializationWriteId(
  outcome: MaterializationOutcome,
  writeId?: string,
): MaterializationEvent {
  if (!writeId) return { ...outcome };
  return {
    ...outcome,
    changes: outcome.changes.map((change) => ({
      ...change,
      source: {
        ...change.source,
        writeIds: [...(change.source?.writeIds ?? []), writeId],
      },
    })),
  };
}

export type WriteOptions = {
  writeId?: string;
};

export type LocalWriteAuthSession = {
  authorizeLocalOps: (ops: readonly Operation[]) => Promise<unknown>;
};

export type LocalWriteOptions = {
  /** Echoed on each materialized change emitted by this local write. */
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

/**
 * Optional half-open index range over a parent's live (non-tombstoned) children, in `(order_key, node)` order.
 *
 * Omitted `index` defaults to `0`. Omitted `length` returns from `index` through the end. Out-of-range
 * starts yield an empty list; a partial tail is returned when `index + length` exceeds the child count.
 */
export type TreecrdtChildrenSlice = {
  index?: number;
  length?: number;
};

/** Normalize optional slice args; throws on negative or non-integer values. */
export function normalizeChildrenSlice(
  slice?: TreecrdtChildrenSlice,
): { index: number; length: number | undefined } | undefined {
  if (slice === undefined) return undefined;
  const index = slice.index ?? 0;
  const length = slice.length;
  if (!Number.isInteger(index) || index < 0) {
    throw new Error(`invalid children slice index: ${String(slice.index)}`);
  }
  if (length !== undefined && (!Number.isInteger(length) || length < 0)) {
    throw new Error(`invalid children slice length: ${String(slice.length)}`);
  }
  return { index, length };
}

/**
 * Lazy materialized-tree handle. Methods issue fresh queries; no local cache.
 */
export type TreecrdtNode = {
  readonly id: string;
  /** Parent handle, or undefined when this node or its parent is not visible. */
  parent: () => Promise<TreecrdtNode | undefined>;
  payload: () => Promise<Uint8Array | null>;
  children: (slice?: TreecrdtChildrenSlice) => Promise<TreecrdtNode[]>;
};

/**
 * Per-node read primitives each backend keeps private; used by createTreecrdtTreeNodes.
 */
export type TreecrdtNodePrimitives = {
  exists: (node: string) => Promise<boolean>;
  parent: (node: string) => Promise<string | null>;
  payload: (node: string) => Promise<Uint8Array | null>;
  /**
   * Live children in stable order. `(null, null)` = all; `(offset, null)` = suffix from offset;
   * `(offset, limit)` = fixed-size window. Empty windows are handled in the engine (no I/O).
   */
  children: (parent: string, offset: number | null, limit: number | null) => Promise<string[]>;
};

export type TreecrdtEngineTree = {
  /** Undefined when the node is absent (tombstoned or never inserted). */
  get: (node: string) => Promise<TreecrdtNode | undefined>;
  /** Always-available ROOT handle; does not check existence. */
  root: TreecrdtNode;
  dump: () => Promise<SqliteTreeRow[]>;
  nodeCount: () => Promise<number>;
};

/**
 * Build the Node navigation surface (`get` + `root`) over per-node backend primitives.
 * Node methods stay lazy so callers always see current materialized state.
 */
export function createTreecrdtTreeNodes(
  primitives: TreecrdtNodePrimitives,
): Pick<TreecrdtEngineTree, 'get' | 'root'> {
  const createNode = (id: string): TreecrdtNode => {
    const node: TreecrdtNode = {
      id,
      parent: async () => {
        const parentId = await primitives.parent(id);
        return parentId === null ? undefined : createNode(parentId);
      },
      payload: () => primitives.payload(id),
      children: async (slice) => {
        const normalized = normalizeChildrenSlice(slice);
        if (normalized !== undefined && normalized.length === 0) return [];
        const offset = normalized === undefined ? null : normalized.index;
        const limit = normalized === undefined ? null : (normalized.length ?? null);
        return (await primitives.children(id, offset, limit)).map(createNode);
      },
    };
    return node;
  };

  return {
    get: async (nodeId) => {
      if (!(await primitives.exists(nodeId))) return undefined;
      return createNode(nodeId);
    },
    root: createNode(ROOT_NODE_ID_HEX),
  };
}

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

import type { Operation, OperationId } from '@treecrdt/interface';
import { normalizeNodeId } from '@treecrdt/interface/ids';
import type { WasmTree } from '../pkg/treecrdt_wasm.js';
import { toWasmOperation } from './operation-input.js';
import {
  createMemoryProjection,
  type MemoryProjection,
  type MemoryProjectionOptions,
} from './memory-projection.js';

export type { MemoryProjection, MemoryProjectionOptions } from './memory-projection.js';

export type MemoryClientOptions = { replicaId?: Uint8Array };
export type MemoryContent = {
  readonly id: string;
  readonly parentId: string | null;
  readonly payload: Uint8Array | null;
};
export type MemoryRow = MemoryContent & {
  readonly children: readonly string[];
};
type LocalCommands = {
  /** Omitted after appends; null prepends; a node ID inserts after that sibling. */
  insert(
    parent: string,
    node: string,
    after?: string | null,
    payload?: Uint8Array | null,
  ): Operation;
  move(node: string, parent: string, after?: string | null): Operation;
  payload(node: string, payload: Uint8Array | null): Operation;
  delete(node: string): Operation;
};
export type MemoryRowChange = {
  readonly id: string;
  readonly before: MemoryRow | null;
  readonly after: MemoryRow | null;
};
export type MemoryChanges = {
  readonly revision: number;
  readonly reset: boolean;
  readonly changes: readonly MemoryRowChange[];
};
export interface MemoryReader {
  /** Returns an owned row from the current tree, not a retained tree version. */
  get(id: string): MemoryRow | undefined;
  /** Returns owned content without copying the node's child list. Payload bytes are caller-owned. */
  getContent(id: string): MemoryContent | undefined;
  /** Canonical raw order, including payload-less nodes. Unchanged cached results retain identity. */
  getChildren(id: string): readonly string[];
  /** Position in the raw parent order, or undefined for a missing node or root. */
  getPosition(id: string): number | undefined;
  nodeIds(): string[];
}
export interface MemoryTransaction extends MemoryReader {
  readonly local: LocalCommands;
  /** Appends compensating operations; revert their IDs to redo. May overwrite intervening writes. */
  revert(operationIds: readonly OperationId[]): Operation[];
  /** Cumulative changes; repeated reads retain identity until another write is attempted. */
  getChanges(): MemoryChanges;
}
export interface MemoryClient extends MemoryReader {
  readonly revision: number;
  /** Creates a client-owned live projection outside transactions, enumerating existing content once. */
  createProjection<T>(options: MemoryProjectionOptions<T>): MemoryProjection<T>;
  readonly local: LocalCommands;
  revert(operationIds: readonly OperationId[]): Operation[];
  transact<T>(work: (transaction: MemoryTransaction) => T): {
    value: T;
    operations: Operation[];
    changes: MemoryChanges;
  };
  appendOperations(operations: readonly Operation[]): void;
  operationCount(): number;
  operationsFrom(cursor: number): Operation[];
  operationsAt(indices: readonly number[]): Operation[];
  /**
   * Reads ordered, non-overlapping [from, to) ranges using one temporary native reader.
   * Cursors are local accepted-operation offsets, not Lamport times or transferable revisions.
   * Readers expire after each synchronous callback; returned rows remain owned.
   * Captures the requested history before visiting, independently of later live-client changes.
   */
  readHistory(
    spans: readonly { from: number; to: number }[],
    visit: (
      history: {
        before: MemoryReader;
        after: MemoryReader;
        changes: readonly MemoryRowChange[];
      },
      index: number,
    ) => void,
  ): void;
  maxLamport(): number;
  subscribe(listener: (changes: MemoryChanges) => void): () => void;
  close(): void;
}

function equalRows(a: MemoryRow | null, b: MemoryRow | null): boolean {
  if (!a || !b) return a === b;
  const left = a.payload;
  const right = b.payload;
  return (
    a.parentId === b.parentId &&
    a.children.length === b.children.length &&
    a.children.every((id, index) => id === b.children[index]) &&
    (left === null || right === null
      ? left === right
      : left.length === right.length && left.every((byte, index) => byte === right[index]))
  );
}

/** Reuses requested raw sibling queries; the caller supplies lifecycle checks and effective changes. */
function createTopologyReader(
  readChildren: (id: string) => readonly string[],
  readParent: (id: string) => string | null,
  check: () => void,
) {
  const orders = new Map<
    string,
    { children: readonly string[]; positions?: Map<string, number> }
  >();
  const getChildren = (id: string): readonly string[] => {
    check();
    const key = normalizeNodeId(id);
    let order = orders.get(key);
    if (!order) {
      order = { children: Object.freeze(readChildren(key)) };
      orders.set(key, order);
    }
    return order.children;
  };
  return {
    getChildren,
    getPosition: (id: string): number | undefined => {
      check();
      const key = normalizeNodeId(id);
      const parent = readParent(key);
      if (parent === null) return undefined;
      const children = getChildren(parent);
      const order = orders.get(parent)!;
      order.positions ??= new Map(children.map((child, index) => [child, index]));
      return order.positions.get(key);
    },
    invalidate: ({ reset, changes }: Pick<MemoryChanges, 'reset' | 'changes'>) => {
      if (reset) orders.clear();
      else
        for (const { id, after } of changes) {
          const cached = orders.get(id)?.children;
          const current = after?.children ?? [];
          if (
            cached &&
            (cached.length !== current.length ||
              cached.some((child, index) => child !== current[index]))
          ) {
            orders.delete(id);
          }
        }
    },
    clear: () => orders.clear(),
  };
}

/** Only requested rows and changed before/after records cross the native boundary. */
export function createInitializedMemoryClient(native: WasmTree): MemoryClient {
  native.enableReadTracking();
  native.drainReadChanges();
  let revision = 0;
  let closed = false;
  type PendingChanges = { changes: Map<string, MemoryRowChange>; reset: boolean };
  let pending: PendingChanges | undefined;
  let needsDrain = false;
  let failed = false;
  let cachedChanges: MemoryChanges | undefined;
  let notifications: MemoryChanges[] | undefined;
  const projections = new Set<ReturnType<typeof createMemoryProjection>>();
  let evaluatingProjection = false;
  const listeners = new Set<(changes: MemoryChanges) => void>();
  const ensureOpen = () => {
    if (closed) throw new Error('The memory client is closed');
  };
  const ensureMutable = () => {
    ensureOpen();
    if (evaluatingProjection) {
      if (pending) failed = true;
      throw new Error('Projection callbacks cannot mutate the memory client');
    }
  };
  const evaluateProjection = <T>(callback: () => T): T => {
    evaluatingProjection = true;
    try {
      const value = callback();
      assertSynchronous(value, 'Projection callbacks');
      if (failed) throw new Error('The memory transaction has failed');
      return value;
    } catch (error) {
      if (pending) failed = true;
      throw error;
    } finally {
      evaluatingProjection = false;
    }
  };
  const committed = <T>(read: () => T): T => {
    ensureOpen();
    if (pending) throw new Error('The operation log is unavailable during a transaction');
    return read();
  };
  const read = (id: string) => {
    ensureOpen();
    const row = native.readNode(normalizeNodeId(id));
    return row ? immutableRow(row) : undefined;
  };
  const getContent = (id: string): MemoryContent | undefined => {
    ensureOpen();
    const content = native.readContent(normalizeNodeId(id));
    return content ? Object.freeze(content) : undefined;
  };
  const topology = createTopologyReader(
    (id) => native.readChildren(id),
    (id) => native.readParent(id),
    () => {
      ensureOpen();
      if (failed) throw new Error('The memory transaction has failed');
      // Queries inside a transaction must see writes even before the caller asks for change records.
      if (pending) drain();
    },
  );
  const nodeIds = () => {
    ensureOpen();
    return native.nodeIds();
  };
  const local = (run: <T>(work: () => T) => T): LocalCommands => ({
    insert: (parent, node, after, payload) =>
      run(() =>
        native.localInsert(
          normalizeNodeId(parent),
          normalizeNodeId(node),
          after == null ? after : normalizeNodeId(after),
          payload,
        ),
      ),
    move: (node, parent, after) =>
      run(() =>
        native.localMove(
          normalizeNodeId(node),
          normalizeNodeId(parent),
          after == null ? after : normalizeNodeId(after),
        ),
      ),
    payload: (node, payload) => run(() => native.localPayload(normalizeNodeId(node), payload)),
    delete: (node) => run(() => native.localDelete(normalizeNodeId(node))),
  });
  const drain = () => {
    if (!needsDrain) return;
    const batch = native.drainReadChanges();
    topology.invalidate(batch);
    const changes = batch.changes.map((change) =>
      Object.freeze({
        id: change.id,
        before: change.before ? immutableRow(change.before) : null,
        after: change.after ? immutableRow(change.after) : null,
      }),
    );
    pending!.reset ||= batch.reset;
    for (const change of changes) {
      const previous = pending!.changes.get(change.id);
      pending!.changes.set(
        change.id,
        previous ? Object.freeze({ ...change, before: previous.before }) : change,
      );
    }
    needsDrain = false;
    for (const projection of projections) projection.apply({ reset: batch.reset, changes });
  };
  const collect = (): MemoryChanges => {
    drain();
    return (cachedChanges ??= Object.freeze({
      revision,
      reset: pending!.reset,
      changes: Object.freeze(
        [...pending!.changes.values()].filter(({ before, after }) => !equalRows(before, after)),
      ),
    }));
  };
  const transact = <T>(work: (transaction: MemoryTransaction) => T) => {
    ensureMutable();
    if (pending) throw new Error('Nested memory transactions are not supported');
    const cursor = native.operationCount();
    native.beginTransaction();
    pending = { changes: new Map(), reset: false };
    needsDrain = true;
    failed = false;
    for (const projection of projections) projection.begin();
    let active = true;
    const during = <V>(work: () => V): V => {
      if (!active) throw new Error('The memory transaction has finished');
      return work();
    };
    let value: T;
    let changes: MemoryChanges;
    try {
      value = work({
        local: local((work) => during(() => write(work))),
        revert: (ids) => during(() => write(() => native.revertOperations(ids))),
        get: (id) => during(() => read(id)),
        getContent: (id) => during(() => getContent(id)),
        getChildren: (id) => during(() => topology.getChildren(id)),
        getPosition: (id) => during(() => topology.getPosition(id)),
        nodeIds: () => during(nodeIds),
        getChanges: () => during(collect),
      });
      assertSynchronous(value, 'Memory transactions');
      if (failed) throw new Error('The memory transaction failed; its writes were rolled back');
      changes = collect();
      native.commitTransaction();
      for (const projection of projections) projection.commit();
    } catch (error) {
      try {
        native.rollbackTransaction();
      } finally {
        topology.clear();
        for (const projection of projections) projection.rollback();
      }
      throw error;
    } finally {
      active = false;
      pending = undefined;
      needsDrain = false;
      failed = false;
      cachedChanges = undefined;
    }
    const operations = native.operationsFrom(cursor);
    if (changes.reset || changes.changes.length) {
      changes = Object.freeze({ ...changes, revision: ++revision });
      if (notifications) notifications.push(changes);
      else {
        notifications = [changes];
        try {
          for (const batch of notifications) {
            for (const listener of [...listeners]) {
              try {
                listener(batch);
              } catch (error) {
                console.error('TreeCRDT subscriber failed', error);
              }
            }
          }
        } finally {
          notifications = undefined;
        }
      }
    }
    return { value, operations, changes };
  };
  const write = <T>(work: () => T): T => {
    ensureMutable();
    if (!pending) return transact(() => write(work)).value;
    cachedChanges = undefined;
    needsDrain = true;
    try {
      return work();
    } catch (error) {
      failed = true;
      throw error;
    }
  };
  return {
    createProjection: (options) => {
      ensureMutable();
      if (pending) throw new Error('Cannot create a projection during a transaction');
      const projection = createMemoryProjection(
        {
          getContent,
          getChildren: topology.getChildren,
          nodeIds,
        },
        options,
        () => {
          ensureOpen();
          if (evaluatingProjection) throw new Error('Projection callbacks cannot read projections');
          if (failed) throw new Error('The memory transaction has failed');
          if (pending) drain();
        },
        evaluateProjection,
      );
      projections.add(projection);
      return projection.projection;
    },
    get: read,
    getContent,
    getChildren: topology.getChildren,
    getPosition: topology.getPosition,
    nodeIds,
    get revision() {
      return revision;
    },
    local: local(write),
    revert: (ids) => write(() => native.revertOperations(ids)),
    transact,
    appendOperations: (operations) =>
      write(() => native.appendOps(operations.map((operation) => toWasmOperation(operation)))),
    operationCount: () => committed(() => native.operationCount()),
    operationsFrom: (cursor) => committed(() => native.operationsFrom(cursor)),
    operationsAt: (indices) => committed(() => native.operationsAt([...indices])),
    readHistory: (spans, visit) =>
      committed(() => {
        const limit = native.operationCount();
        let previousEnd = 0;
        const ranges = spans.map(({ from, to }) => {
          if (
            !Number.isSafeInteger(from) ||
            !Number.isSafeInteger(to) ||
            from < previousEnd ||
            from > to ||
            to > limit
          ) {
            throw new RangeError(
              'History spans must be ordered, non-overlapping ranges within the operation log',
            );
          }
          previousEnd = to;
          return { from, to };
        });
        if (!ranges.length) return;
        const reader = native.createHistoryReader(previousEnd);
        try {
          ranges.forEach(({ from, to }, index) => {
            reader.advance(from, false);
            const changes = Object.freeze(
              reader.advance(to, true).changes.map((change) =>
                Object.freeze({
                  id: change.id,
                  before: change.before ? immutableRow(change.before) : null,
                  after: change.after ? immutableRow(change.after) : null,
                }),
              ),
            );
            const previous = new Map(changes.map((change) => [change.id, change.before]));
            let active = true;
            const during = <T>(read: () => T): T => {
              if (!active) throw new Error('The historical read callback has finished');
              return read();
            };
            const afterTopology = createTopologyReader(
              (id) => reader.readChildren(id),
              (id) => reader.readParent(id),
              () => during(() => {}),
            );
            const after: MemoryReader = {
              get: (id) =>
                during(() => {
                  const row = reader.readNode(normalizeNodeId(id));
                  return row ? immutableRow(row) : undefined;
                }),
              getContent: (id) =>
                during(() => {
                  const content = reader.readContent(normalizeNodeId(id));
                  return content ? Object.freeze(content) : undefined;
                }),
              getChildren: afterTopology.getChildren,
              getPosition: afterTopology.getPosition,
              nodeIds: () => during(() => reader.nodeIds()),
            };
            const beforeTopology = createTopologyReader(
              (id) =>
                previous.has(id) ? (previous.get(id)?.children ?? []) : after.getChildren(id),
              (id) =>
                previous.has(id) ? (previous.get(id)?.parentId ?? null) : reader.readParent(id),
              () => during(() => {}),
            );
            const before: MemoryReader = {
              get: (id) =>
                during(() => {
                  const key = normalizeNodeId(id);
                  return previous.has(key) ? (previous.get(key) ?? undefined) : after.get(key);
                }),
              getContent: (id) =>
                during(() => {
                  const key = normalizeNodeId(id);
                  if (!previous.has(key)) return after.getContent(key);
                  const row = previous.get(key);
                  return row
                    ? Object.freeze({ id: row.id, parentId: row.parentId, payload: row.payload })
                    : undefined;
                }),
              getChildren: beforeTopology.getChildren,
              getPosition: beforeTopology.getPosition,
              nodeIds: () =>
                during(() => {
                  const ids = new Set(after.nodeIds());
                  for (const change of changes) {
                    if (change.before) ids.add(change.id);
                    else ids.delete(change.id);
                  }
                  return [...ids].sort();
                }),
            };
            try {
              assertSynchronous(
                visit({ before, after, changes }, index),
                'Historical read callbacks',
              );
            } finally {
              active = false;
            }
          });
        } finally {
          reader.free();
        }
      }),
    maxLamport: () => committed(() => native.maxLamport()),
    subscribe: (listener) => {
      ensureOpen();
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    close: () => {
      if (closed) return;
      ensureMutable();
      if (pending) throw new Error('Cannot close a memory client during a transaction');
      closed = true;
      listeners.clear();
      topology.clear();
      for (const projection of projections) projection.close();
      projections.clear();
      native.free();
    },
  };
}

/** Rejects asynchronous work before a transaction or callback-scoped reader can escape. */
function assertSynchronous(value: unknown, context: string): void {
  if (
    value &&
    (typeof value === 'object' || typeof value === 'function') &&
    'then' in value &&
    typeof value.then === 'function'
  ) {
    void Promise.resolve(value).catch(() => {});
    throw new Error(`${context} must be synchronous`);
  }
}

/** Protects retained query results and change records, not whole tree versions. */
function immutableRow(row: MemoryRow): MemoryRow {
  const payload = row.payload;
  return Object.freeze({
    id: row.id,
    parentId: row.parentId,
    children: Object.freeze([...row.children]),
    get payload() {
      return payload?.slice() ?? null;
    },
  });
}

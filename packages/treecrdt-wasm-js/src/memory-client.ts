import type { Operation, OperationId } from '@treecrdt/interface';
import { normalizeNodeId } from '@treecrdt/interface/ids';
import type { WasmTree } from '../pkg/treecrdt_wasm.js';
import { toWasmOperation } from './operation-input.js';

export type MemoryClientOptions = { replicaId?: Uint8Array };
export type MemoryRow = {
  readonly id: string;
  readonly parentId: string | null;
  readonly payload: Uint8Array | null;
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

/** Only requested rows and changed before/after records cross the native boundary. */
export function createInitializedMemoryClient(native: WasmTree): MemoryClient {
  native.enableReadTracking();
  native.drainReadChanges();
  let revision = 0;
  let closed = false;
  let pending: Map<string, MemoryRowChange> | undefined;
  let reset = false;
  let failed = false;
  let cachedChanges: MemoryChanges | undefined;
  let notifications: MemoryChanges[] | undefined;
  const listeners = new Set<(changes: MemoryChanges) => void>();
  const ensureOpen = () => {
    if (closed) throw new Error('The memory client is closed');
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
  const collect = (): MemoryChanges => {
    if (cachedChanges) return cachedChanges;
    const batch = native.drainReadChanges();
    reset ||= batch.reset;
    for (const change of batch.changes) {
      const previous = pending!.get(change.id);
      pending!.set(
        change.id,
        Object.freeze({
          id: change.id,
          before: previous ? previous.before : change.before ? immutableRow(change.before) : null,
          after: change.after ? immutableRow(change.after) : null,
        }),
      );
    }
    return (cachedChanges = Object.freeze({
      revision,
      reset,
      changes: Object.freeze(
        [...pending!.values()].filter(({ before, after }) => !equalRows(before, after)),
      ),
    }));
  };
  const transact = <T>(work: (transaction: MemoryTransaction) => T) => {
    ensureOpen();
    if (pending) throw new Error('Nested memory transactions are not supported');
    const cursor = native.operationCount();
    native.beginTransaction();
    pending = new Map();
    reset = false;
    failed = false;
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
        nodeIds: () => during(nodeIds),
        getChanges: () => during(collect),
      });
      if (
        value &&
        (typeof value === 'object' || typeof value === 'function') &&
        'then' in value &&
        typeof value.then === 'function'
      ) {
        void Promise.resolve(value).catch(() => {});
        throw new Error('Memory transactions must be synchronous');
      }
      if (failed) throw new Error('The memory transaction failed; its writes were rolled back');
      changes = collect();
      native.commitTransaction();
    } catch (error) {
      native.rollbackTransaction();
      throw error;
    } finally {
      active = false;
      pending = undefined;
      reset = false;
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
    ensureOpen();
    if (!pending) return transact(() => write(work)).value;
    cachedChanges = undefined;
    try {
      return work();
    } catch (error) {
      failed = true;
      throw error;
    }
  };
  return {
    get: read,
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
      if (pending) throw new Error('Cannot close a memory client during a transaction');
      closed = true;
      listeners.clear();
      native.free();
    },
  };
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

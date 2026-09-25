import type { Operation } from '@treecrdt/interface';
import { bytesToHex } from '@treecrdt/interface/ids';
import { WasmTree, type InitInput, type TreeSnapshotRow } from '../pkg-web/treecrdt_wasm.js';

export type MemoryClientOptions = { replicaId?: Uint8Array; wasm?: InitInput };
export type MemorySnapshotRow = {
  readonly id: string;
  readonly parentId: string | null;
  readonly payload: Uint8Array | null;
  readonly children: readonly string[];
};
export type MemorySnapshot = ReadonlyMap<string, MemorySnapshotRow>;
export interface MemoryClient {
  transact<T>(work: (client: MemoryClient) => T): { value: T; operations: Operation[] };
  getSnapshot(): MemorySnapshot;
  local: {
    insert(
      parent: string,
      node: string,
      after?: string | null,
      payload?: Uint8Array | null,
    ): Operation;
    move(node: string, parent: string, after?: string | null): Operation;
    payload(node: string, payload?: Uint8Array | null): Operation;
    delete(node: string): Operation;
  };
  appendOperations(operations: readonly Operation[]): void;
  tree: {
    snapshot(): MemorySnapshot;
    get(id: string): MemorySnapshotRow | undefined;
    children(id: string): string[];
    exists(id: string): boolean;
    parent(id: string): string | null;
    payload(id: string): Uint8Array | null;
    dump(): unknown[];
    nodeCount(): number;
  };
  operationCount(): number;
  operationsFrom(cursor: number): Operation[];
  operationsAt(indices: readonly number[]): Operation[];
  maxLamport(): number;
  subscribe(listener: () => void): () => void;
  close(): void;
}

/** Exposes a map without mutators; freezing a native Map alone does not disable set/delete. */
function readonlyMap(entries: Map<string, MemorySnapshotRow>): MemorySnapshot {
  const view: MemorySnapshot = Object.freeze({
    get size() {
      return entries.size;
    },
    get: (id: string) => entries.get(id),
    has: (id: string) => entries.has(id),
    entries: () => entries.entries(),
    keys: () => entries.keys(),
    values: () => entries.values(),
    [Symbol.iterator]: () => entries[Symbol.iterator](),
    forEach: (
      callback: (row: MemorySnapshotRow, id: string, map: MemorySnapshot) => void,
      thisArg?: unknown,
    ) => {
      entries.forEach((row, id) => callback.call(thisArg, row, id, view));
    },
  });
  // The callback's third argument must be the read-only view, never the backing Map.
  return view;
}

function equalRow(a: MemorySnapshotRow | undefined, b: TreeSnapshotRow): boolean {
  if (!a || a.parentId !== b.parentId || a.children.length !== b.children.length) return false;
  const payload = a.payload;
  return (
    (payload === null || b.payload === null
      ? payload === b.payload
      : payload.length === b.payload.length &&
        payload.every((byte, index) => byte === b.payload![index])) &&
    a.children.every((id, index) => id === b.children[index])
  );
}

function immutableRow(row: TreeSnapshotRow): MemorySnapshotRow {
  const payload = row.payload === null ? null : Uint8Array.from(row.payload);
  return Object.freeze({
    id: row.id,
    parentId: row.parentId,
    children: Object.freeze([...row.children]),
    // Typed arrays cannot be deeply frozen. Return a copy so consumers cannot corrupt a held snapshot.
    get payload() {
      return payload?.slice() ?? null;
    },
  });
}

/** The synchronous package implementation; loaders must initialize WASM before constructing it. */
export function createInitializedMemoryClient(options: MemoryClientOptions = {}): MemoryClient {
  const replica = options.replicaId ?? crypto.getRandomValues(new Uint8Array(32));
  if (replica.length !== 32) throw new Error('A memory replica id must contain 32 bytes');
  const native = new WasmTree(bytesToHex(replica));
  let snapshot: MemorySnapshot = readonlyMap(new Map());
  let checkpoint: MemorySnapshot | undefined;
  let closed = false;
  const listeners = new Set<() => void>();

  const ensureOpen = () => {
    if (closed) throw new Error('The memory client is closed');
  };
  const ensureCommitted = () => {
    ensureOpen();
    if (checkpoint) throw new Error('The operation log is unavailable during a transaction');
  };

  const getSnapshot = (): MemorySnapshot => {
    ensureOpen();
    const changes = native.drainSnapshotChanges();
    if (!changes.reset && !changes.rows.length && !changes.removed.length) return snapshot;
    const next = changes.reset ? new Map<string, MemorySnapshotRow>() : new Map(snapshot);
    for (const id of changes.removed) next.delete(id);
    for (const row of changes.rows) {
      const current = snapshot.get(row.id);
      const previous = checkpoint?.get(row.id);
      next.set(
        row.id,
        equalRow(current, row) ? current! : equalRow(previous, row) ? previous! : immutableRow(row),
      );
    }
    const unchanged = (old: MemorySnapshot) =>
      old.size === next.size && [...next].every(([id, row]) => old.get(id) === row);
    if (unchanged(snapshot)) return snapshot;
    snapshot = checkpoint && unchanged(checkpoint) ? checkpoint : readonlyMap(next);
    return snapshot;
  };

  const transact = <T>(
    work: (client: MemoryClient) => T,
  ): { value: T; operations: Operation[] } => {
    ensureOpen();
    if (checkpoint) throw new Error('Nested memory transactions are not supported');
    const previous = getSnapshot();
    const cursor = native.operationCount();
    native.beginTransaction();
    checkpoint = previous;
    let value: T;
    try {
      value = work(client);
      if (
        value &&
        typeof value === 'object' &&
        'then' in value &&
        typeof value.then === 'function'
      ) {
        throw new Error('Memory transactions must be synchronous');
      }
      getSnapshot();
      native.commitTransaction();
    } catch (error) {
      native.rollbackTransaction();
      snapshot = previous;
      throw error;
    } finally {
      checkpoint = undefined;
    }
    const operations = native.operationsFrom(cursor);
    if (snapshot !== previous) {
      for (const listener of [...listeners]) {
        try {
          listener();
        } catch (error) {
          console.error('TreeCRDT subscriber failed', error);
        }
      }
    }
    return { value, operations };
  };

  const write = <T>(work: () => T): T => {
    ensureOpen();
    return checkpoint ? work() : transact(work).value;
  };

  const client: MemoryClient = {
    transact,
    getSnapshot,
    local: {
      insert: (
        parent: string,
        node: string,
        after?: string | null,
        payload?: Uint8Array | null,
      ): Operation => write(() => native.localInsert(parent, node, after, payload)),
      move: (node: string, parent: string, after?: string | null): Operation =>
        write(() => native.localMove(node, parent, after)),
      payload: (node: string, payload?: Uint8Array | null): Operation =>
        write(() => native.localPayload(node, payload)),
      delete: (node: string): Operation => write(() => native.localDelete(node)),
    },
    appendOperations: (operations: readonly Operation[]): void =>
      write(() => native.appendOperations([...operations])),
    tree: {
      snapshot: getSnapshot,
      get: (id: string) => getSnapshot().get(id),
      children: (id: string): string[] => {
        ensureOpen();
        return native.treeChildren(id);
      },
      exists: (id: string): boolean => {
        ensureOpen();
        return native.treeExists(id);
      },
      parent: (id: string): string | null => {
        ensureOpen();
        return native.treeParent(id) ?? null;
      },
      payload: (id: string): Uint8Array | null => {
        ensureOpen();
        return native.treePayload(id) ?? null;
      },
      dump: (): unknown[] => {
        ensureOpen();
        return native.treeDump();
      },
      nodeCount: (): number => {
        ensureOpen();
        return native.treeNodeCount();
      },
    },
    operationCount: (): number => {
      ensureCommitted();
      return native.operationCount();
    },
    operationsFrom: (cursor: number): Operation[] => {
      ensureCommitted();
      return native.operationsFrom(cursor);
    },
    operationsAt: (indices: readonly number[]): Operation[] => {
      ensureCommitted();
      return native.operationsAt([...indices]);
    },
    maxLamport: (): number => {
      ensureCommitted();
      return native.maxLamport();
    },
    subscribe: (listener: () => void): (() => void) => {
      ensureOpen();
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    close: (): void => {
      if (closed) return;
      if (checkpoint) throw new Error('Cannot close a memory client during a transaction');
      closed = true;
      listeners.clear();
      native.free();
    },
  };
  getSnapshot();
  return client;
}

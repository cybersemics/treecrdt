import type { Operation } from '@treecrdt/interface';
import type { TreecrdtPlacement } from '@treecrdt/interface/engine';
import { normalizeNodeId } from '@treecrdt/interface/ids';
import type { WasmTree } from '../pkg/treecrdt_wasm.js';
import { toWasmOperation } from './operation-input.js';

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
  insert(
    parent: string,
    node: string,
    placement: TreecrdtPlacement,
    payload: Uint8Array | null,
  ): Operation;
  move(node: string, parent: string, placement: TreecrdtPlacement): Operation;
  payload(node: string, payload: Uint8Array | null): Operation;
  delete(node: string): Operation;
};
export interface MemoryReader {
  /** Returns an owned row from the current tree, not a retained tree version. */
  get(id: string): MemoryRow | undefined;
  /** Returns owned content without copying the node's child list. Payload bytes are caller-owned. */
  getContent(id: string): MemoryContent | undefined;
  /** Canonical raw order, including payload-less nodes. */
  getChildren(id: string): readonly string[];
  nodeIds(): string[];
}
export interface MemoryClient extends MemoryReader {
  readonly local: LocalCommands;
  appendOperations(operations: readonly Operation[]): void;
  operationCount(): number;
  operationsFrom(cursor: number): Operation[];
  operationsAt(indices: readonly number[]): Operation[];
  maxLamport(): number;
  close(): void;
}

function normalizePlacement(placement: TreecrdtPlacement): TreecrdtPlacement {
  return placement.type === 'after'
    ? { type: 'after', after: normalizeNodeId(placement.after) }
    : placement;
}

export function createInitializedMemoryClient(native: WasmTree): MemoryClient {
  let closed = false;
  let writing = false;
  const ensureOpen = () => {
    if (closed) throw new Error('The memory client is closed');
  };
  const committed = <T>(read: () => T): T => {
    ensureOpen();
    if (writing) throw new Error('The operation log is unavailable during a transaction');
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
  const getChildren = (id: string): readonly string[] => {
    ensureOpen();
    return Object.freeze(native.readChildren(normalizeNodeId(id)));
  };
  const nodeIds = () => {
    ensureOpen();
    return native.nodeIds();
  };
  const local = (run: <T>(work: () => T) => T): LocalCommands => ({
    insert: (parent, node, placement, payload) =>
      run(() =>
        native.localInsert(
          normalizeNodeId(parent),
          normalizeNodeId(node),
          normalizePlacement(placement),
          payload,
        ),
      ),
    move: (node, parent, placement) =>
      run(() =>
        native.localMove(
          normalizeNodeId(node),
          normalizeNodeId(parent),
          normalizePlacement(placement),
        ),
      ),
    payload: (node, payload) => run(() => native.localPayload(normalizeNodeId(node), payload)),
    delete: (node) => run(() => native.localDelete(normalizeNodeId(node))),
  });
  const write = <T>(work: () => T): T => {
    ensureOpen();
    native.beginTransaction();
    writing = true;
    try {
      const value = work();
      native.commitTransaction();
      return value;
    } catch (error) {
      native.rollbackTransaction();
      throw error;
    } finally {
      writing = false;
    }
  };
  return {
    get: read,
    getContent,
    getChildren,
    nodeIds,
    local: local(write),
    appendOperations: (operations) =>
      write(() => native.appendOps(operations.map((operation) => toWasmOperation(operation)))),
    operationCount: () => committed(() => native.operationCount()),
    operationsFrom: (cursor) => committed(() => native.operationsFrom(cursor)),
    operationsAt: (indices) => committed(() => native.operationsAt([...indices])),
    maxLamport: () => committed(() => native.maxLamport()),
    close: () => {
      if (closed) return;
      if (writing) throw new Error('Cannot close a memory client during a transaction');
      closed = true;
      native.free();
    },
  };
}

/** Protects retained query results, not whole tree versions. */
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

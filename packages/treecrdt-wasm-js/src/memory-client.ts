import type { Operation } from '@treecrdt/interface';
import type { TreecrdtPlacement } from '@treecrdt/interface/engine';
import { bytesToHex, normalizeNodeId } from '@treecrdt/interface/ids';
import type { TreeReadContent, TreeReadRow, WasmTree } from '../pkg/treecrdt_wasm.js';
import { toWasmOperation } from './operation-input.js';

export type MemoryClientOptions = { replicaId?: Uint8Array };
export type MemoryContent = Readonly<TreeReadContent>;
export type MemoryRow = Readonly<Omit<TreeReadRow, 'children'>> & {
  readonly children: Readonly<TreeReadRow['children']>;
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
  /** Returns an owned current row. Payload bytes are caller-owned, as with getContent. */
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

/** Creates clients after loading WASM, capturing replica bytes before the asynchronous boundary. */
export function createMemoryClientFactory(loadWasm: () => Promise<{ WasmTree: typeof WasmTree }>) {
  return async (options: MemoryClientOptions = {}): Promise<MemoryClient> => {
    const replica = options.replicaId ?? crypto.getRandomValues(new Uint8Array(32));
    if (replica.length !== 32) throw new Error('A memory replica id must contain 32 bytes');
    const replicaHex = bytesToHex(replica);
    const { WasmTree } = await loadWasm();
    return createInitializedMemoryClient(new WasmTree(replicaHex));
  };
}

export function createInitializedMemoryClient(native: WasmTree): MemoryClient {
  let closed = false;
  let writing = false;
  const ensureOpen = () => {
    if (closed) throw new Error('The memory client is closed');
  };
  const committed = <T>(read: () => T): T => {
    ensureOpen();
    if (writing) throw new Error('The operation log is unavailable during a write');
    return read();
  };
  const write = <T>(work: () => T): T => {
    ensureOpen();
    if (writing) throw new Error('A memory write is already active');
    writing = true;
    try {
      return work();
    } finally {
      writing = false;
    }
  };
  return {
    get: (id) => {
      ensureOpen();
      const row = native.readNode(normalizeNodeId(id));
      if (!row) return undefined;
      Object.freeze(row.children);
      return Object.freeze(row);
    },
    getContent: (id) => {
      ensureOpen();
      const content = native.readContent(normalizeNodeId(id));
      return content ? Object.freeze(content) : undefined;
    },
    getChildren: (id) => {
      ensureOpen();
      return Object.freeze(native.readChildren(normalizeNodeId(id)));
    },
    nodeIds: () => {
      ensureOpen();
      return native.nodeIds();
    },
    local: {
      insert: (parent, node, placement, payload) =>
        write(() =>
          native.localInsert(
            normalizeNodeId(parent),
            normalizeNodeId(node),
            normalizePlacement(placement),
            payload,
          ),
        ),
      move: (node, parent, placement) =>
        write(() =>
          native.localMove(
            normalizeNodeId(node),
            normalizeNodeId(parent),
            normalizePlacement(placement),
          ),
        ),
      payload: (node, payload) => write(() => native.localPayload(normalizeNodeId(node), payload)),
      delete: (node) => write(() => native.localDelete(normalizeNodeId(node))),
    },
    appendOperations: (operations) =>
      write(() => native.appendOps(operations.map((operation) => toWasmOperation(operation)))),
    operationCount: () => committed(() => native.operationCount()),
    operationsFrom: (cursor) => committed(() => native.operationsFrom(cursor)),
    operationsAt: (indices) => committed(() => native.operationsAt(indices)),
    maxLamport: () => committed(() => native.maxLamport()),
    close: () => {
      if (closed) return;
      if (writing) throw new Error('Cannot close a memory client during a write');
      closed = true;
      native.free();
    },
  };
}

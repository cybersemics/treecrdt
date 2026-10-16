import type { Operation } from '@treecrdt/interface';
import type { TreecrdtLocalCommands, TreecrdtPlacement } from '@treecrdt/interface/engine';
import { bytesToHex, hexToBytes, normalizeNodeId } from '@treecrdt/interface/ids';
import { deriveOpRefV0 } from '@treecrdt/sync-protocol/opref';
import type { TreeReadContent, TreeReadRow, WasmTree } from '../pkg/treecrdt_wasm.js';
import { toWasmOperation } from './operation-input.js';

export type MemoryClientOptions = { docId: string; replicaId?: Uint8Array };
export type MemoryContent = Readonly<TreeReadContent>;
export type MemoryRow = Readonly<Omit<TreeReadRow, 'children'>> & {
  readonly children: Readonly<TreeReadRow['children']>;
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
  readonly docId: string;
  readonly local: TreecrdtLocalCommands<Operation>;
  readonly ops: {
    appendMany(operations: readonly Operation[]): void;
    get(opRefs: Uint8Array[]): Operation[];
  };
  readonly opRefs: { all(): Uint8Array[] };
  readonly meta: { headLamport(): number };
  operationCount(): number;
  operationsFrom(cursor: number): Operation[];
  close(): void;
}

function normalizePlacement(placement: TreecrdtPlacement): TreecrdtPlacement {
  return placement.type === 'after'
    ? { type: 'after', after: normalizeNodeId(placement.after) }
    : placement;
}

/** Creates clients after loading WASM, capturing replica bytes before the asynchronous boundary. */
export function createMemoryClientFactory(loadWasm: () => Promise<{ WasmTree: typeof WasmTree }>) {
  return async ({ docId, replicaId }: MemoryClientOptions): Promise<MemoryClient> => {
    const replica = replicaId ?? crypto.getRandomValues(new Uint8Array(32));
    if (replica.length !== 32) throw new Error('A memory replica id must contain 32 bytes');
    const replicaHex = bytesToHex(replica);
    const { WasmTree } = await loadWasm();
    return createInitializedMemoryClient(new WasmTree(replicaHex), docId);
  };
}

export function createInitializedMemoryClient(native: WasmTree, docId: string): MemoryClient {
  let closed = false;
  let writing = false;
  const indices = new Map<string, number>();
  let indexed = 0;
  const refreshOpRefs = () => {
    for (const operation of native.operationsFrom(indexed)) {
      indices.set(bytesToHex(deriveOpRefV0(docId, operation.meta.id)), indexed++);
    }
  };
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
    docId,
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
    ops: {
      appendMany: (operations) =>
        write(() => native.appendOps(operations.map((operation) => toWasmOperation(operation)))),
      get: (refs) =>
        committed(() => {
          refreshOpRefs();
          return native.operationsAt(
            refs.map((ref) => {
              const hex = bytesToHex(ref);
              const index = indices.get(hex);
              if (index === undefined) throw new Error(`Missing memory operation ${hex}`);
              return index;
            }),
          );
        }),
    },
    opRefs: {
      all: () =>
        committed(() => {
          refreshOpRefs();
          return [...indices.keys()].map(hexToBytes);
        }),
    },
    meta: { headLamport: () => committed(() => native.headLamport()) },
    operationCount: () => committed(() => native.operationCount()),
    operationsFrom: (cursor) => committed(() => native.operationsFrom(cursor)),
    close: () => {
      if (closed) return;
      if (writing) throw new Error('Cannot close a memory client during a write');
      closed = true;
      indices.clear();
      native.free();
    },
  };
}

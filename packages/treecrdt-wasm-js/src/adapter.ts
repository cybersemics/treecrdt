import type { Operation, TreecrdtAdapter } from '@treecrdt/interface';
import { emptyMaterializationOutcome } from '@treecrdt/interface/engine';
import { bytesToHex, hexToBytes } from '@treecrdt/interface/ids';
import { createMemoryClient } from './index.node.js';
import { createMemorySyncBackend } from './sync.js';

/** Keeps the benchmark adapter's raw SQLite-compatible rows at its boundary only. */
function operationRow(operation: Operation) {
  const { meta, kind } = operation;
  return {
    replica: Array.from(meta.id.replica),
    counter: meta.id.counter,
    lamport: meta.lamport,
    kind: kind.type,
    node: kind.node,
    ...('parent' in kind && { parent: kind.parent }),
    ...('newParent' in kind && { new_parent: kind.newParent }),
    ...('orderKey' in kind && { order_key: Array.from(kind.orderKey) }),
    ...('payload' in kind &&
      kind.payload !== undefined && {
        payload: kind.payload === null ? null : Array.from(kind.payload),
      }),
    ...(meta.knownState && { known_state: Array.from(meta.knownState) }),
  };
}

/** Legacy async benchmark surface backed by the same synchronous implementation. */
export async function createWasmAdapter(
  options: { replicaHex?: string } = {},
): Promise<TreecrdtAdapter> {
  const client = await createMemoryClient({
    replicaId: options.replicaHex ? hexToBytes(options.replicaHex) : undefined,
  });
  let docId = 'treecrdt';
  let backend = createMemorySyncBackend(client, { docId });
  return {
    setDocId: (value) => {
      docId = value;
      backend = createMemorySyncBackend(client, { docId });
    },
    docId: () => docId,
    opRefsAll: async () => (await backend.listOpRefs({ all: {} })).map((ref) => Array.from(ref)),
    opRefsChildren: async () => {
      throw new Error('The WASM benchmark adapter supports only full-document operation filters');
    },
    opsByOpRefs: async (refs) => (await backend.getOpsByOpRefs(refs)).map(operationRow),
    treeChildren: async (parent) =>
      client.tree.children(bytesToHex(parent)).map((id) => Array.from(hexToBytes(id))),
    treeDump: async () => client.tree.dump(),
    treeNodeCount: () => client.tree.nodeCount(),
    treeParent: async (node) => {
      const parent = client.tree.parent(bytesToHex(node));
      return parent ? hexToBytes(parent) : null;
    },
    treeExists: async (node) => client.tree.exists(bytesToHex(node)),
    treePayload: async (node) => client.tree.payload(bytesToHex(node)),
    headLamport: () => client.maxLamport(),
    replicaMaxCounter: (replica) =>
      client
        .operationsFrom(0)
        .reduce(
          (max, operation) =>
            bytesToHex(operation.meta.id.replica) === bytesToHex(replica)
              ? Math.max(max, operation.meta.id.counter)
              : max,
          0,
        ),
    appendOp: async (operation) => {
      client.appendOperations([operation]);
      return emptyMaterializationOutcome();
    },
    appendOps: async (operations) => {
      client.appendOperations(operations);
      return emptyMaterializationOutcome();
    },
    opsSince: async (lamport, root) => {
      if (root !== undefined)
        throw new Error('The WASM benchmark adapter supports only full-document operation filters');
      return client
        .operationsFrom(0)
        .filter((operation) => operation.meta.lamport > lamport)
        .map(operationRow);
    },
    close: () => client.close(),
  };
}

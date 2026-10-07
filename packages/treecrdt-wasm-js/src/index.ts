import type { Operation, TreecrdtAdapter } from '@treecrdt/interface';
import { emptyMaterializationOutcome } from '@treecrdt/interface/engine';
import { bytesToHex, hexToBytes, normalizeNodeId } from '@treecrdt/interface/ids';
import { WasmTree } from '../pkg/treecrdt_wasm.js';
import { createHash } from 'node:crypto';
import { isUint8Array } from 'node:util/types';

type LoadOptions = {
  replicaHex?: string;
};

export async function createWasmAdapter(opts: LoadOptions = {}): Promise<TreecrdtAdapter> {
  const tree = new WasmTree(opts.replicaHex ?? '7761736d'); // "wasm" in hex
  let docId = 'treecrdt';

  const allOps = (): JsOp[] => {
    const ops = tree.opsSince(0n);
    return Array.isArray(ops) ? (ops as JsOp[]) : [];
  };

  const normalizeReplicaHex = (hex: string): string => hex.replace(/^0x/i, '').toLowerCase();

  const opRefFor = (op: JsOp): Uint8Array => {
    const h = createHash('sha256');
    h.update('treecrdt/opref/wasm-adapter/v0');
    h.update(docId);
    h.update(hexToBytes(normalizeReplicaHex(op.replica)));
    const counterBytes = new Uint8Array(8);
    new DataView(counterBytes.buffer).setBigUint64(0, BigInt(op.counter), false);
    h.update(counterBytes);
    return new Uint8Array(h.digest()).slice(0, 16);
  };

  const sortedOps = (): JsOp[] =>
    allOps()
      .slice()
      .sort((a, b) => {
        if (a.lamport !== b.lamport) return a.lamport - b.lamport;
        const ar = normalizeReplicaHex(a.replica);
        const br = normalizeReplicaHex(b.replica);
        if (ar !== br) return ar < br ? -1 : 1;
        return a.counter - b.counter;
      });

  return {
    setDocId: (next) => {
      docId = next;
    },
    docId: () => docId,
    opRefsAll: async () => sortedOps().map((op) => Array.from(opRefFor(op))),
    opRefsChildren: async (parent) => {
      const parentHex = bytesToHex(parent);
      const filtered = sortedOps().filter((op) => {
        if (op.kind === 'insert') return op.parent === parentHex;
        if (op.kind === 'move') return op.new_parent === parentHex;
        return false;
      });
      return filtered.map((op) => Array.from(opRefFor(op)));
    },
    opsByOpRefs: async (opRefs) => {
      const wanted = new Set(opRefs.map((r) => bytesToHex(r)));
      return sortedOps().filter((op) => wanted.has(bytesToHex(opRefFor(op))));
    },
    treeChildren: async (parent) => {
      const parentHex = bytesToHex(parent);
      const out = tree.treeChildren(parentHex);
      if (!Array.isArray(out)) return [];
      return out.map((hex) => Array.from(hexToBytes(hex)));
    },
    treeDump: async () => tree.treeDump(),
    treeNodeCount: () => tree.treeNodeCount(),
    treeParent: async (node) => {
      const nodeHex = bytesToHex(node);
      const result = tree.treeParent(nodeHex);
      if (result === null || result === undefined) return null;
      return hexToBytes(result);
    },
    treeExists: async (node) => tree.treeExists(bytesToHex(node)),
    treePayload: async (node) => tree.treePayload(bytesToHex(node)) ?? null,
    headLamport: () => Math.max(0, ...allOps().map((op) => op.lamport)),
    replicaMaxCounter: (replica) => {
      const target = bytesToHex(replica);
      let max = 0;
      for (const op of allOps()) {
        if (bytesToHex(hexToBytes(normalizeReplicaHex(op.replica))) !== target) continue;
        if (op.counter > max) max = op.counter;
      }
      return max;
    },
    appendOp: async (op, _serializeNodeId, serializeReplica) => {
      tree.appendOp(toWasmOperation(op, serializeReplica));
      return emptyMaterializationOutcome();
    },
    appendOps: async (ops, _serializeNodeId, serializeReplica) => {
      tree.appendOps(ops.map((op) => toWasmOperation(op, serializeReplica)));
      return emptyMaterializationOutcome();
    },
    opsSince: async (lamport: number) => {
      const ops = tree.opsSince(BigInt(lamport));
      return ops;
    },
    close: async () => {
      tree.free();
    },
  };
}

type JsOp = {
  replica: string;
  counter: number;
  lamport: number;
  kind: string;
  parent?: string | null;
  node: string;
  new_parent?: string | null;
  order_key?: string | null;
  known_state?: number[] | null;
  payload?: string | null;
};

// Serde's byte-buffer reader uses realm-local instanceof checks.
function localByteView(bytes: Uint8Array): Uint8Array {
  if (bytes instanceof Uint8Array || !isUint8Array(bytes)) return bytes;
  const view = bytes as Uint8Array;
  return new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
}

function toWasmOperation(
  op: Operation,
  serializeReplica: (replica: Operation['meta']['id']['replica']) => Uint8Array,
): Operation {
  if (op.kind.type === 'payload' && op.kind.payload === undefined) {
    throw new Error('treecrdt: payload operations require payload or null');
  }
  const kind = { ...op.kind, node: normalizeNodeId(op.kind.node) };
  if (kind.type === 'insert') kind.parent = normalizeNodeId(kind.parent);
  else if (kind.type === 'move') kind.newParent = normalizeNodeId(kind.newParent);
  if ('orderKey' in kind) kind.orderKey = localByteView(kind.orderKey);
  if ('payload' in kind && kind.payload) kind.payload = localByteView(kind.payload);
  return {
    meta: {
      ...op.meta,
      id: { ...op.meta.id, replica: new Uint8Array(serializeReplica(op.meta.id.replica)) },
      knownState: op.meta.knownState && localByteView(op.meta.knownState),
    },
    kind,
  };
}

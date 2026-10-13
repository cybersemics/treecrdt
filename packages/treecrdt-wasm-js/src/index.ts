import type { Operation, TreecrdtAdapter } from '@treecrdt/interface';
import { emptyMaterializationOutcome } from '@treecrdt/interface/engine';
import { bytesToHex, hexToBytes } from '@treecrdt/interface/ids';
import { WasmTree } from '../pkg/treecrdt_wasm.js';
import { createHash } from 'node:crypto';
import { toWasmOperation } from './operation-input.js';

type LoadOptions = {
  replicaHex?: string;
};

export async function createWasmAdapter(
  opts: LoadOptions = {},
): Promise<TreecrdtAdapter<Operation>> {
  const tree = new WasmTree(opts.replicaHex ?? '7761736d'); // "wasm" in hex
  let docId = 'treecrdt';

  const opRefFor = (op: Operation): Uint8Array => {
    const h = createHash('sha256');
    h.update('treecrdt/opref/wasm-adapter/v0');
    h.update(docId);
    h.update(op.meta.id.replica);
    const counterBytes = new Uint8Array(8);
    new DataView(counterBytes.buffer).setBigUint64(0, BigInt(op.meta.id.counter), false);
    h.update(counterBytes);
    return new Uint8Array(h.digest()).slice(0, 16);
  };

  const sortedOps = (): Operation[] =>
    tree.opsSince(0n).sort((a, b) => {
      if (a.meta.lamport !== b.meta.lamport) return a.meta.lamport - b.meta.lamport;
      return (
        Buffer.compare(a.meta.id.replica, b.meta.id.replica) ||
        a.meta.id.counter - b.meta.id.counter
      );
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
        if (op.kind.type === 'insert') return op.kind.parent === parentHex;
        if (op.kind.type === 'move') return op.kind.newParent === parentHex;
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
    headLamport: () => Math.max(0, ...tree.opsSince(0n).map((op) => op.meta.lamport)),
    replicaMaxCounter: (replica) => {
      let max = 0;
      for (const op of tree.opsSince(0n)) {
        if (Buffer.compare(op.meta.id.replica, replica) !== 0) continue;
        if (op.meta.id.counter > max) max = op.meta.id.counter;
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
    opsSince: async (lamport: number) => tree.opsSince(BigInt(lamport)),
    close: async () => {
      tree.free();
    },
  };
}

import type { Operation, TreecrdtAdapter } from "@treecrdt/interface";
import { bytesToHex, hexToBytes, normalizeNodeId } from "@treecrdt/interface/ids";
import { WasmTree } from "../pkg/treecrdt_wasm.js";
import { createHash } from "node:crypto";

type LoadOptions = {
  replicaHex?: string;
};

export async function createWasmAdapter(opts: LoadOptions = {}): Promise<TreecrdtAdapter> {
  const tree = new WasmTree(opts.replicaHex ?? "7761736d"); // "wasm" in hex
  let docId = "treecrdt";

  const allOps = (): JsOp[] => {
    const ops = tree.opsSince(0n);
    return Array.isArray(ops) ? (ops as JsOp[]) : [];
  };

  const normalizeReplicaHex = (hex: string): string => hex.replace(/^0x/i, "").toLowerCase();

  const opRefFor = (op: JsOp): Uint8Array => {
    const h = createHash("sha256");
    h.update("treecrdt/opref/wasm-adapter/v0");
    h.update(docId);
    h.update(hexToBytes(normalizeReplicaHex(op.replica)));
    const counterBytes = new Uint8Array(8);
    new DataView(counterBytes.buffer).setBigUint64(0, BigInt(op.counter), false);
    h.update(counterBytes);
    return new Uint8Array(h.digest()).slice(0, 16);
  };

  const sortedOps = (): JsOp[] =>
    allOps().slice().sort((a, b) => {
      if (a.lamport !== b.lamport) return a.lamport - b.lamport;
      const ar = normalizeReplicaHex(a.replica);
      const br = normalizeReplicaHex(b.replica);
      if (ar !== br) return ar < br ? -1 : 1;
      return a.counter - b.counter;
    });

  const buildTreeState = (): Map<string, { parent: string | null; pos: number | null; tombstone: boolean }> => {
    const nodes = new Map<string, { parent: string | null; pos: number | null; tombstone: boolean }>();
    const ROOT = "0".repeat(32);
    nodes.set(ROOT, { parent: null, pos: null, tombstone: false });

    for (const op of sortedOps()) {
      if (op.kind === "insert") {
        nodes.set(op.node, {
          parent: op.parent ?? ROOT,
          pos: op.position ?? 0,
          tombstone: false,
        });
        continue;
      }
      if (op.kind === "move") {
        const current = nodes.get(op.node) ?? { parent: null, pos: null, tombstone: false };
        nodes.set(op.node, {
          parent: op.new_parent ?? ROOT,
          pos: op.position ?? 0,
          tombstone: false,
        });
        if (current.tombstone) {
          // Keep behavior simple: treat moves as re-activating a node.
        }
        continue;
      }
      if (op.kind === "delete" || op.kind === "tombstone") {
        const current = nodes.get(op.node) ?? { parent: null, pos: null, tombstone: false };
        nodes.set(op.node, { ...current, tombstone: true });
      }
    }

    return nodes;
  };

  const childrenOf = (parentHex: string): string[] => {
    const nodes = buildTreeState();
    const out: Array<{ node: string; pos: number }> = [];
    for (const [node, st] of nodes.entries()) {
      if (node === "0".repeat(32)) continue;
      if (st.tombstone) continue;
      if (st.parent === parentHex) out.push({ node, pos: st.pos ?? 0 });
    }
    out.sort((a, b) => (a.pos !== b.pos ? a.pos - b.pos : a.node < b.node ? -1 : a.node > b.node ? 1 : 0));
    return out.map((v) => v.node);
  };

  return {
    setDocId: (next) => {
      docId = next;
    },
    docId: () => docId,
    opRefsAll: async () => sortedOps().map((op) => Array.from(opRefFor(op))),
    opRefsChildren: async (parent) => {
      const parentHex = bytesToHex(parent);
      const filtered = sortedOps().filter((op) => {
        if (op.kind === "insert") return op.parent === parentHex;
        if (op.kind === "move") return op.new_parent === parentHex;
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
      return childrenOf(parentHex).map((hex) => Array.from(hexToBytes(hex)));
    },
    treeDump: async () => {
      const nodes = buildTreeState();
      return Array.from(nodes.entries(), ([node, st]) => ({
        node: Array.from(hexToBytes(node)),
        parent: st.parent ? Array.from(hexToBytes(st.parent)) : null,
        pos: st.pos,
        tombstone: st.tombstone,
      }));
    },
    treeNodeCount: () => {
      const nodes = buildTreeState();
      let count = 0;
      for (const [node, st] of nodes.entries()) {
        if (node === "0".repeat(32)) continue;
        if (!st.tombstone) count += 1;
      }
      return count;
    },
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
    appendOp: async (op, serializeNodeId, serializeReplica) => {
      const jsOp = toJsOp(op, serializeNodeId, serializeReplica);
      tree.appendOp(JSON.stringify(jsOp));
    },
    appendOps: async (ops, serializeNodeId, serializeReplica) => {
      for (const op of ops) {
        const jsOp = toJsOp(op, serializeNodeId, serializeReplica);
        tree.appendOp(JSON.stringify(jsOp));
      }
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
  position?: number | null;
};

function toHex(bytes: Uint8Array): string {
  return bytesToHex(bytes);
}

function toJsOp(
  op: Operation,
  _serializeNodeId: (id: string) => Uint8Array,
  serializeReplica: (replica: Operation["meta"]["id"]["replica"]) => Uint8Array
): JsOp {
  const base = {
    replica: toHex(serializeReplica(op.meta.id.replica)),
    counter: op.meta.id.counter,
    lamport: op.meta.lamport,
  };

  switch (op.kind.type) {
    case "insert":
      return {
        ...base,
        kind: "insert",
        parent: normalizeNodeId(op.kind.parent),
        node: normalizeNodeId(op.kind.node),
        position: op.kind.position,
      };
    case "move":
      return {
        ...base,
        kind: "move",
        node: normalizeNodeId(op.kind.node),
        new_parent: normalizeNodeId(op.kind.newParent),
        position: op.kind.position,
      };
    case "delete":
      return {
        ...base,
        kind: "delete",
        node: normalizeNodeId(op.kind.node),
      };
    case "tombstone":
      return {
        ...base,
        kind: "tombstone",
        node: normalizeNodeId(op.kind.node),
      };
    default:
      throw new Error("unknown op kind");
  }
}

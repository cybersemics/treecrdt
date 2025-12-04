import type { Operation, TreecrdtAdapter } from "@treecrdt/interface";
import { WasmTree } from "../pkg/treecrdt_wasm.js";

type LoadOptions = {
  replicaHex?: string;
};

export async function createWasmAdapter(opts: LoadOptions = {}): Promise<TreecrdtAdapter> {
  const tree = new WasmTree(opts.replicaHex ?? "7761736d"); // "wasm" in hex

  return {
    async appendOp(op, serializeNodeId, serializeReplica) {
      const jsOp = toJsOp(op, serializeNodeId, serializeReplica);
      tree.appendOp(JSON.stringify(jsOp));
    },
    async opsSince(lamport: number) {
      const ops = tree.opsSince(BigInt(lamport));
      return ops;
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
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function normalizeHex(id: string): string {
  const clean = id.startsWith("0x") ? id.slice(2) : id;
  const padded = clean.length % 2 === 0 ? clean : `0${clean}`;
  return padded.toLowerCase();
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
        parent: normalizeHex(op.kind.parent),
        node: normalizeHex(op.kind.node),
        position: op.kind.position,
      };
    case "move":
      return {
        ...base,
        kind: "move",
        node: normalizeHex(op.kind.node),
        new_parent: normalizeHex(op.kind.newParent),
        position: op.kind.position,
      };
    case "delete":
      return {
        ...base,
        kind: "delete",
        node: normalizeHex(op.kind.node),
      };
    case "tombstone":
      return {
        ...base,
        kind: "tombstone",
        node: normalizeHex(op.kind.node),
      };
    default:
      throw new Error("unknown op kind");
  }
}

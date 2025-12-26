import type { Operation, TreecrdtAdapter } from "@treecrdt/interface";
import { bytesToHex, normalizeNodeId } from "@treecrdt/interface/ids";
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

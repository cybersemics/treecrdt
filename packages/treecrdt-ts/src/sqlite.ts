import type { Operation, OperationKind } from "./index.js";

export type SerializeNodeId = (id: string) => Uint8Array;
export type SerializeReplica = (replica: Operation["meta"]["id"]["replica"]) => Uint8Array;

export type SqlCall = {
  sql: string;
  params: (number | string | Uint8Array | null)[];
};

export function buildAppendOp(
  kind: OperationKind,
  opts: {
    replica: Uint8Array;
    counter: number;
    lamport: number;
    serializeNodeId: SerializeNodeId;
  }
): SqlCall {
  const base = [opts.replica, opts.counter, opts.lamport] as (
    | number
    | string
    | Uint8Array
    | null
  )[];

  switch (kind.type) {
    case "insert":
      return {
        sql: "SELECT treecrdt_append_op(?1,?2,?3,?4,?5,?6,NULL,NULL)",
        params: [
          ...base,
          "insert",
          opts.serializeNodeId(kind.parent),
          opts.serializeNodeId(kind.node),
        ],
      };
    case "move":
      return {
        sql: "SELECT treecrdt_append_op(?1,?2,?3,?4,NULL,?5,?6,?7)",
        params: [
          ...base,
          "move",
          opts.serializeNodeId(kind.node),
          opts.serializeNodeId(kind.newParent),
          kind.position,
        ],
      };
    case "delete":
      return {
        sql: "SELECT treecrdt_append_op(?1,?2,?3,?4,NULL,?5,NULL,NULL)",
        params: [...base, "delete", opts.serializeNodeId(kind.node)],
      };
    case "tombstone":
      return {
        sql: "SELECT treecrdt_append_op(?1,?2,?3,?4,NULL,?5,NULL,NULL)",
        params: [...base, "tombstone", opts.serializeNodeId(kind.node)],
      };
    default:
      throw new Error("unsupported operation kind");
  }
}

export function buildOpsSince(filter: { lamport: number; root?: string; serializeNodeId: SerializeNodeId }): SqlCall {
  if (filter.root === undefined) {
    return { sql: "SELECT treecrdt_ops_since(?1)", params: [filter.lamport] };
  }
  return {
    sql: "SELECT treecrdt_ops_since(?1, ?2)",
    params: [filter.lamport, filter.serializeNodeId(filter.root)],
  };
}

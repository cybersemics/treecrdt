import type { TreecrdtAdapter } from "./adapter.js";
import { nodeIdToBytes16 } from "./ids.js";
import type { Operation, OperationKind } from "./index.js";

export type SerializeNodeId = (id: string) => Uint8Array;
export type SerializeReplica = (replica: Operation["meta"]["id"]["replica"]) => Uint8Array;

export type SqlCall = {
  sql: string;
  params: (number | string | Uint8Array | null)[];
};

export type SqliteRunner = {
  exec: (sql: string) => Promise<void> | void;
  getText: (sql: string, params?: SqlCall["params"]) => Promise<string | null> | string | null;
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

export function buildAppendOpsPayload(
  ops: Operation[],
  serializeNodeId: SerializeNodeId,
  serializeReplica: SerializeReplica
): unknown[] {
  const serialize = (val: string) => Array.from(serializeNodeId(val));
  return ops.map((op) => {
    const { meta, kind } = op;
    const { id, lamport } = meta;
    const { replica, counter } = id;
    const serReplica = serializeReplica(replica);
    const base = {
      replica: Array.from(serReplica),
      counter,
      lamport,
      kind: kind.type,
      position: "position" in kind ? kind.position ?? null : null,
    };
    if (kind.type === "insert") {
      return { ...base, parent: serialize(kind.parent), node: serialize(kind.node), new_parent: null };
    } else if (kind.type === "move") {
      return { ...base, parent: null, node: serialize(kind.node), new_parent: serialize(kind.newParent) };
    } else if (kind.type === "delete") {
      return { ...base, parent: null, node: serialize(kind.node), new_parent: null };
    }
    return { ...base, parent: null, node: serialize(kind.node), new_parent: null };
  });
}

export async function treecrdtAppendOp(
  runner: SqliteRunner,
  op: Operation,
  serializeNodeId: SerializeNodeId,
  serializeReplica: SerializeReplica
): Promise<void> {
  const { meta, kind } = op;
  const { id, lamport } = meta;
  const { replica, counter } = id;

  const { sql, params } = buildAppendOp(kind, {
    replica: serializeReplica(replica),
    counter,
    lamport,
    serializeNodeId,
  });

  await runner.getText(sql, params);
}

export async function treecrdtAppendOps(
  runner: SqliteRunner,
  ops: Operation[],
  serializeNodeId: SerializeNodeId,
  serializeReplica: SerializeReplica,
  opts: { maxBulkOps?: number } = {}
): Promise<void> {
  if (ops.length === 0) return;

  const maxBulkOps = opts.maxBulkOps ?? 5_000;
  const bulkSql = "SELECT treecrdt_append_ops(?1)";

  // Try bulk entrypoint first, chunked to avoid huge JSON payloads.
  let bulkFailedAt: number | null = null;
  for (let start = 0; start < ops.length; start += maxBulkOps) {
    const chunk = ops.slice(start, start + maxBulkOps);
    const payload = buildAppendOpsPayload(chunk, serializeNodeId, serializeReplica);
    try {
      await runner.getText(bulkSql, [JSON.stringify(payload)]);
    } catch {
      bulkFailedAt = start;
      break;
    }
  }
  if (bulkFailedAt === null) return;

  const remaining = ops.slice(bulkFailedAt);
  await runner.exec("BEGIN");
  try {
    for (const op of remaining) {
      await treecrdtAppendOp(runner, op, serializeNodeId, serializeReplica);
    }
    await runner.exec("COMMIT");
  } catch (err) {
    await runner.exec("ROLLBACK");
    throw err;
  }
}

export async function treecrdtOpsSince(
  runner: SqliteRunner,
  filter: { lamport: number; root?: string }
): Promise<unknown[]> {
  const { sql, params } = buildOpsSince({
    lamport: filter.lamport,
    root: filter.root,
    serializeNodeId: nodeIdToBytes16,
  });

  const json = await runner.getText(sql, params);
  if (!json) return [];
  return JSON.parse(json);
}

export function createTreecrdtSqliteAdapter(
  runner: SqliteRunner,
  opts: { maxBulkOps?: number } = {}
): TreecrdtAdapter {
  return {
    appendOp: (op, serializeNodeId, serializeReplica) =>
      treecrdtAppendOp(runner, op, serializeNodeId, serializeReplica),
    appendOps: (ops, serializeNodeId, serializeReplica) =>
      treecrdtAppendOps(runner, ops, serializeNodeId, serializeReplica, opts),
    opsSince: (lamport, root) => treecrdtOpsSince(runner, { lamport, root }),
  };
}

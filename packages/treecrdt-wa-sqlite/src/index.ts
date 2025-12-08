import type { Database } from "wa-sqlite";
import type { Operation, TreecrdtAdapter } from "@treecrdt/interface";
import { buildAppendOp, buildOpsSince } from "@treecrdt/interface/sqlite";

export type LoadOptions = {
  db: Database;
  extensionUrl?: string; // optional; baked-in wa-sqlite build does not need it
  entrypoint?: string; // defaults to sqlite3_treecrdt_init
};

export type OpsSinceFilter = {
  lamport: number;
  root?: string; // node id as hex string or other canonical encoding
};

/**
  Load the TreeCRDT SQLite extension into a wa-sqlite database.
  Expects the extension wasm to export `sqlite3_treecrdt_init`.
*/
export async function loadTreecrdtExtension({
  db,
  extensionUrl,
  entrypoint = "sqlite3_treecrdt_init",
}: LoadOptions): Promise<void> {
  // Our wa-sqlite build already links and auto-registers the extension; nothing to do.
  // If a future build exposes loadExtension and an external wasm URL is provided,
  // we could call it here.
  void db;
  void extensionUrl;
  void entrypoint;
  return Promise.resolve();
}

/**
  Append an operation by calling the extension function.
*/
export async function appendOp(
  db: Database,
  op: Operation,
  serializeNodeId: (id: string) => Uint8Array,
  serializeReplica: (id: Operation["meta"]["id"]["replica"]) => Uint8Array
) {
  const { meta, kind } = op;
  const { id, lamport } = meta;
  const { replica, counter } = id;

  const { sql, params } = buildAppendOp(kind, {
    replica: serializeReplica(replica),
    counter,
    lamport,
    serializeNodeId,
  });

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare(sql);
  let idx = 1;
  for (const p of params) {
    await db.bind(stmt, idx++, p);
  }
  await db.step(stmt);
  await db.finalize(stmt);
}

/**
  Fetch ops since a lamport (optionally filtered by root) and parse to typed operations.
  Currently returns raw JSON from the extension; caller should map to Operation.
*/
export async function opsSince(
  db: Database,
  filter: OpsSinceFilter
): Promise<unknown[]> {
  const { sql, params } = buildOpsSince({
    lamport: filter.lamport,
    root: filter.root,
    serializeNodeId: (id: string) => new TextEncoder().encode(id),
  });

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare(sql);
  let idx = 1;
  for (const p of params) {
    await db.bind(stmt, idx++, p);
  }
  const row = await db.step(stmt);
  let result: unknown[] = [];
  if (row === 0) {
    // done
  } else {
    const json = await db.column_text(stmt, 0);
    result = JSON.parse(json);
  }
  await db.finalize(stmt);
  return result;
}

export function createWaSqliteAdapter(db: Database): TreecrdtAdapter {
  return {
    appendOp: (op, serializeNodeId, serializeReplica) =>
      appendOp(db, op, serializeNodeId, serializeReplica),
    appendOps: async (ops, serializeNodeId, serializeReplica) => {
      if (ops.length === 0) return;
      // Try bulk entrypoint first.
      const payload = ops.map((op) => {
        const { meta, kind } = op;
        const { id, lamport } = meta;
        const { replica, counter } = id;
        const serReplica = serializeReplica(replica);
        const serialize = (val: string) => Array.from(serializeNodeId(val));
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

      const bulkSql = "SELECT treecrdt_append_ops(?1)";
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const bulkStmt: any = await db.prepare(bulkSql);
      try {
        await db.bind(bulkStmt, 1, JSON.stringify(payload));
        await db.step(bulkStmt);
        await db.finalize(bulkStmt);
        return;
      } catch {
        await db.finalize(bulkStmt);
        // Fallback to per-op inserts inside one transaction.
      }

      await db.exec("BEGIN");
      try {
        for (const op of ops) {
          const { meta, kind } = op;
          const { id, lamport } = meta;
          const { replica, counter } = id;
          const { sql, params } = buildAppendOp(kind, {
            replica: serializeReplica(replica),
            counter,
            lamport,
            serializeNodeId,
          });
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const stmt: any = await db.prepare(sql);
          let idx = 1;
          for (const p of params) {
            await db.bind(stmt, idx++, p);
          }
          await db.step(stmt);
          await db.finalize(stmt);
        }
        await db.exec("COMMIT");
      } catch (err) {
        await db.exec("ROLLBACK");
        throw err;
      }
    },
    opsSince: (lamport, root) => opsSince(db, { lamport, root }),
  };
}

import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildAppendOp, buildOpsSince } from "@treecrdt/interface/sqlite";
import type { Operation, TreecrdtAdapter } from "@treecrdt/interface";
import { nodeIdToBytes16 } from "@treecrdt/interface/ids";

export type LoadOptions = {
  extensionPath?: string;
  entrypoint?: string;
};

// Minimal shape we need from better-sqlite3 Database.
export type LoadableDatabase = {
  loadExtension: (path: string, entryPoint?: string) => void;
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function platformExt(): ".dylib" | ".so" | ".dll" {
  switch (process.platform) {
    case "darwin":
      return ".dylib";
    case "win32":
      return ".dll";
    default:
      return ".so";
  }
}

/**
 * Resolve the bundled TreeCRDT SQLite extension for this platform.
 * Falls back to the `native/` directory within this package.
 */
export function defaultExtensionPath(): string {
  const ext = platformExt();
  const base =
    ext === ".dll" ? "treecrdt_sqlite_ext" : "libtreecrdt_sqlite_ext";
  return path.resolve(__dirname, "../native", `${base}${ext}`);
}

/**
 * Load the TreeCRDT SQLite extension into a better-sqlite3 Database.
 */
export function loadTreecrdtExtension(
  db: LoadableDatabase,
  opts: LoadOptions = {}
): string {
  const path = opts.extensionPath ?? defaultExtensionPath();
  const entrypoint = opts.entrypoint ?? "sqlite3_treecrdt_init";
  db.loadExtension(path, entrypoint);
  return path;
}

export function createSqliteNodeAdapter(db: any): TreecrdtAdapter {
  const stmtCache = new Map<string, any>();
  const prepare = (sql: string) => {
    const cached = stmtCache.get(sql);
    if (cached) return cached;
    const stmt = db.prepare(sql);
    stmtCache.set(sql, stmt);
    return stmt;
  };

  return {
    appendOp: async (op: Operation, serializeNodeId, serializeReplica) => {
      const { meta, kind } = op;
      const { id, lamport } = meta;
      const { replica, counter } = id;
      const { sql, params } = buildAppendOp(kind, {
        replica: serializeReplica(replica),
        counter,
        lamport,
        serializeNodeId,
      });
      const bindings = params.reduce<Record<number, unknown>>((acc, val, idx) => {
        acc[idx + 1] = val;
        return acc;
      }, {});
      prepare(sql).get(bindings);
    },
    appendOps: async (
      ops: Operation[],
      serializeNodeId,
      serializeReplica
    ) => {
      if (ops.length === 0) return;
      // Prefer the extension bulk entrypoint when available.
      const maxBulkOps = 5_000;
      const bulkSql = "SELECT treecrdt_append_ops(?1)";
      const serialize = (val: string) => Array.from(serializeNodeId(val));

      let bulkFailedAt: number | null = null;
      for (let start = 0; start < ops.length; start += maxBulkOps) {
        const chunk = ops.slice(start, start + maxBulkOps);
        const payload = chunk.map((op) => {
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
            return {
              ...base,
              parent: null,
              node: serialize(kind.node),
              new_parent: serialize(kind.newParent),
            };
          } else if (kind.type === "delete") {
            return { ...base, parent: null, node: serialize(kind.node), new_parent: null };
          }
          return { ...base, parent: null, node: serialize(kind.node), new_parent: null };
        });
        try {
          prepare(bulkSql).get({ 1: JSON.stringify(payload) });
        } catch {
          bulkFailedAt = start;
          break;
        }
      }
      if (bulkFailedAt === null) return;

      const runMany = db.transaction((batch: Operation[]) => {
        for (const op of batch) {
          const { meta, kind } = op;
          const { id, lamport } = meta;
          const { replica, counter } = id;
          const { sql, params: bindParams } = buildAppendOp(kind, {
            replica: serializeReplica(replica),
            counter,
            lamport,
            serializeNodeId,
          });
          const bindings = bindParams.reduce<Record<number, unknown>>(
            (acc, val, idx) => {
              acc[idx + 1] = val;
              return acc;
            },
            {}
          );
          prepare(sql).get(bindings);
        }
      });
      runMany(ops.slice(bulkFailedAt));
    },
    opsSince: async (lamport: number, root?: string) => {
      const { sql, params } = buildOpsSince({
        lamport,
        root,
        serializeNodeId: nodeIdToBytes16,
      });
      const bindings = params.reduce<Record<number, unknown>>((acc, val, idx) => {
        acc[idx + 1] = val;
        return acc;
      }, {});
      const row = prepare(sql).get(bindings);
      const json = row?.ops ?? row?.["treecrdt_ops_since(0)"] ?? Object.values(row ?? {})[0];
      return JSON.parse(json);
    },
  };
}

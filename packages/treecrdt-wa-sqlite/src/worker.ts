/// <reference lib="webworker" />
import { createWaSqliteAdapter, opsSince as opsSinceRaw, appendOp as appendOpRaw, type Database } from "./index.js";
import { createOpfsVfs } from "./opfs.js";
import type { Operation } from "@treecrdt/interface";

const encoder = new TextEncoder();
let db: Database | null = null;
let storage: "memory" | "opfs" = "memory";

self.onmessage = async (ev: MessageEvent) => {
  const { id, method, params } = ev.data as { id: number; method: string; params?: any };
  const respond = (ok: boolean, result?: any, error?: string) => {
    (self as unknown as Worker).postMessage({ id, ok, result, error });
  };

  try {
    if (method === "init") {
      await handleInit(params as { baseUrl: string; filename?: string; storage: "memory" | "opfs" });
      respond(true, { storage });
      return;
    }
    if (method === "append") {
      await ensureDb();
      await appendOpRaw(db!, (params as any).op as Operation, encodeNodeId, encodeReplica);
      respond(true, null);
      return;
    }
    if (method === "appendMany") {
      await ensureDb();
      const ops = (params as any).ops as Operation[];
      const adapter = createWaSqliteAdapter(db!);
      if (adapter.appendOps) {
        await adapter.appendOps(ops, encodeNodeId, encodeReplica);
      } else {
        for (const op of ops) {
          await appendOpRaw(db!, op, encodeNodeId, encodeReplica);
        }
      }
      respond(true, null);
      return;
    }
    if (method === "opsSince") {
      await ensureDb();
      const lamport = (params as any).lamport as number;
      const rows = await opsSinceRaw(db!, { lamport });
      respond(true, rows);
      return;
    }
    if (method === "close") {
      if (db?.close) await db.close();
      db = null;
      respond(true, null);
      return;
    }
    respond(false, null, "unknown method");
  } catch (err) {
    respond(false, null, err instanceof Error ? err.message : String(err));
  }
};

async function handleInit(opts: { baseUrl: string; filename?: string; storage: "memory" | "opfs" }) {
  if (db) {
    if (db.close) await db.close();
    db = null;
  }
  storage = opts.storage;
  const base = opts.baseUrl;
  const sqliteModule = await import(/* @vite-ignore */ `${base}wa-sqlite/wa-sqlite-async.mjs`);
  const sqliteApi = await import(/* @vite-ignore */ `${base}wa-sqlite/sqlite-api.js`);

  const module = await sqliteModule.default({
    locateFile: (file: string) => (file.endsWith(".wasm") ? `${base}wa-sqlite/wa-sqlite-async.wasm` : file),
  });
  const sqlite3 = sqliteApi.Factory(module);

  if (storage === "opfs") {
    try {
      const vfs = await createOpfsVfs(module, { name: "opfs" });
      sqlite3.vfs_register(vfs, true);
    } catch {
      storage = "memory";
    }
  }

  const filename = storage === "opfs" ? opts.filename ?? "/treecrdt.db" : ":memory:";
  const handle = await sqlite3.open_v2(filename);
  db = makeDbAdapter(sqlite3, handle);
}

async function ensureDb() {
  if (!db) throw new Error("db not initialized");
}

function encodeNodeId(id: string): Uint8Array {
  const clean = id.startsWith("0x") ? id.slice(2) : id;
  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < clean.length; i += 2) {
    out[i / 2] = parseInt(clean.slice(i, i + 2), 16);
  }
  return out;
}

function encodeReplica(replica: Operation["meta"]["id"]["replica"]): Uint8Array {
  return typeof replica === "string" ? encoder.encode(replica) : replica;
}

function makeDbAdapter(sqlite3: any, handle: number): Database {
  const prepare = async (sql: string) => {
    const iter = sqlite3.statements(handle, sql, { unscoped: true });
    const { value } = await iter.next();
    if (!value) {
      throw new Error(`Failed to prepare statement: ${sql}`);
    }
    return value;
  };

  return {
    prepare,
    bind: async (stmt: number, index: number, value: unknown) => sqlite3.bind(stmt, index, value),
    step: async (stmt: number) => sqlite3.step(stmt),
    column_text: async (stmt: number, index: number) => sqlite3.column_text(stmt, index),
    finalize: async (stmt: number) => sqlite3.finalize(stmt),
    exec: async (sql: string) => sqlite3.exec(handle, sql),
    close: async () => sqlite3.close(handle),
  } as unknown as Database;
}

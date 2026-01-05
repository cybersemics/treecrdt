/// <reference lib="webworker" />
import {
  createWaSqliteAdapter,
  opsSince as opsSinceRaw,
  appendOp as appendOpRaw,
  opRefsAll as opRefsAllRaw,
  opRefsChildren as opRefsChildrenRaw,
  opsByOpRefs as opsByOpRefsRaw,
  treeChildren as treeChildrenRaw,
  treeDump as treeDumpRaw,
  treeNodeCount as treeNodeCountRaw,
  headLamport as headLamportRaw,
  replicaMaxCounter as replicaMaxCounterRaw,
  setDocId as setDocIdRaw,
  type Database,
} from "./index.js";
import { createOpfsVfs } from "./opfs.js";
import type { Operation } from "@treecrdt/interface";
import { nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";

let db: Database | null = null;
let storage: "memory" | "opfs" = "memory";

const methods = {
  init,
  append,
  appendMany,
  opsSince,
  opRefsAll,
  opRefsChildren,
  opsByOpRefs,
  treeChildren,
  treeDump,
  treeNodeCount,
  headLamport,
  replicaMaxCounter,
  close,
} as const;

self.onmessage = async (ev: MessageEvent) => {
  const { id, method, params } = ev.data as { id: number; method: string; params?: any[] };
  const respond = (ok: boolean, result?: any, error?: string) => {
    (self as unknown as Worker).postMessage({ id, ok, result, error });
  };

  try {
    const methodFn = methods[method as keyof typeof methods];
    if (!methodFn) {
      respond(false, null, "unknown method");
      return;
    }
    const result = await (methodFn as (...args: any[]) => Promise<any>)(...(params ?? []));
    respond(true, result);
  } catch (err) {
    respond(false, null, err instanceof Error ? err.message : String(err));
  }
};

async function init(
  baseUrl: string,
  filename: string | undefined,
  storageParam: "memory" | "opfs",
  docId: string
): Promise<{ storage: "memory" | "opfs"; opfsError?: string }> {
  if (db) {
    if (db.close) await db.close();
    db = null;
  }
  storage = storageParam;
  let opfsError: string | undefined;
  const base = baseUrl;
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
    } catch (err) {
      opfsError = err instanceof Error ? err.message : String(err);
      storage = "memory";
    }
  }

  const dbFilename = storage === "opfs" ? filename ?? "/treecrdt.db" : ":memory:";
  const handle = await sqlite3.open_v2(dbFilename);
  db = makeDbAdapter(sqlite3, handle);
  await setDocIdRaw(db, docId);
  return opfsError ? { storage, opfsError } : { storage };
}

async function append(op: Operation) {
  await ensureDb();
  await appendOpRaw(db!, op, nodeIdToBytes16, replicaIdToBytes);
  return null;
}

async function appendMany(ops: Operation[]) {
  await ensureDb();
  const adapter = createWaSqliteAdapter(db!);
  if (adapter.appendOps) {
    await adapter.appendOps(ops, nodeIdToBytes16, replicaIdToBytes);
  } else {
    for (const op of ops) {
      await appendOpRaw(db!, op, nodeIdToBytes16, replicaIdToBytes);
    }
  }
  return null;
}

async function opsSince(lamport: number, root: string | undefined) {
  await ensureDb();
  return await opsSinceRaw(db!, { lamport, root });
}

async function opRefsAll() {
  await ensureDb();
  return await opRefsAllRaw(db!);
}

async function opRefsChildren(parent: string) {
  await ensureDb();
  return await opRefsChildrenRaw(db!, nodeIdToBytes16(parent));
}

async function opsByOpRefs(opRefs: number[][]) {
  await ensureDb();
  const opRefsArray = opRefs.map((r) => Uint8Array.from(r));
  return await opsByOpRefsRaw(db!, opRefsArray);
}

async function treeChildren(parent: string) {
  await ensureDb();
  return await treeChildrenRaw(db!, nodeIdToBytes16(parent));
}

async function treeDump() {
  await ensureDb();
  return await treeDumpRaw(db!);
}

async function treeNodeCount() {
  await ensureDb();
  return await treeNodeCountRaw(db!);
}

async function headLamport() {
  await ensureDb();
  return await headLamportRaw(db!);
}

async function replicaMaxCounter(replica: number[] | string) {
  await ensureDb();
  const replicaBytes =
    typeof replica === "string" ? replicaIdToBytes(replica) : Uint8Array.from(replica);
  return await replicaMaxCounterRaw(db!, replicaBytes);
}

async function close() {
  if (db?.close) await db.close();
  db = null;
  return null;
}

async function ensureDb() {
  if (!db) throw new Error("db not initialized");
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

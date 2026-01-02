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
import type {
  RpcMethod,
  RpcParams,
  RpcRequest,
  RpcResponse,
  RpcResult,
} from "./rpc.js";

let db: Database | null = null;
let storage: "memory" | "opfs" = "memory";

type HandlerMap = { [M in RpcMethod]: (params: RpcParams<M>) => Promise<RpcResult<M>> };

function postResponse(resp: RpcResponse) {
  (self as unknown as Worker).postMessage(resp);
}

function makeOk<M extends RpcMethod>(id: number, result: RpcResult<M>): RpcResponse<M> {
  return { id, ok: true, result };
}

function makeErr(id: number, err: unknown): RpcResponse {
  return { id, ok: false, error: err instanceof Error ? err.message : String(err) };
}

function withDb<T>(fn: (db: Database) => Promise<T>): Promise<T> {
  if (!db) return Promise.reject(new Error("db not initialized"));
  return fn(db);
}

const handlers: HandlerMap = {
  init: (params) => handleInit(params),
  append: (params) =>
    withDb(async (db) => {
      await appendOpRaw(db, params.op, nodeIdToBytes16, replicaIdToBytes);
      return;
    }),
  appendMany: (params) =>
    withDb(async (db) => {
      const adapter = createWaSqliteAdapter(db);
      if (adapter.appendOps) {
        await adapter.appendOps(params.ops, nodeIdToBytes16, replicaIdToBytes);
      } else {
        for (const op of params.ops) {
          await appendOpRaw(db, op, nodeIdToBytes16, replicaIdToBytes);
        }
      }
      return;
    }),
  opsSince: (params) => withDb((db) => opsSinceRaw(db, params)),
  opRefsAll: () => withDb((db) => opRefsAllRaw(db)),
  opRefsChildren: (params) => withDb((db) => opRefsChildrenRaw(db, nodeIdToBytes16(params.parent))),
  opsByOpRefs: (params) =>
    withDb((db) => opsByOpRefsRaw(db, params.opRefs.map((r) => Uint8Array.from(r)))),
  treeChildren: (params) => withDb((db) => treeChildrenRaw(db, nodeIdToBytes16(params.parent))),
  treeDump: () => withDb((db) => treeDumpRaw(db)),
  treeNodeCount: () => withDb((db) => treeNodeCountRaw(db)),
  headLamport: () => withDb((db) => headLamportRaw(db)),
  replicaMaxCounter: (params) =>
    withDb((db) => {
      const replica =
        typeof params.replica === "string"
          ? replicaIdToBytes(params.replica)
          : Uint8Array.from(params.replica);
      return replicaMaxCounterRaw(db, replica);
    }),
  close: async () => {
    if (db?.close) await db.close();
    db = null;
    return;
  },
};

self.onmessage = async (ev: MessageEvent<RpcRequest>) => {
  const { id, method, params } = ev.data;
  const handler = (handlers as Record<string, (p: unknown) => Promise<unknown>>)[method];
  if (!handler) {
    postResponse({ id, ok: false, error: `unknown method: ${method}` });
    return;
  }
  try {
    const result = await handler(params);
    postResponse(makeOk(id, result as any));
  } catch (err) {
    postResponse(makeErr(id, err));
  }
};

async function handleInit(opts: {
  baseUrl: string;
  filename?: string;
  storage: "memory" | "opfs";
  docId: string;
}): Promise<{ storage: "memory" | "opfs"; opfsError?: string }> {
  if (db) {
    if (db.close) await db.close();
    db = null;
  }
  storage = opts.storage;
  let opfsError: string | undefined;
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
    } catch (err) {
      opfsError = err instanceof Error ? err.message : String(err);
      storage = "memory";
    }
  }

  const filename = storage === "opfs" ? opts.filename ?? "/treecrdt.db" : ":memory:";
  const handle = await sqlite3.open_v2(filename);
  db = makeDbAdapter(sqlite3, handle);
  await setDocIdRaw(db, opts.docId);
  return opfsError ? { storage, opfsError } : { storage };
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

/// <reference lib="webworker" />
import {
  type Database,
} from "./index.js";
import { bytesToHex, nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";
import type { Operation, ReplicaId } from "@treecrdt/interface";
import type { TreecrdtAdapter } from "@treecrdt/interface";
import {
  createTreecrdtSqliteWriter,
  type SqliteRunner,
  type TreecrdtSqlitePlacement,
  type TreecrdtSqliteWriter,
} from "@treecrdt/interface/sqlite";
import type { RpcMethod, RpcRequest, RpcSqlParams } from "./rpc.js";
import { openTreecrdtDb } from "./open.js";

let db: Database | null = null;
let api: TreecrdtAdapter | null = null;
let runner: SqliteRunner | null = null;
const localWriters = new Map<string, TreecrdtSqliteWriter>();

const methods = {
  init,
  sqlExec,
  sqlGetText,
  append,
  appendMany,
  opsSince,
  opRefsAll,
  opRefsChildren,
  opsByOpRefs,
  treeChildren,
  treeChildrenPage,
  treeDump,
  treeNodeCount,
  headLamport,
  replicaMaxCounter,
  localInsert,
  localMove,
  localDelete,
  localPayload,
  close,
} as const;

self.onmessage = async (ev: MessageEvent<RpcRequest>) => {
  const { id, method, params } = ev.data;
  const respond = (ok: boolean, result?: any, error?: string) => {
    (self as unknown as Worker).postMessage({ id, ok, result, error });
  };

  try {
    const methodFn = (methods as Record<RpcMethod, (...args: any[]) => Promise<any>>)[method];
    if (!methodFn) {
      respond(false, null, `unknown method: ${method}`);
      return;
    }
    const result = await methodFn(...(params ?? []));
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
    api = null;
    runner = null;
  }
  const opened = await openTreecrdtDb({
    baseUrl,
    filename,
    storage: storageParam,
    docId,
    requireOpfs: false,
  });
  db = opened.db;
  api = opened.api;
  runner = makeRunner(opened.db);
  localWriters.clear();
  return opened.opfsError ? { storage: opened.storage, opfsError: opened.opfsError } : { storage: opened.storage };
}

async function sqlExec(sql: string) {
  const db = ensureDb();
  await db.exec(sql);
  return null;
}

async function sqlGetText(sql: string, params?: RpcSqlParams): Promise<string | null> {
  return dbGetText(ensureDb(), sql, params ?? []);
}

async function append(op: Operation) {
  const api = ensureApi();
  await api.appendOp(op, nodeIdToBytes16, replicaIdToBytes);
  return null;
}

async function appendMany(ops: Operation[]) {
  const api = ensureApi();
  await api.appendOps!(ops, nodeIdToBytes16, replicaIdToBytes);
  return null;
}

async function opsSince(lamport: number, root: string | undefined) {
  const api = ensureApi();
  return await api.opsSince(lamport, root);
}

async function opRefsAll() {
  const api = ensureApi();
  return await api.opRefsAll();
}

async function opRefsChildren(parent: string) {
  const api = ensureApi();
  return await api.opRefsChildren(nodeIdToBytes16(parent));
}

async function opsByOpRefs(opRefs: number[][]) {
  const api = ensureApi();
  const opRefsArray = opRefs.map((r) => Uint8Array.from(r));
  return await api.opsByOpRefs(opRefsArray);
}

async function treeChildren(parent: string) {
  const api = ensureApi();
  return await api.treeChildren(nodeIdToBytes16(parent));
}

async function treeChildrenPage(
  parent: string,
  cursor: { orderKey: number[]; node: number[] } | null,
  limit: number
) {
  const api = ensureApi();
  const cursorBytes = cursor
    ? {
        orderKey: Uint8Array.from(cursor.orderKey),
        node: Uint8Array.from(cursor.node),
      }
    : null;
  return await api.treeChildrenPage!(nodeIdToBytes16(parent), cursorBytes, limit);
}

async function treeDump() {
  const api = ensureApi();
  return await api.treeDump();
}

async function treeNodeCount() {
  const api = ensureApi();
  return await api.treeNodeCount();
}

async function headLamport() {
  const api = ensureApi();
  return await api.headLamport();
}

async function replicaMaxCounter(replica: number[] | string) {
  const api = ensureApi();
  const replicaBytes = typeof replica === "string" ? replicaIdToBytes(replica) : Uint8Array.from(replica);
  return await api.replicaMaxCounter(replicaBytes);
}

async function localInsert(
  replica: number[] | string,
  parent: string,
  node: string,
  placement: TreecrdtSqlitePlacement,
  payload: Uint8Array | null
) {
  const writer = ensureLocalWriter(normalizeReplica(replica));
  return await writer.insert(parent, node, placement, payload ? { payload } : {});
}

async function localMove(replica: number[] | string, node: string, newParent: string, placement: TreecrdtSqlitePlacement) {
  const writer = ensureLocalWriter(normalizeReplica(replica));
  return await writer.move(node, newParent, placement);
}

async function localDelete(replica: number[] | string, node: string) {
  const writer = ensureLocalWriter(normalizeReplica(replica));
  return await writer.delete(node);
}

async function localPayload(replica: number[] | string, node: string, payload: Uint8Array | null) {
  const writer = ensureLocalWriter(normalizeReplica(replica));
  return await writer.payload(node, payload);
}

async function close() {
  if (db?.close) await db.close();
  db = null;
  api = null;
  runner = null;
  localWriters.clear();
  return null;
}

function ensureApi(): TreecrdtAdapter {
  if (!db || !api) throw new Error("db not initialized");
  return api;
}

function ensureDb(): Database {
  if (!db) throw new Error("db not initialized");
  return db;
}

function ensureRunner(): SqliteRunner {
  if (!runner) throw new Error("db not initialized");
  return runner;
}

function makeRunner(db: Database): SqliteRunner {
  return {
    exec: (sql) => db.exec(sql),
    getText: (sql, params = []) => dbGetText(db, sql, params),
  };
}

function replicaKey(replica: ReplicaId): string {
  return typeof replica === "string" ? replica : bytesToHex(replica);
}

function normalizeReplica(replica: number[] | string): ReplicaId {
  return typeof replica === "string" ? replica : Uint8Array.from(replica);
}

function ensureLocalWriter(replica: ReplicaId): TreecrdtSqliteWriter {
  const key = replicaKey(replica);
  const existing = localWriters.get(key);
  if (existing) return existing;
  const writer = createTreecrdtSqliteWriter(ensureRunner(), { replica });
  localWriters.set(key, writer);
  return writer;
}

async function dbGetText(db: Database, sql: string, params: RpcSqlParams = []): Promise<string | null> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stmt: any = await db.prepare(sql);
  try {
    let idx = 1;
    for (const p of params) {
      await db.bind(stmt, idx++, p);
    }
    const row = await db.step(stmt);
    if (row === 0) return null;
    return await db.column_text(stmt, 0);
  } finally {
    await db.finalize(stmt);
  }
}

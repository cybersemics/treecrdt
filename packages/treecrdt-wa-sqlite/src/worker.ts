/// <reference lib="webworker" />
import { dbGetText } from './sql.js';
import type { Database } from './index.js';
import { nodeIdToBytes16, replicaIdToBytes } from '@justthrowaway/interface/ids';
import type { Operation } from '@justthrowaway/interface';
import type { TreecrdtAdapter } from '@justthrowaway/interface';
import type { MaterializationEvent } from '@justthrowaway/interface/engine';
import type { RpcInitResult, RpcMethod, RpcRequest, RpcSqlParams } from './rpc.js';
import { openTreecrdtDb } from './open.js';
import { clearOpfsStorage } from './opfs.js';

let db: Database | null = null;
let storedFilename: string | undefined;
let storedStorage: 'memory' | 'opfs' = 'memory';
let api: TreecrdtAdapter | null = null;

function postMaterialized(event: MaterializationEvent) {
  (self as unknown as Worker).postMessage({ type: 'materialized', event });
}

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
  treeParent,
  treeExists,
  treePayload,
  headLamport,
  replicaMaxCounter,
  close,
  drop,
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
  storageParam: 'memory' | 'opfs',
  docId: string,
): Promise<RpcInitResult> {
  if (db) {
    if (db.close) await db.close();
    db = null;
    api = null;
  }
  const opened = await openTreecrdtDb({
    baseUrl,
    filename,
    storage: storageParam,
    docId,
    requireOpfs: false,
    onMaterialized: postMaterialized,
  });
  db = opened.db;
  api = opened.api;
  storedFilename = opened.filename;
  storedStorage = opened.storage;
  return opened.opfsError
    ? { storage: opened.storage, filename: opened.filename, opfsError: opened.opfsError }
    : { storage: opened.storage, filename: opened.filename };
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
  return await api.appendOp(op, nodeIdToBytes16, replicaIdToBytes);
}

async function appendMany(ops: Operation[]) {
  const api = ensureApi();
  return await api.appendOps!(ops, nodeIdToBytes16, replicaIdToBytes);
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
  limit: number,
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

async function treePayload(node: string) {
  const api = ensureApi();
  const payload = await api.treePayload(nodeIdToBytes16(node));
  return payload === null ? null : Array.from(payload);
}

async function treeNodeCount() {
  const api = ensureApi();
  return await api.treeNodeCount();
}

async function treeParent(node: string) {
  const api = ensureApi();
  const result = await api.treeParent(nodeIdToBytes16(node));
  return result === null ? null : Array.from(result);
}

async function treeExists(node: string) {
  const api = ensureApi();
  return await api.treeExists(nodeIdToBytes16(node));
}

async function headLamport() {
  const api = ensureApi();
  return await api.headLamport();
}

async function replicaMaxCounter(replica: number[]) {
  const api = ensureApi();
  return await api.replicaMaxCounter(Uint8Array.from(replica));
}

async function closeDbAndReset() {
  if (db?.close) await db.close();
  db = null;
  api = null;
  storedFilename = undefined;
  storedStorage = 'memory';
}

async function close() {
  await closeDbAndReset();
  return null;
}

async function drop() {
  const filename = storedFilename;
  const storage = storedStorage;
  await closeDbAndReset();
  if (storage === 'opfs' && filename) {
    await clearOpfsStorage(filename);
  }
  return null;
}

function ensureApi(): TreecrdtAdapter {
  if (!db || !api) throw new Error('db not initialized');
  return api;
}

function ensureDb(): Database {
  if (!db) throw new Error('db not initialized');
  return db;
}

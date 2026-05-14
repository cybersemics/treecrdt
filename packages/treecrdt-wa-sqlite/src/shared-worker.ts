/// <reference lib="webworker" />
import { dbGetText } from './sql.js';
import type { Database } from './index.js';
import { nodeIdToBytes16, replicaIdToBytes } from '@justthrowaway/interface/ids';
import type { Operation, TreecrdtAdapter } from '@justthrowaway/interface';
import type { MaterializationEvent } from '@justthrowaway/interface/engine';
import type {
  RpcInitResult,
  RpcMethod,
  RpcParams,
  RpcRequest,
  RpcResult,
  RpcSqlParams,
} from './rpc.js';
import { openTreecrdtDb } from './open.js';
import { clearOpfsStorage } from './opfs.js';

type SharedWorkerGlobal = typeof globalThis & {
  onconnect: ((ev: MessageEvent) => void) | null;
};

type StoredConfig = {
  baseUrl: string;
  requestedFilename: string;
  requestedStorage: 'memory' | 'opfs';
  docId: string;
};

const ports = new Set<MessagePort>();
let db: Database | null = null;
let api: TreecrdtAdapter | null = null;
let storedFilename: string | undefined;
let storedStorage: 'memory' | 'opfs' = 'memory';
let storedConfig: StoredConfig | null = null;
let initResult: RpcInitResult | null = null;
let callQueue: Promise<void> = Promise.resolve();

const settleQueue = <T>(promise: Promise<T>): Promise<void> =>
  promise.then(
    () => undefined,
    () => undefined,
  );

function broadcastMaterialized(event: MaterializationEvent, exclude?: MessagePort) {
  if (event.changes.length === 0) return;
  for (const port of ports) {
    if (port === exclude) continue;
    port.postMessage({ type: 'materialized', event });
  }
}

(self as unknown as SharedWorkerGlobal).onconnect = (ev: MessageEvent) => {
  const port = ev.ports[0];
  if (!port) return;
  ports.add(port);
  port.onmessage = (message: MessageEvent<RpcRequest>) => {
    const request = message.data;
    const respond = (ok: boolean, result?: any, error?: string) => {
      port.postMessage({ id: request.id, ok, result, error });
    };
    const run = callQueue.then(() => handleRequest(port, request));
    callQueue = settleQueue(run);
    run.then(
      (result) => respond(true, result),
      (err) => respond(false, null, err instanceof Error ? err.message : String(err)),
    );
  };
  port.start();
};

async function handleRequest<M extends RpcMethod>(
  sourcePort: MessagePort,
  request: RpcRequest<M>,
): Promise<RpcResult<M> | void> {
  if (request.method === 'init') {
    const [baseUrl, filename, storage, docId] = request.params as RpcParams<'init'>;
    return (await init(baseUrl, filename, storage, docId)) as RpcResult<M>;
  }

  if (request.method === 'broadcastMaterialized') {
    const [event] = request.params as RpcParams<'broadcastMaterialized'>;
    broadcastMaterialized(event, sourcePort);
    return undefined;
  }

  if (request.method === 'close') {
    await close(sourcePort);
    return undefined;
  }

  if (request.method === 'drop') {
    await drop();
    return undefined;
  }

  const methodFn = methods[request.method as keyof typeof methods] as
    | ((...args: any[]) => Promise<unknown>)
    | undefined;
  if (!methodFn) throw new Error(`unknown method: ${request.method}`);
  return (await methodFn(...((request.params ?? []) as any[]))) as RpcResult<M>;
}

async function init(
  baseUrl: string,
  filename: string | undefined,
  storageParam: 'memory' | 'opfs',
  docId: string,
): Promise<RpcInitResult> {
  const requestedFilename = storageParam === 'opfs' ? (filename ?? '/treecrdt.db') : ':memory:';
  if (storedConfig && initResult) {
    if (
      storedConfig.baseUrl !== baseUrl ||
      storedConfig.requestedFilename !== requestedFilename ||
      storedConfig.requestedStorage !== storageParam ||
      storedConfig.docId !== docId
    ) {
      throw new Error('shared worker already initialized with a different TreeCRDT database');
    }
    return initResult;
  }

  const opened = await openTreecrdtDb({
    baseUrl,
    filename,
    storage: storageParam,
    docId,
    requireOpfs: false,
    opfsVfs: storageParam === 'opfs' ? 'any-context' : undefined,
    onMaterialized: (event) => broadcastMaterialized(event),
  });
  db = opened.db;
  api = opened.api;
  storedFilename = opened.filename;
  storedStorage = opened.storage;
  storedConfig = { baseUrl, requestedFilename, requestedStorage: storageParam, docId };
  initResult = opened.opfsError
    ? { storage: opened.storage, filename: opened.filename, opfsError: opened.opfsError }
    : { storage: opened.storage, filename: opened.filename };
  return initResult;
}

const methods = {
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
} as const;

async function sqlExec(sql: string) {
  await ensureDb().exec(sql);
  return null;
}

async function sqlGetText(sql: string, params?: RpcSqlParams): Promise<string | null> {
  return dbGetText(ensureDb(), sql, params ?? []);
}

async function append(op: Operation) {
  return await ensureApi().appendOp(op, nodeIdToBytes16, replicaIdToBytes);
}

async function appendMany(ops: Operation[]) {
  return await ensureApi().appendOps!(ops, nodeIdToBytes16, replicaIdToBytes);
}

async function opsSince(lamport: number, root: string | undefined) {
  return await ensureApi().opsSince(lamport, root);
}

async function opRefsAll() {
  return await ensureApi().opRefsAll();
}

async function opRefsChildren(parent: string) {
  return await ensureApi().opRefsChildren(nodeIdToBytes16(parent));
}

async function opsByOpRefs(opRefs: number[][]) {
  return await ensureApi().opsByOpRefs(opRefs.map((r) => Uint8Array.from(r)));
}

async function treeChildren(parent: string) {
  return await ensureApi().treeChildren(nodeIdToBytes16(parent));
}

async function treeChildrenPage(
  parent: string,
  cursor: { orderKey: number[]; node: number[] } | null,
  limit: number,
) {
  const cursorBytes = cursor
    ? {
        orderKey: Uint8Array.from(cursor.orderKey),
        node: Uint8Array.from(cursor.node),
      }
    : null;
  return await ensureApi().treeChildrenPage!(nodeIdToBytes16(parent), cursorBytes, limit);
}

async function treeDump() {
  return await ensureApi().treeDump();
}

async function treeNodeCount() {
  return await ensureApi().treeNodeCount();
}

async function treeParent(node: string) {
  const result = await ensureApi().treeParent(nodeIdToBytes16(node));
  return result === null ? null : Array.from(result);
}

async function treeExists(node: string) {
  return await ensureApi().treeExists(nodeIdToBytes16(node));
}

async function treePayload(node: string) {
  const payload = await ensureApi().treePayload(nodeIdToBytes16(node));
  return payload === null ? null : Array.from(payload);
}

async function headLamport() {
  return await ensureApi().headLamport();
}

async function replicaMaxCounter(replica: number[]) {
  return await ensureApi().replicaMaxCounter(Uint8Array.from(replica));
}

async function close(port: MessagePort) {
  ports.delete(port);
  if (ports.size > 0) return;
  await closeDbAndReset();
}

async function drop() {
  const filename = storedFilename;
  const storage = storedStorage;
  await closeDbAndReset();
  if (storage === 'opfs' && filename) {
    await clearOpfsStorage(filename);
  }
}

async function closeDbAndReset() {
  if (db?.close) await db.close();
  db = null;
  api = null;
  storedFilename = undefined;
  storedStorage = 'memory';
  storedConfig = null;
  initResult = null;
}

function ensureApi(): TreecrdtAdapter {
  if (!db || !api) throw new Error('db not initialized');
  return api;
}

function ensureDb(): Database {
  if (!db) throw new Error('db not initialized');
  return db;
}

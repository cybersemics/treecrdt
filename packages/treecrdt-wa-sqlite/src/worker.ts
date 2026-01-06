/// <reference lib="webworker" />
import {
  type Database,
} from "./index.js";
import { nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";
import type { Operation } from "@treecrdt/interface";
import type { TreecrdtAdapter } from "@treecrdt/interface";
import type { RpcMethod, RpcRequest } from "./rpc.js";
import { openTreecrdtDb } from "./open.js";

let db: Database | null = null;
let api: TreecrdtAdapter | null = null;
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
  storage = opened.storage;
  return opened.opfsError ? { storage: opened.storage, opfsError: opened.opfsError } : { storage: opened.storage };
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

async function close() {
  if (db?.close) await db.close();
  db = null;
  api = null;
  return null;
}

function ensureApi(): TreecrdtAdapter {
  if (!db || !api) throw new Error("db not initialized");
  return api;
}

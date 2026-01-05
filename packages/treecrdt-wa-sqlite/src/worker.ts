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
  type Database,
} from "./index.js";
import { nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";
import type { Operation } from "@treecrdt/interface";
import type { RpcMethod, RpcRequest } from "./rpc.js";
import { openTreecrdtDb } from "./open.js";

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
  }
  const opened = await openTreecrdtDb({
    baseUrl,
    filename,
    storage: storageParam,
    docId,
    requireOpfs: false,
  });
  db = opened.db;
  storage = opened.storage;
  return opened.opfsError ? { storage: opened.storage, opfsError: opened.opfsError } : { storage: opened.storage };
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
  const replicaBytes = typeof replica === "string" ? replicaIdToBytes(replica) : Uint8Array.from(replica);
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

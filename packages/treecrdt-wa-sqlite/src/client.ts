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
} from "./index.js";
import { detectOpfsSupport } from "./opfs.js";
import type { Operation } from "@treecrdt/interface";
import {
  decodeSqliteNodeIds,
  decodeSqliteOpRefs,
  decodeSqliteOps,
  decodeSqliteTreeRows,
} from "@treecrdt/interface/sqlite";
import { nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";
import type { RpcMethod, RpcParams, RpcRequest, RpcResponse, RpcResult } from "./rpc.js";
import { openTreecrdtDb } from "./open.js";

export type StorageMode = "memory" | "opfs";
export type ClientMode = "direct" | "worker";

export type TreecrdtOpsApi = {
  append: (op: Operation) => Promise<void>;
  appendMany: (ops: Operation[]) => Promise<void>;
  all: () => Promise<Operation[]>;
  since: (lamport: number, root?: string) => Promise<Operation[]>;
  children: (parent: string) => Promise<Operation[]>;
  get: (opRefs: Uint8Array[]) => Promise<Operation[]>;
};

export type TreecrdtOpRefsApi = {
  all: () => Promise<Uint8Array[]>;
  children: (parent: string) => Promise<Uint8Array[]>;
};

export type TreeNodeRow = {
  node: string;
  parent: string | null;
  pos: number | null;
  tombstone: boolean;
};

export type TreecrdtTreeApi = {
  children: (parent: string) => Promise<string[]>;
  dump: () => Promise<TreeNodeRow[]>;
  nodeCount: () => Promise<number>;
};

export type TreecrdtMetaApi = {
  headLamport: () => Promise<number>;
  replicaMaxCounter: (replica: Operation["meta"]["id"]["replica"]) => Promise<number>;
};

export type TreecrdtClient = {
  mode: ClientMode;
  storage: StorageMode;
  docId: string;
  ops: TreecrdtOpsApi;
  opRefs: TreecrdtOpRefsApi;
  tree: TreecrdtTreeApi;
  meta: TreecrdtMetaApi;
  close: () => Promise<void>;
};

export type ClientOptions = {
  storage?: StorageMode | "auto";
  baseUrl?: string; // where wa-sqlite assets live; defaults to import.meta.env.BASE_URL + wa-sqlite/
  filename?: string; // only for opfs; defaults to /treecrdt-playground.db
  preferWorker?: boolean; // when true (default for opfs), use a worker instead of main-thread SQLite
  docId?: string; // used for v0 sync opRef derivation inside the extension
};

export async function createTreecrdtClient(opts: ClientOptions = {}): Promise<TreecrdtClient> {
  const storage = opts.storage === "memory" ? "memory" : opts.storage === "opfs" ? "opfs" : "auto";
  const requireOpfs = opts.storage === "opfs";
  const docId = opts.docId ?? "treecrdt";
  const rawBase =
    opts.baseUrl ??
    (typeof import.meta !== "undefined" && (import.meta as any).env?.BASE_URL ? (import.meta as any).env.BASE_URL : "/");
  const baseUrl = rawBase.endsWith("/") ? rawBase : `${rawBase}/`;
  const support = detectOpfsSupport();

  const shouldUseOpfs = storage === "opfs" || (storage === "auto" && support.available);
  const preferWorker = opts.preferWorker ?? shouldUseOpfs; // default to worker when targeting OPFS

  // If OPFS requested, default to worker path to avoid main-thread sync handles.
  if (shouldUseOpfs) {
    if (preferWorker) {
      return createWorkerClient({ baseUrl, filename: opts.filename, storage: "opfs", requireOpfs, docId });
    }
    if (!support.available) {
      throw new Error(`OPFS unavailable in this environment: ${support.reason ?? "unknown reason"}`);
    }
  }

  // Direct path.
  return createDirectClient({
    baseUrl,
    filename: opts.filename,
    storage: shouldUseOpfs ? "opfs" : "memory",
    requireOpfs,
    docId,
  });
}

// --- Worker client

type WorkerProxy = {
  postMessage(msg: RpcRequest, transfer?: Transferable[]): void;
  terminate: () => void;
  addEventListener: (type: "message" | "error", fn: (ev: any) => void) => void;
  removeEventListener: (type: "message" | "error", fn: (ev: any) => void) => void;
};

async function createWorkerClient(opts: {
  baseUrl: string;
  filename?: string;
  storage: StorageMode;
  docId: string;
  requireOpfs?: boolean;
}): Promise<TreecrdtClient> {
  // Keep the URL inline so Vite detects and bundles the worker properly.
  const worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" }) as unknown as WorkerProxy;
  let nextId = 1;
  const pending = new Map<number, { resolve: (value: any) => void; reject: (err: Error) => void }>();
  let terminalError: Error | null = null;

  const call = <M extends RpcMethod>(method: M, params: RpcParams<M>): Promise<RpcResult<M>> => {
    const id = nextId++;
    if (terminalError) return Promise.reject(terminalError);
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      worker.postMessage({ id, method, params } satisfies RpcRequest<M>);
    });
  };

  const onMessage = (ev: MessageEvent<RpcResponse>) => {
    const handler = pending.get(ev.data.id as number);
    if (!handler) return;
    pending.delete(ev.data.id as number);
    if (ev.data.ok) handler.resolve(ev.data.result);
    else handler.reject(new Error(ev.data.error || "worker error"));
  };
  const onError = (ev: ErrorEvent) => {
    const err = new Error(ev.message || "worker error");
    terminalError = err;
    for (const { reject } of pending.values()) reject(err);
    pending.clear();
  };
  worker.addEventListener("message", onMessage);
  worker.addEventListener("error", onError);

  // init
  const initResult = (await call("init", {
    baseUrl: opts.baseUrl,
    filename: opts.filename,
    storage: opts.storage,
    docId: opts.docId,
  })) as { storage?: StorageMode; opfsError?: string } | undefined;
  const effectiveStorage: StorageMode = initResult?.storage === "opfs" ? "opfs" : "memory";
  if (opts.requireOpfs && effectiveStorage !== "opfs") {
    const reason = initResult?.opfsError ? `: ${initResult.opfsError}` : "";
    try {
      if (!terminalError) await call("close", undefined);
    } catch {
      // ignore close errors on init failure
    } finally {
      worker.removeEventListener("error", onError);
      worker.removeEventListener("message", onMessage);
      worker.terminate();
    }
    throw new Error(`OPFS requested but could not be initialized${reason}`);
  }

  const opsSinceImpl = (lamport: number, root?: string) =>
    call("opsSince", { lamport, root }).then((rows) => decodeSqliteOps(rows));
  const opRefsAllImpl = () => call("opRefsAll", undefined).then((rows) => decodeSqliteOpRefs(rows));
  const opRefsChildrenImpl = (parent: string) =>
    call("opRefsChildren", { parent }).then((rows) => decodeSqliteOpRefs(rows));
  const opsByOpRefsImpl = (opRefs: Uint8Array[]) =>
    call("opsByOpRefs", { opRefs: opRefs.map((r) => Array.from(r)) }).then((rows) => decodeSqliteOps(rows));
  const treeChildrenImpl = (parent: string) =>
    call("treeChildren", { parent }).then((rows) => decodeSqliteNodeIds(rows));
  const treeDumpImpl = () => call("treeDump", undefined).then((rows) => decodeSqliteTreeRows(rows));
  const treeNodeCountImpl = () => call("treeNodeCount", undefined).then((v) => Number(v));
  const headLamportImpl = () => call("headLamport", undefined).then((v) => Number(v));
  const replicaMaxCounterImpl = (replica: Operation["meta"]["id"]["replica"]) =>
    call("replicaMaxCounter", { replica: Array.from(encodeReplica(replica)) }).then((v) => Number(v));

  return {
    mode: "worker",
    storage: effectiveStorage,
    docId: opts.docId,
    ops: {
      append: (op) => call("append", { op }),
      appendMany: (ops) => call("appendMany", { ops }),
      all: () => opsSinceImpl(0),
      since: opsSinceImpl,
      children: async (parent) => opsByOpRefsImpl(await opRefsChildrenImpl(parent)),
      get: opsByOpRefsImpl,
    },
    opRefs: { all: opRefsAllImpl, children: opRefsChildrenImpl },
    tree: { children: treeChildrenImpl, dump: treeDumpImpl, nodeCount: treeNodeCountImpl },
    meta: { headLamport: headLamportImpl, replicaMaxCounter: replicaMaxCounterImpl },
    close: async () => {
      try {
        if (!terminalError) await call("close", undefined);
      } finally {
        worker.removeEventListener("error", onError);
        worker.removeEventListener("message", onMessage);
        worker.terminate();
      }
    },
  };
}

// --- Direct client (main-thread, used for memory or opt-in opfs)

async function createDirectClient(opts: {
  baseUrl: string;
  filename?: string;
  storage: StorageMode;
  docId: string;
  requireOpfs?: boolean;
}): Promise<TreecrdtClient> {
  const { baseUrl, storage, requireOpfs } = opts;
  const opened = await openTreecrdtDb({
    baseUrl,
    filename: opts.filename,
    storage,
    docId: opts.docId,
    requireOpfs,
  });
  const db = opened.db;
  const finalStorage: StorageMode = opened.storage;
  const filename = opened.filename;
  const adapter = createWaSqliteAdapter(db);
  const wrapError = (stage: string, err: unknown) =>
    new Error(
      JSON.stringify({
        stage,
        storage: finalStorage,
        filename,
        baseUrl,
        message: err instanceof Error ? err.message : String(err),
      })
    );

  const appendImpl = async (op: Operation) => {
    try {
      await appendOpRaw(db, op, nodeIdToBytes16, encodeReplica);
    } catch (err) {
      throw wrapError("append", err);
    }
  };
  const appendManyImpl = async (ops: Operation[]) => {
    try {
      await adapter.appendOps!(ops, nodeIdToBytes16, encodeReplica);
    } catch (err) {
      throw wrapError("appendMany", err);
    }
  };
  const opsSinceImpl = async (lamport: number, root?: string) => {
    try {
      const rows = await opsSinceRaw(db, { lamport, root });
      return decodeSqliteOps(rows);
    } catch (err) {
      throw wrapError("opsSince", err);
    }
  };
  const opRefsAllImpl = async () => {
    try {
      const rows = await opRefsAllRaw(db);
      return decodeSqliteOpRefs(rows);
    } catch (err) {
      throw wrapError("opRefsAll", err);
    }
  };
  const opRefsChildrenImpl = async (parent: string) => {
    try {
      const rows = await opRefsChildrenRaw(db, nodeIdToBytes16(parent));
      return decodeSqliteOpRefs(rows);
    } catch (err) {
      throw wrapError("opRefsChildren", err);
    }
  };
  const opsByOpRefsImpl = async (opRefs: Uint8Array[]) => {
    try {
      const rows = await opsByOpRefsRaw(db, opRefs);
      return decodeSqliteOps(rows);
    } catch (err) {
      throw wrapError("opsByOpRefs", err);
    }
  };
  const treeChildrenImpl = async (parent: string) => {
    try {
      const rows = await treeChildrenRaw(db, nodeIdToBytes16(parent));
      return decodeSqliteNodeIds(rows);
    } catch (err) {
      throw wrapError("treeChildren", err);
    }
  };
  const treeDumpImpl = async () => {
    try {
      const rows = await treeDumpRaw(db);
      return decodeSqliteTreeRows(rows);
    } catch (err) {
      throw wrapError("treeDump", err);
    }
  };
  const treeNodeCountImpl = async () => {
    try {
      return await treeNodeCountRaw(db);
    } catch (err) {
      throw wrapError("treeNodeCount", err);
    }
  };
  const headLamportImpl = async () => {
    try {
      return await headLamportRaw(db);
    } catch (err) {
      throw wrapError("headLamport", err);
    }
  };
  const replicaMaxCounterImpl = async (replica: Operation["meta"]["id"]["replica"]) => {
    try {
      return await replicaMaxCounterRaw(db, encodeReplica(replica));
    } catch (err) {
      throw wrapError("replicaMaxCounter", err);
    }
  };

  return {
    mode: "direct",
    storage: finalStorage,
    docId: opts.docId,
    ops: {
      append: appendImpl,
      appendMany: appendManyImpl,
      all: () => opsSinceImpl(0),
      since: opsSinceImpl,
      children: async (parent) => opsByOpRefsImpl(await opRefsChildrenImpl(parent)),
      get: opsByOpRefsImpl,
    },
    opRefs: { all: opRefsAllImpl, children: opRefsChildrenImpl },
    tree: { children: treeChildrenImpl, dump: treeDumpImpl, nodeCount: treeNodeCountImpl },
    meta: { headLamport: headLamportImpl, replicaMaxCounter: replicaMaxCounterImpl },
    close: async () => {
      if (db.close) await db.close();
    },
  };
}

// --- helpers

function encodeReplica(replica: Operation["meta"]["id"]["replica"]): Uint8Array {
  return replicaIdToBytes(replica);
}

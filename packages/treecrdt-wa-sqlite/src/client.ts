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

type RpcCall = <M extends RpcMethod>(method: M, params: RpcParams<M>) => Promise<RpcResult<M>>;

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
  const initResult = (await call("init", [
    opts.baseUrl,
    opts.filename,
    opts.storage,
    opts.docId,
  ])) as { storage?: StorageMode; opfsError?: string } | undefined;
  const effectiveStorage: StorageMode = initResult?.storage === "opfs" ? "opfs" : "memory";
  if (opts.requireOpfs && effectiveStorage !== "opfs") {
    const reason = initResult?.opfsError ? `: ${initResult.opfsError}` : "";
    try {
      if (!terminalError) await call("close", [] as RpcParams<"close">);
    } catch {
      // ignore close errors on init failure
    } finally {
      worker.removeEventListener("error", onError);
      worker.removeEventListener("message", onMessage);
      worker.terminate();
    }
    throw new Error(`OPFS requested but could not be initialized${reason}`);
  }

  const closeImpl = async () => {
    try {
      if (!terminalError) await call("close", [] as RpcParams<"close">);
    } finally {
      worker.removeEventListener("error", onError);
      worker.removeEventListener("message", onMessage);
      worker.terminate();
    }
  };

  return makeTreecrdtClientFromCall({
    mode: "worker",
    storage: effectiveStorage,
    docId: opts.docId,
    call,
    close: closeImpl,
  });
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

  const call: RpcCall = async (method, params) => {
    try {
      switch (method) {
        case "append": {
          const [op] = params as RpcParams<"append">;
          await appendOpRaw(db, op, nodeIdToBytes16, encodeReplica);
          return undefined as any;
        }
        case "appendMany": {
          const [ops] = params as RpcParams<"appendMany">;
          await adapter.appendOps!(ops, nodeIdToBytes16, encodeReplica);
          return undefined as any;
        }
        case "opsSince": {
          const [lamport, root] = params as RpcParams<"opsSince">;
          return (await opsSinceRaw(db, { lamport, root })) as any;
        }
        case "opRefsAll":
          return (await opRefsAllRaw(db)) as any;
        case "opRefsChildren": {
          const [parent] = params as RpcParams<"opRefsChildren">;
          return (await opRefsChildrenRaw(db, nodeIdToBytes16(parent))) as any;
        }
        case "opsByOpRefs": {
          const [opRefs] = params as RpcParams<"opsByOpRefs">;
          return (await opsByOpRefsRaw(db, opRefs.map((r) => Uint8Array.from(r)))) as any;
        }
        case "treeChildren": {
          const [parent] = params as RpcParams<"treeChildren">;
          return (await treeChildrenRaw(db, nodeIdToBytes16(parent))) as any;
        }
        case "treeDump":
          return (await treeDumpRaw(db)) as any;
        case "treeNodeCount":
          return (await treeNodeCountRaw(db)) as any;
        case "headLamport":
          return (await headLamportRaw(db)) as any;
        case "replicaMaxCounter": {
          const [rawReplica] = params as RpcParams<"replicaMaxCounter">;
          const replica =
            typeof rawReplica === "string" ? replicaIdToBytes(rawReplica) : Uint8Array.from(rawReplica);
          return (await replicaMaxCounterRaw(db, replica)) as any;
        }
        case "close":
          if (db.close) await db.close();
          return undefined as any;
        default:
          throw new Error(`unsupported direct method: ${method}`);
      }
    } catch (err) {
      throw wrapError(method, err);
    }
  };

  return makeTreecrdtClientFromCall({
    mode: "direct",
    storage: finalStorage,
    docId: opts.docId,
    call,
    close: async () => {
      if (db.close) await db.close();
    },
  });
}

// --- helpers

function makeTreecrdtClientFromCall(opts: {
  mode: ClientMode;
  storage: StorageMode;
  docId: string;
  call: RpcCall;
  close: () => Promise<void>;
}): TreecrdtClient {
  const call = opts.call;

  const opsSinceImpl = async (lamport: number, root?: string) => {
    const rows = await call("opsSince", [lamport, root]);
    return decodeSqliteOps(rows);
  };
  const opRefsAllImpl = async () => decodeSqliteOpRefs(await call("opRefsAll", []));
  const opRefsChildrenImpl = async (parent: string) => decodeSqliteOpRefs(await call("opRefsChildren", [parent]));
  const opsByOpRefsImpl = async (opRefs: Uint8Array[]) =>
    decodeSqliteOps(await call("opsByOpRefs", [opRefs.map((r) => Array.from(r))]));
  const treeChildrenImpl = async (parent: string) => decodeSqliteNodeIds(await call("treeChildren", [parent]));
  const treeDumpImpl = async () => decodeSqliteTreeRows(await call("treeDump", []));
  const treeNodeCountImpl = async () => Number(await call("treeNodeCount", []));
  const headLamportImpl = async () => Number(await call("headLamport", []));
  const replicaMaxCounterImpl = async (replica: Operation["meta"]["id"]["replica"]) =>
    Number(await call("replicaMaxCounter", [Array.from(encodeReplica(replica))]));

  return {
    mode: opts.mode,
    storage: opts.storage,
    docId: opts.docId,
    ops: {
      append: (op) => call("append", [op]).then(() => undefined),
      appendMany: (ops) => call("appendMany", [ops]).then(() => undefined),
      all: () => opsSinceImpl(0),
      since: opsSinceImpl,
      children: async (parent) => opsByOpRefsImpl(await opRefsChildrenImpl(parent)),
      get: opsByOpRefsImpl,
    },
    opRefs: { all: opRefsAllImpl, children: opRefsChildrenImpl },
    tree: { children: treeChildrenImpl, dump: treeDumpImpl, nodeCount: treeNodeCountImpl },
    meta: { headLamport: headLamportImpl, replicaMaxCounter: replicaMaxCounterImpl },
    close: opts.close,
  };
}

function encodeReplica(replica: Operation["meta"]["id"]["replica"]): Uint8Array {
  return replicaIdToBytes(replica);
}

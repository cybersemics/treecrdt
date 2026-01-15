import { detectOpfsSupport } from "./opfs.js";
import type { Operation, ReplicaId } from "@treecrdt/interface";
import {
  createTreecrdtSqliteWriter,
  decodeSqliteNodeIds,
  decodeSqliteOpRefs,
  decodeSqliteOps,
  decodeSqliteTreeChildRows,
  decodeSqliteTreeRows,
  type SqliteTreeChildRow,
  type SqliteTreeRow,
  type SqliteRunner,
  type TreecrdtSqlitePlacement,
  type TreecrdtSqliteWriter,
} from "@treecrdt/interface/sqlite";
import { bytesToHex, nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";
import type {
  TreecrdtEngine,
  TreecrdtEngineLocal,
  TreecrdtEngineMeta,
  TreecrdtEngineOpRefs,
  TreecrdtEngineOps,
  TreecrdtEngineTree,
} from "@treecrdt/interface/engine";
import type { Database } from "./index.js";
import type { RpcMethod, RpcParams, RpcRequest, RpcResponse, RpcResult } from "./rpc.js";
import { openTreecrdtDb } from "./open.js";

export type StorageMode = "memory" | "opfs";
export type ClientMode = "direct" | "worker";

export type TreecrdtOpsApi = TreecrdtEngineOps;

export type TreecrdtOpRefsApi = TreecrdtEngineOpRefs;

export type TreeNodeRow = SqliteTreeRow;

export type TreecrdtTreeApi = TreecrdtEngineTree;

export type TreecrdtMetaApi = TreecrdtEngineMeta;

export type TreecrdtLocalApi = TreecrdtEngineLocal;

export type TreecrdtClient = TreecrdtEngine & {
  mode: ClientMode;
  storage: StorageMode;
  docId: string;
  runner: SqliteRunner;
  ops: TreecrdtOpsApi;
  opRefs: TreecrdtOpRefsApi;
  tree: TreecrdtTreeApi;
  meta: TreecrdtMetaApi;
  local: TreecrdtLocalApi;
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
  const adapter = opened.api;
  const runner: SqliteRunner = { exec: (sql) => db.exec(sql), getText: (sql, params = []) => dbGetText(db, sql, params) };
  const localWriters = new Map<string, TreecrdtSqliteWriter>();
  const localWriterKey = (replica: ReplicaId) => (typeof replica === "string" ? replica : bytesToHex(replica));
  const localWriterFor = (replica: ReplicaId) => {
    const key = localWriterKey(replica);
    const existing = localWriters.get(key);
    if (existing) return existing;
    const next = createTreecrdtSqliteWriter(runner, { replica });
    localWriters.set(key, next);
    return next;
  };
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
        case "sqlExec": {
          const [sql] = params as RpcParams<"sqlExec">;
          await db.exec(sql);
          return undefined as any;
        }
        case "sqlGetText": {
          const [sql, rawParams] = params as RpcParams<"sqlGetText">;
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const stmt: any = await db.prepare(sql);
          try {
            let idx = 1;
            for (const p of rawParams ?? []) {
              await db.bind(stmt, idx++, p);
            }
            const row = await db.step(stmt);
            if (row === 0) return null as any;
            return (await db.column_text(stmt, 0)) as any;
          } finally {
            await db.finalize(stmt);
          }
        }
        case "append": {
          const [op] = params as RpcParams<"append">;
          await adapter.appendOp(op, nodeIdToBytes16, encodeReplica);
          return undefined as any;
        }
        case "appendMany": {
          const [ops] = params as RpcParams<"appendMany">;
          await adapter.appendOps!(ops, nodeIdToBytes16, encodeReplica);
          return undefined as any;
        }
        case "opsSince": {
          const [lamport, root] = params as RpcParams<"opsSince">;
          return (await adapter.opsSince(lamport, root)) as any;
        }
        case "opRefsAll":
          return (await adapter.opRefsAll()) as any;
        case "opRefsChildren": {
          const [parent] = params as RpcParams<"opRefsChildren">;
          return (await adapter.opRefsChildren(nodeIdToBytes16(parent))) as any;
        }
        case "opsByOpRefs": {
          const [opRefs] = params as RpcParams<"opsByOpRefs">;
          return (await adapter.opsByOpRefs(opRefs.map((r) => Uint8Array.from(r)))) as any;
        }
        case "treeChildren": {
          const [parent] = params as RpcParams<"treeChildren">;
          return (await adapter.treeChildren(nodeIdToBytes16(parent))) as any;
        }
        case "treeChildrenPage": {
          const [parent, cursor, limit] = params as RpcParams<"treeChildrenPage">;
          const cursorBytes = cursor
            ? {
                orderKey: Uint8Array.from(cursor.orderKey),
                node: Uint8Array.from(cursor.node),
              }
            : null;
          return (await adapter.treeChildrenPage!(nodeIdToBytes16(parent), cursorBytes, limit)) as any;
        }
        case "treeDump":
          return (await adapter.treeDump()) as any;
        case "treeNodeCount":
          return (await adapter.treeNodeCount()) as any;
        case "headLamport":
          return (await adapter.headLamport()) as any;
        case "replicaMaxCounter": {
          const [rawReplica] = params as RpcParams<"replicaMaxCounter">;
          const replica =
            typeof rawReplica === "string" ? replicaIdToBytes(rawReplica) : Uint8Array.from(rawReplica);
          return (await adapter.replicaMaxCounter(replica)) as any;
        }
        case "localInsert": {
          const [replica, parent, node, placement, payload] = params as RpcParams<"localInsert">;
          const rid: ReplicaId = typeof replica === "string" ? replica : Uint8Array.from(replica);
          return (await localWriterFor(rid).insert(parent, node, placement, payload ? { payload } : {})) as any;
        }
        case "localMove": {
          const [replica, node, newParent, placement] = params as RpcParams<"localMove">;
          const rid: ReplicaId = typeof replica === "string" ? replica : Uint8Array.from(replica);
          return (await localWriterFor(rid).move(node, newParent, placement)) as any;
        }
        case "localDelete": {
          const [replica, node] = params as RpcParams<"localDelete">;
          const rid: ReplicaId = typeof replica === "string" ? replica : Uint8Array.from(replica);
          return (await localWriterFor(rid).delete(node)) as any;
        }
        case "localPayload": {
          const [replica, node, payload] = params as RpcParams<"localPayload">;
          const rid: ReplicaId = typeof replica === "string" ? replica : Uint8Array.from(replica);
          return (await localWriterFor(rid).payload(node, payload)) as any;
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

  const runner: SqliteRunner = {
    exec: (sql) => call("sqlExec", [sql]).then(() => undefined),
    getText: (sql, params = []) => call("sqlGetText", [sql, params]),
  };

  const opsSinceImpl = async (lamport: number, root?: string) => {
    const rows = await call("opsSince", [lamport, root]);
    return decodeSqliteOps(rows);
  };
  const opRefsAllImpl = async () => decodeSqliteOpRefs(await call("opRefsAll", []));
  const opRefsChildrenImpl = async (parent: string) => decodeSqliteOpRefs(await call("opRefsChildren", [parent]));
  const opsByOpRefsImpl = async (opRefs: Uint8Array[]) =>
    decodeSqliteOps(await call("opsByOpRefs", [opRefs.map((r) => Array.from(r))]));
  const treeChildrenImpl = async (parent: string) => decodeSqliteNodeIds(await call("treeChildren", [parent]));
  const treeChildrenPageImpl = async (
    parent: string,
    cursor: { orderKey: Uint8Array; node: Uint8Array } | null,
    limit: number
  ): Promise<SqliteTreeChildRow[]> => {
    const rpcCursor = cursor ? { orderKey: Array.from(cursor.orderKey), node: Array.from(cursor.node) } : null;
    return decodeSqliteTreeChildRows(await call("treeChildrenPage", [parent, rpcCursor, limit]));
  };
  const treeDumpImpl = async () => decodeSqliteTreeRows(await call("treeDump", []));
  const treeNodeCountImpl = async () => Number(await call("treeNodeCount", []));
  const headLamportImpl = async () => Number(await call("headLamport", []));
  const replicaMaxCounterImpl = async (replica: Operation["meta"]["id"]["replica"]) =>
    Number(await call("replicaMaxCounter", [Array.from(encodeReplica(replica))]));
  const localInsertImpl = async (
    replica: ReplicaId,
    parent: string,
    node: string,
    placement: TreecrdtSqlitePlacement,
    payload: Uint8Array | null
  ) => {
    const rid = typeof replica === "string" ? replica : Array.from(replica);
    return (await call("localInsert", [rid, parent, node, placement, payload])) as unknown as Operation;
  };
  const localMoveImpl = async (replica: ReplicaId, node: string, newParent: string, placement: TreecrdtSqlitePlacement) => {
    const rid = typeof replica === "string" ? replica : Array.from(replica);
    return (await call("localMove", [rid, node, newParent, placement])) as unknown as Operation;
  };
  const localDeleteImpl = async (replica: ReplicaId, node: string) => {
    const rid = typeof replica === "string" ? replica : Array.from(replica);
    return (await call("localDelete", [rid, node])) as unknown as Operation;
  };
  const localPayloadImpl = async (replica: ReplicaId, node: string, payload: Uint8Array | null) => {
    const rid = typeof replica === "string" ? replica : Array.from(replica);
    return (await call("localPayload", [rid, node, payload])) as unknown as Operation;
  };

  return {
    mode: opts.mode,
    storage: opts.storage,
    docId: opts.docId,
    runner,
    ops: {
      append: (op) => call("append", [op]).then(() => undefined),
      appendMany: (ops) => call("appendMany", [ops]).then(() => undefined),
      all: () => opsSinceImpl(0),
      since: opsSinceImpl,
      children: async (parent) => opsByOpRefsImpl(await opRefsChildrenImpl(parent)),
      get: opsByOpRefsImpl,
    },
    opRefs: { all: opRefsAllImpl, children: opRefsChildrenImpl },
    tree: {
      children: treeChildrenImpl,
      childrenPage: treeChildrenPageImpl,
      dump: treeDumpImpl,
      nodeCount: treeNodeCountImpl,
    },
    meta: { headLamport: headLamportImpl, replicaMaxCounter: replicaMaxCounterImpl },
    local: {
      insert: localInsertImpl,
      move: localMoveImpl,
      delete: localDeleteImpl,
      payload: localPayloadImpl,
    },
    close: opts.close,
  };
}

function encodeReplica(replica: Operation["meta"]["id"]["replica"]): Uint8Array {
  return replicaIdToBytes(replica);
}

async function dbGetText(db: Database, sql: string, params: unknown[] = []): Promise<string | null> {
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

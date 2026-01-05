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
import { createOpfsVfs, detectOpfsSupport } from "./opfs.js";
import type { Operation } from "@treecrdt/interface";
import { decodeNodeId, decodeReplicaId, nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";

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

type WorkerReq = {
  id: number;
  method: string;
  params?: any;
};

type WorkerResp = {
  id: number;
  ok: boolean;
  result?: any;
  error?: string;
};

type WorkerInit = { baseUrl: string; filename?: string; storage: StorageMode; docId: string };

type WorkerProxy = {
  postMessage(msg: WorkerReq, transfer?: Transferable[]): void;
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

  const call = (method: string, params?: any): Promise<any> => {
    const id = nextId++;
    if (terminalError) return Promise.reject(terminalError);
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      worker.postMessage({ id, method, params } satisfies WorkerReq);
    });
  };

  const onMessage = (ev: MessageEvent<WorkerResp>) => {
    const handler = pending.get(ev.data.id);
    if (!handler) return;
    pending.delete(ev.data.id);
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
      if (!terminalError) await call("close", []);
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
    call("opsSince", [lamport, root]).then((rows) => parseOps(rows as any[]));
  const opRefsAllImpl = () => call("opRefsAll", []).then((rows) => parseOpRefs(rows as any[]));
  const opRefsChildrenImpl = (parent: string) =>
    call("opRefsChildren", [parent]).then((rows) => parseOpRefs(rows as any[]));
  const opsByOpRefsImpl = (opRefs: Uint8Array[]) =>
    call("opsByOpRefs", [opRefs.map((r) => Array.from(r))]).then((rows) => parseOps(rows as any[]));
  const treeChildrenImpl = (parent: string) =>
    call("treeChildren", [parent]).then((rows) => parseNodeIds(rows as any[]));
  const treeDumpImpl = () => call("treeDump", []).then((rows) => parseTreeRows(rows as any[]));
  const treeNodeCountImpl = () => call("treeNodeCount", []).then((v) => Number(v));
  const headLamportImpl = () => call("headLamport", []).then((v) => Number(v));
  const replicaMaxCounterImpl = (replica: Operation["meta"]["id"]["replica"]) =>
    call("replicaMaxCounter", [Array.from(encodeReplica(replica))]).then((v) => Number(v));

  return {
    mode: "worker",
    storage: effectiveStorage,
    docId: opts.docId,
    ops: {
      append: (op) => call("append", [op]),
      appendMany: (ops) => call("appendMany", [ops]),
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
        if (!terminalError) await call("close", []);
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
  const { baseUrl, filename: filenameOpt, storage, requireOpfs } = opts;
  const sqliteModule = await import(/* @vite-ignore */ `${baseUrl}wa-sqlite/wa-sqlite-async.mjs`);
  const sqliteApi = await import(/* @vite-ignore */ `${baseUrl}wa-sqlite/sqlite-api.js`);
  const module = await sqliteModule.default({
    locateFile: (file: string) => (file.endsWith(".wasm") ? `${baseUrl}wa-sqlite/wa-sqlite-async.wasm` : file),
  });
  const sqlite3 = sqliteApi.Factory(module);

  let finalStorage: StorageMode = storage === "opfs" ? "opfs" : "memory";
  if (storage === "opfs") {
    try {
      const vfs = await createOpfsVfs(module, { name: "opfs" });
      sqlite3.vfs_register(vfs, true);
    } catch (err) {
      if (requireOpfs) {
        throw new Error(
          `OPFS requested but could not be initialized: ${err instanceof Error ? err.message : String(err)}`
        );
      }
      finalStorage = "memory";
    }
  }

  const filename = finalStorage === "opfs" ? filenameOpt ?? "/treecrdt.db" : ":memory:";
  const handle = await sqlite3.open_v2(filename);
  const db = makeDbAdapter(sqlite3, handle);
  await setDocIdRaw(db, opts.docId);
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
      return parseOps(rows as any[]);
    } catch (err) {
      throw wrapError("opsSince", err);
    }
  };
  const opRefsAllImpl = async () => {
    try {
      const rows = await opRefsAllRaw(db);
      return parseOpRefs(rows as any[]);
    } catch (err) {
      throw wrapError("opRefsAll", err);
    }
  };
  const opRefsChildrenImpl = async (parent: string) => {
    try {
      const rows = await opRefsChildrenRaw(db, nodeIdToBytes16(parent));
      return parseOpRefs(rows as any[]);
    } catch (err) {
      throw wrapError("opRefsChildren", err);
    }
  };
  const opsByOpRefsImpl = async (opRefs: Uint8Array[]) => {
    try {
      const rows = await opsByOpRefsRaw(db, opRefs);
      return parseOps(rows as any[]);
    } catch (err) {
      throw wrapError("opsByOpRefs", err);
    }
  };
  const treeChildrenImpl = async (parent: string) => {
    try {
      const rows = await treeChildrenRaw(db, nodeIdToBytes16(parent));
      return parseNodeIds(rows as any[]);
    } catch (err) {
      throw wrapError("treeChildren", err);
    }
  };
  const treeDumpImpl = async () => {
    try {
      const rows = await treeDumpRaw(db);
      return parseTreeRows(rows as any[]);
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

function parseOpRefs(raw: any[]): Uint8Array[] {
  return raw.map((val) => (val instanceof Uint8Array ? val : Uint8Array.from(val)));
}

function parseNodeIds(raw: any[]): string[] {
  const decodeNode = decodeNodeId;
  return raw.map((val) => decodeNode(val instanceof Uint8Array ? val : Uint8Array.from(val)));
}

function parseTreeRows(raw: any[]): TreeNodeRow[] {
  const decodeNode = decodeNodeId;
  return raw.map((row) => {
    const node = decodeNode(row.node);
    const parent = row.parent ? decodeNode(row.parent) : null;
    const pos = row.pos === null || row.pos === undefined ? null : Number(row.pos);
    const tombstone = Boolean(row.tombstone);
    return { node, parent, pos, tombstone };
  });
}

function parseOps(raw: any[]): Operation[] {
  const decodeNode = decodeNodeId;
  const decodeReplica = decodeReplicaId;
  return raw.map((row) => {
    const replica = decodeReplica(row.replica);
    const base = { meta: { id: { replica, counter: row.counter }, lamport: row.lamport } } as Operation;
    if (row.kind === "insert") {
      return {
        ...base,
        kind: { type: "insert", parent: decodeNode(row.parent), node: decodeNode(row.node), position: row.position ?? 0 },
      } as Operation;
    }
    if (row.kind === "move") {
      return {
        ...base,
        kind: { type: "move", node: decodeNode(row.node), newParent: decodeNode(row.new_parent), position: row.position ?? 0 },
      } as Operation;
    }
    if (row.kind === "delete") {
      return { ...base, kind: { type: "delete", node: decodeNode(row.node) } } as Operation;
    }
    return { ...base, kind: { type: "tombstone", node: decodeNode(row.node) } } as Operation;
  });
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

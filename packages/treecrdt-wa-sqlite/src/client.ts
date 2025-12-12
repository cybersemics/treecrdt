import { createWaSqliteAdapter, opsSince as opsSinceRaw, appendOp as appendOpRaw, type Database } from "./index.js";
import { createOpfsVfs, detectOpfsSupport } from "./opfs.js";
import type { Operation } from "@treecrdt/interface";

export type StorageMode = "memory" | "opfs";
export type ClientMode = "direct" | "worker";

export type TreecrdtClient = {
  mode: ClientMode;
  storage: StorageMode;
  append: (op: Operation) => Promise<void>;
  appendMany?: (ops: Operation[]) => Promise<void>;
  opsSince: (lamport: number) => Promise<Operation[]>;
  close: () => Promise<void>;
};

export type ClientOptions = {
  storage?: StorageMode | "auto";
  baseUrl?: string; // where wa-sqlite assets live; defaults to import.meta.env.BASE_URL + wa-sqlite/
  filename?: string; // only for opfs; defaults to /treecrdt-playground.db
  preferWorker?: boolean; // when true (default for opfs), use a worker instead of main-thread SQLite
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export async function createTreecrdtClient(opts: ClientOptions = {}): Promise<TreecrdtClient> {
  const storage = opts.storage === "memory" ? "memory" : opts.storage === "opfs" ? "opfs" : "auto";
  const requireOpfs = opts.storage === "opfs";
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
      return createWorkerClient({ baseUrl, filename: opts.filename, storage: "opfs", requireOpfs });
    }
    if (!support.available) {
      throw new Error(`OPFS unavailable in this environment: ${support.reason ?? "unknown reason"}`);
    }
  }

  // Direct path.
  return createDirectClient({ baseUrl, filename: opts.filename, storage: shouldUseOpfs ? "opfs" : "memory", requireOpfs });
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

type WorkerInit = { baseUrl: string; filename?: string; storage: StorageMode };

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
  const initResult = (await call("init", {
    baseUrl: opts.baseUrl,
    filename: opts.filename,
    storage: opts.storage,
  } satisfies WorkerInit)) as { storage?: StorageMode; opfsError?: string } | undefined;
  const effectiveStorage: StorageMode = initResult?.storage === "opfs" ? "opfs" : "memory";
  if (opts.requireOpfs && effectiveStorage !== "opfs") {
    const reason = initResult?.opfsError ? `: ${initResult.opfsError}` : "";
    try {
      if (!terminalError) await call("close");
    } catch {
      // ignore close errors on init failure
    } finally {
      worker.removeEventListener("error", onError);
      worker.removeEventListener("message", onMessage);
      worker.terminate();
    }
    throw new Error(`OPFS requested but could not be initialized${reason}`);
  }

  return {
    mode: "worker",
    storage: effectiveStorage,
    append: (op) => call("append", { op }),
    appendMany: (ops) => call("appendMany", { ops }),
    opsSince: (lamport) => call("opsSince", { lamport }).then((rows) => parseOps(rows as any[])),
    close: async () => {
      try {
        if (!terminalError) await call("close");
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

  return {
    mode: "direct",
    storage: finalStorage,
    append: async (op) => {
      try {
        await appendOpRaw(db, op, encodeNodeId, encodeReplica);
      } catch (err) {
        throw wrapError("append", err);
      }
    },
    appendMany: adapter.appendOps
      ? async (ops) => {
          try {
            await adapter.appendOps!(ops, encodeNodeId, encodeReplica);
          } catch (err) {
            throw wrapError("appendMany", err);
          }
        }
      : undefined,
    opsSince: async (lamport) => {
      try {
        const rows = await opsSinceRaw(db, { lamport });
        return parseOps(rows as any[]);
      } catch (err) {
        throw wrapError("opsSince", err);
      }
    },
    close: async () => {
      if (db.close) await db.close();
    },
  };
}

// --- helpers

function encodeNodeId(id: string): Uint8Array {
  // treat id as hex string
  const clean = id.startsWith("0x") ? id.slice(2) : id;
  const bytes = new Uint8Array(clean.length / 2);
  for (let i = 0; i < clean.length; i += 2) {
    bytes[i / 2] = parseInt(clean.slice(i, i + 2), 16);
  }
  return bytes;
}

function encodeReplica(replica: Operation["meta"]["id"]["replica"]): Uint8Array {
  return typeof replica === "string" ? encoder.encode(replica) : replica;
}

function parseOps(raw: any[]): Operation[] {
  const decodeNode = (val: any) => {
    if (val === null || val === undefined) return "";
    if (typeof val === "number") {
      return BigInt(val).toString(16).padStart(32, "0");
    }
    if (typeof val === "string") {
      const clean = val.trim();
      const looksHex = /^[0-9a-fA-F]{32}$/.test(clean);
      if (looksHex) return clean.toLowerCase();
      if (/^\d+$/.test(clean)) {
        return BigInt(clean).toString(16).padStart(32, "0");
      }
      return clean;
    }
    const bytes = val instanceof Uint8Array ? val : Uint8Array.from(val);
    return bytesToHex(bytes);
  };
  const decodeReplica = (val: any) => {
    if (val === null || val === undefined) return "";
    if (typeof val === "string") return val;
    return decoder.decode(val instanceof Uint8Array ? val : Uint8Array.from(val));
  };
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

function bytesToHex(bytes: Uint8Array | number[]): string {
  const view = bytes instanceof Uint8Array ? bytes : Uint8Array.from(bytes);
  return Array.from(view)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
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

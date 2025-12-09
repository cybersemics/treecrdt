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
  preferWorker?: boolean; // when true, will spin a worker for opfs if direct opfs unsupported
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export async function createTreecrdtClient(opts: ClientOptions = {}): Promise<TreecrdtClient> {
  const storage = opts.storage === "memory" ? "memory" : opts.storage === "opfs" ? "opfs" : "auto";
  const rawBase =
    opts.baseUrl ??
    (typeof import.meta !== "undefined" && (import.meta as any).env?.BASE_URL ? (import.meta as any).env.BASE_URL : "/");
  const baseUrl = rawBase.endsWith("/") ? rawBase : `${rawBase}/`;
  const support = detectOpfsSupport();

  const shouldUseOpfs = storage === "opfs" || (storage === "auto" && support.available);

  // If OPFS requested but not available in main thread and preferWorker, spin worker.
  if (shouldUseOpfs && !support.available && opts.preferWorker) {
    return createWorkerClient({ baseUrl, filename: opts.filename, storage: "opfs" });
  }

  // Direct path.
  const sqliteModule = await import(/* @vite-ignore */ `${baseUrl}wa-sqlite/wa-sqlite-async.mjs`);
  const sqliteApi = await import(/* @vite-ignore */ `${baseUrl}wa-sqlite/sqlite-api.js`);
  const module = await sqliteModule.default({
    locateFile: (file: string) => (file.endsWith(".wasm") ? `${baseUrl}wa-sqlite/wa-sqlite-async.wasm` : file),
  });
  const sqlite3 = sqliteApi.Factory(module);

  let finalStorage: StorageMode = "memory";
  if (shouldUseOpfs && support.available) {
    try {
      const vfs = await createOpfsVfs(module, { name: "opfs" });
      sqlite3.vfs_register(vfs, true);
      finalStorage = "opfs";
    } catch {
      finalStorage = "memory";
    }
  }

  const filename = finalStorage === "opfs" ? opts.filename ?? "/treecrdt.db" : ":memory:";
  let handle: number;
  try {
    handle = await sqlite3.open_v2(filename);
  } catch (err) {
    throw new Error(
      JSON.stringify({
        where: "open_v2",
        filename,
        baseUrl,
        message: err instanceof Error ? err.message : String(err),
      })
    );
  }
  try {
    await sqlite3.exec(handle, "PRAGMA user_version");
  } catch (err) {
    throw new Error(
      JSON.stringify({
        where: "pragma",
        filename,
        baseUrl,
        message: err instanceof Error ? err.message : String(err),
      })
    );
  }
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
  addEventListener: (type: "message", fn: (ev: MessageEvent<WorkerResp>) => void) => void;
  removeEventListener: (type: "message", fn: (ev: MessageEvent<WorkerResp>) => void) => void;
};

async function createWorkerClient(opts: { baseUrl: string; filename?: string; storage: StorageMode }): Promise<TreecrdtClient> {
  const workerUrl = new URL("./worker.js", import.meta.url);
  const worker = new Worker(workerUrl, { type: "module" }) as unknown as WorkerProxy;
  let nextId = 1;
  const pending = new Map<number, (res: WorkerResp) => void>();

  const call = (method: string, params?: any): Promise<any> => {
    const id = nextId++;
    return new Promise((resolve, reject) => {
      pending.set(id, (resp) => {
        if (resp.ok) resolve(resp.result);
        else reject(new Error(resp.error || "worker error"));
      });
      worker.postMessage({ id, method, params } satisfies WorkerReq);
    });
  };

  const onMessage = (ev: MessageEvent<WorkerResp>) => {
    const handler = pending.get(ev.data.id);
    if (handler) {
      pending.delete(ev.data.id);
      handler(ev.data);
    }
  };
  worker.addEventListener("message", onMessage);

  // init
  await call("init", { baseUrl: opts.baseUrl, filename: opts.filename, storage: opts.storage } satisfies WorkerInit);

  return {
    mode: "worker",
    storage: opts.storage,
    append: (op) => call("append", { op }),
    appendMany: (ops) => call("appendMany", { ops }),
    opsSince: (lamport) => call("opsSince", { lamport }).then((rows) => parseOps(rows as any[])),
    close: async () => {
      try {
        await call("close");
      } finally {
        worker.removeEventListener("message", onMessage);
        worker.terminate();
      }
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

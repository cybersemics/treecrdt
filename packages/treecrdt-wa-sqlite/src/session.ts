import type { Operation, TreecrdtAdapter } from '@treecrdt/interface';
import { nodeIdToBytes16, replicaIdToBytes } from '@treecrdt/interface/ids';
import type { MaterializationEvent, MaterializationOutcome } from '@treecrdt/interface/engine';
import { createTreecrdtSqliteAdapter } from '@treecrdt/interface/sqlite';
import * as Comlink from 'comlink';
import type { OpfsVfsKind } from './opfs.js';
import { clearOpfsStorage } from './opfs.js';
import type { OpenTreecrdtDbOptions, OpenTreecrdtDbResult } from './open-core.js';
import type { Database, StorageMode } from './types.js';

export type SessionOpenFn = (opts: OpenTreecrdtDbOptions) => Promise<OpenTreecrdtDbResult>;

export type BackendInitConfig = {
  baseUrl?: string;
  filename?: string;
  storage: StorageMode;
  docId: string;
  fallback: 'memory' | 'throw';
  opfsVfs?: OpfsVfsKind;
};

export type BackendInitResult = {
  storage: StorageMode;
  filename: string;
  opfsError?: string;
};

export type MaterializationListener = (event: MaterializationEvent) => void;

export type SqlParam = number | string | null | Uint8Array;
export type TreeChildrenCursor = { orderKey: Uint8Array; node: Uint8Array };

/**
 * Runtime-agnostic SQLite / CRDT data API.
 * Same surface for every runtime; lifecycle lives on TreecrdtConnection.
 */
export interface TreecrdtSession {
  sqlExec: (sql: string) => Promise<void>;
  sqlGetText: (sql: string, params?: SqlParam[]) => Promise<string | null>;

  append: (op: Operation) => Promise<MaterializationOutcome>;
  appendMany: (ops: Operation[]) => Promise<MaterializationOutcome>;
  opsSince: (lamport: number, root?: string) => Promise<unknown[]>;
  opRefsAll: () => Promise<unknown[]>;
  opRefsChildren: (parent: string) => Promise<unknown[]>;
  opsByOpRefs: (opRefs: Uint8Array[]) => Promise<unknown[]>;

  treeChildren: (parent: string) => Promise<unknown[]>;
  treeChildrenPage: (
    parent: string,
    cursor: TreeChildrenCursor | null,
    limit: number,
  ) => Promise<unknown[]>;
  treeDump: () => Promise<unknown[]>;
  treePayload: (node: string) => Promise<Uint8Array | null>;
  treeNodeCount: () => Promise<number>;
  treeParent: (node: string) => Promise<Uint8Array | null>;
  treeExists: (node: string) => Promise<boolean>;
  headLamport: () => Promise<number>;
  replicaMaxCounter: (replica: Uint8Array) => Promise<number>;
}

/** Open/close and materialization fan-out used only by connection implementations. */
export interface TreecrdtSessionLifecycle {
  open: (config: BackendInitConfig) => Promise<BackendInitResult>;
  closeDb: () => Promise<void>;
  dropStorage: () => Promise<void>;
  subscribeMaterialized: (listener: MaterializationListener) => void;
  unsubscribeMaterialized: (listener: MaterializationListener) => void;
  emitMaterialized: (event: MaterializationEvent, exclude?: MaterializationListener) => void;
}

/** Owns one DB: narrow lifecycle + the data surface as `session` (em-style composition). */
export type TreecrdtSessionOwner = TreecrdtSessionLifecycle & {
  readonly session: TreecrdtSession;
};

/** Creates one serialized SQLite / CRDT session owned by a connection. */
const createTreecrdtSession = (openDb: SessionOpenFn): TreecrdtSessionOwner => {
  let db: Database | null = null;
  let api: TreecrdtAdapter | null = null;
  let storedFilename: string | undefined;
  let storedStorage: StorageMode = 'memory';
  let queue: Promise<void> = Promise.resolve();
  const listeners = new Set<MaterializationListener>();

  /** Serializes session work so open/close/append cannot interleave. */
  const run = <T>(work: () => Promise<T>): Promise<T> => {
    const next = queue.then(work, work);
    queue = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  };

  const ensureApi = (): TreecrdtAdapter => {
    if (!db || !api) throw new Error('db not initialized');
    return api;
  };

  const ensureDb = (): Database => {
    if (!db) throw new Error('db not initialized');
    return db;
  };

  const closeDbUnlocked = async (): Promise<void> => {
    if (db?.close) await db.close();
    db = null;
    api = null;
    storedFilename = undefined;
    storedStorage = 'memory';
  };

  /** Fan out materialization; `exclude` skips the originating shared-worker port. */
  const emitMaterialized = (
    event: MaterializationEvent,
    exclude?: MaterializationListener,
  ): void => {
    if (event.changes.length === 0) return;
    for (const listener of listeners) {
      if (listener === exclude) continue;
      try {
        listener(event);
      } catch {
        // Listener failures must not break the session.
      }
    }
  };

  /** Open or replace the DB; used by connection implementations only. */
  const open = (config: BackendInitConfig): Promise<BackendInitResult> =>
    run(async () => {
      await closeDbUnlocked();
      const opened = await openDb({
        baseUrl: config.baseUrl,
        filename: config.filename,
        storage: config.storage,
        docId: config.docId,
        requireOpfs: config.fallback === 'throw',
        opfsVfs: config.opfsVfs,
      });
      db = opened.db;
      // CRDT SQL mapping lives on the session; open only returns a Database (SqliteRunner).
      api = createTreecrdtSqliteAdapter(opened.db, {
        onMaterialized: (event) => emitMaterialized(event),
      });
      storedFilename = opened.filename;
      storedStorage = opened.storage;
      return toInitResult(opened);
    });

  const closeDb = (): Promise<void> => run(() => closeDbUnlocked());

  /** Close the DB and clear OPFS when the session was on durable storage. */
  const dropStorage = (): Promise<void> =>
    run(async () => {
      const filename = storedFilename;
      const storage = storedStorage;
      await closeDbUnlocked();
      if (storage === 'opfs' && filename) {
        await clearOpfsStorage(filename);
      }
    });

  const session: TreecrdtSession = {
    sqlExec: (sql) =>
      run(async () => {
        await ensureDb().exec(sql);
      }),
    sqlGetText: (sql, params = []) => run(async () => ensureDb().getText(sql, params)),

    append: (op) => run(async () => ensureApi().appendOp(op, nodeIdToBytes16, replicaIdToBytes)),
    appendMany: (ops) =>
      run(async () => ensureApi().appendOps!(ops, nodeIdToBytes16, replicaIdToBytes)),
    opsSince: (lamport, root?) => run(async () => ensureApi().opsSince(lamport, root)),
    opRefsAll: () => run(async () => ensureApi().opRefsAll()),
    opRefsChildren: (parent) =>
      run(async () => ensureApi().opRefsChildren(nodeIdToBytes16(parent))),
    opsByOpRefs: (opRefs) => run(async () => ensureApi().opsByOpRefs(opRefs)),

    treeChildren: (parent) => run(async () => ensureApi().treeChildren(nodeIdToBytes16(parent))),
    treeChildrenPage: (parent, cursor, limit) =>
      run(async () => ensureApi().treeChildrenPage!(nodeIdToBytes16(parent), cursor, limit)),
    treeDump: () => run(async () => ensureApi().treeDump()),
    treePayload: (node) =>
      run(async () => transferBinary(await ensureApi().treePayload(nodeIdToBytes16(node)))),
    treeNodeCount: () => run(async () => ensureApi().treeNodeCount()),
    treeParent: (node) =>
      run(async () => transferBinary(await ensureApi().treeParent(nodeIdToBytes16(node)))),
    treeExists: (node) => run(async () => ensureApi().treeExists(nodeIdToBytes16(node))),
    headLamport: () => run(async () => ensureApi().headLamport()),
    replicaMaxCounter: (replica) => run(async () => ensureApi().replicaMaxCounter(replica)),
  };

  return {
    session,
    open,
    closeDb,
    dropStorage,
    subscribeMaterialized: (listener) => {
      listeners.add(listener);
    },
    unsubscribeMaterialized: (listener) => {
      listeners.delete(listener);
    },
    emitMaterialized,
  };
};

export default createTreecrdtSession;

function toInitResult(opened: OpenTreecrdtDbResult): BackendInitResult {
  return opened.opfsError
    ? { storage: opened.storage, filename: opened.filename, opfsError: opened.opfsError }
    : { storage: opened.storage, filename: opened.filename };
}

/**
 * Copy off WASM / SharedArrayBuffer / subarray views, then mark the owned
 * ArrayBuffer for Comlink to transfer instead of structured-cloning.
 * Direct (same-thread) calls only set a WeakMap hint; the buffer is not detached.
 */
const transferBinary = (bytes: Uint8Array | null): Uint8Array | null => {
  if (bytes === null) return null;
  const { buffer } = bytes;
  if (
    buffer instanceof ArrayBuffer &&
    bytes.byteOffset === 0 &&
    bytes.byteLength === buffer.byteLength
  ) {
    return Comlink.transfer(bytes, [buffer]);
  }
  const copy = Uint8Array.from(bytes);
  return Comlink.transfer(copy, [copy.buffer]);
};

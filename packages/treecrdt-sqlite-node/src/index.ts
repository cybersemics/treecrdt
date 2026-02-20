import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  createTreecrdtSqliteAdapter,
  createTreecrdtSqliteWriter,
  decodeSqliteNodeIds,
  decodeSqliteOpRefs,
  decodeSqliteOps,
  decodeSqliteTreeChildRows,
  decodeSqliteTreeRows,
  type SqliteRunner,
  type TreecrdtSqlitePlacement,
  type TreecrdtSqliteWriter,
} from '@treecrdt/interface/sqlite';
import { bytesToHex, nodeIdToBytes16, replicaIdToBytes } from '@treecrdt/interface/ids';
import type { Operation, ReplicaId, TreecrdtAdapter } from '@treecrdt/interface';
import type { TreecrdtEngine } from '@treecrdt/interface/engine';

export type LoadOptions = {
  extensionPath?: string;
  entrypoint?: string;
};

// Minimal shape we need from better-sqlite3 Database.
export type LoadableDatabase = {
  loadExtension: (path: string, entryPoint?: string) => void;
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function platformExt(): '.dylib' | '.so' | '.dll' {
  switch (process.platform) {
    case 'darwin':
      return '.dylib';
    case 'win32':
      return '.dll';
    default:
      return '.so';
  }
}

/**
 * Resolve the bundled TreeCRDT SQLite extension for this platform.
 * Falls back to the `native/` directory within this package.
 */
export function defaultExtensionPath(): string {
  const ext = platformExt();
  const base = ext === '.dll' ? 'treecrdt_sqlite_ext' : 'libtreecrdt_sqlite_ext';
  return path.resolve(__dirname, '../native', `${base}${ext}`);
}

/**
 * Load the TreeCRDT SQLite extension into a better-sqlite3 Database.
 */
export function loadTreecrdtExtension(db: LoadableDatabase, opts: LoadOptions = {}): string {
  const path = opts.extensionPath ?? defaultExtensionPath();
  const entrypoint = opts.entrypoint ?? 'sqlite3_treecrdt_init';
  db.loadExtension(path, entrypoint);
  return path;
}

const sqliteRunnerCache = new WeakMap<object, SqliteRunner>();

function createRunner(db: any): SqliteRunner {
  if (db && (typeof db === 'object' || typeof db === 'function')) {
    const cached = sqliteRunnerCache.get(db as object);
    if (cached) return cached;
  }

  const stmtCache = new Map<string, any>();
  const prepare = (sql: string) => {
    const cached = stmtCache.get(sql);
    if (cached) return cached;
    const stmt = db.prepare(sql);
    stmtCache.set(sql, stmt);
    return stmt;
  };

  const toBindings = (params: unknown[]) =>
    params.reduce<Record<number, unknown>>((acc, val, idx) => {
      acc[idx + 1] = val;
      return acc;
    }, {});

  const runner: SqliteRunner = {
    exec: (sql) => db.exec(sql),
    getText: (sql, params = []) => {
      const row = prepare(sql).get(toBindings(params));
      if (row === undefined || row === null) return null;
      const val = Object.values(row as Record<string, unknown>)[0];
      if (val === undefined || val === null) return null;
      return String(val);
    },
  };

  if (db && (typeof db === 'object' || typeof db === 'function')) {
    sqliteRunnerCache.set(db as object, runner);
  }

  return runner;
}

export function createSqliteNodeApi(db: any, opts: { maxBulkOps?: number } = {}): TreecrdtAdapter {
  return createTreecrdtSqliteAdapter(createRunner(db), opts);
}

/**
 * High-level engine API (matches `@treecrdt/wa-sqlite` client shape).
 *
 * This is the recommended surface for applications: it exposes local op minting via the SQLite
 * extension UDFs and typed reads (decoded ops/opRefs/tree rows).
 */
export function createTreecrdtClient(
  db: any,
  opts: { docId?: string; maxBulkOps?: number } = {},
): Promise<TreecrdtEngine & { runner: SqliteRunner }> {
  const runner = createRunner(db);
  const adapter = createTreecrdtSqliteAdapter(runner, { maxBulkOps: opts.maxBulkOps });
  const docId = opts.docId ?? 'treecrdt';

  const ready = Promise.resolve(adapter.setDocId(docId));

  const localWriters = new Map<string, TreecrdtSqliteWriter>();
  const localWriterKey = (replica: ReplicaId) => bytesToHex(replica);
  const localWriterFor = (replica: ReplicaId) => {
    const key = localWriterKey(replica);
    const existing = localWriters.get(key);
    if (existing) return existing;
    const next = createTreecrdtSqliteWriter(runner, { replica });
    localWriters.set(key, next);
    return next;
  };

  const encodeReplica = (replica: Operation['meta']['id']['replica']): Uint8Array =>
    replicaIdToBytes(replica);

  const opsSinceImpl = async (lamport: number, root?: string) =>
    decodeSqliteOps(await adapter.opsSince(lamport, root));
  const opRefsAllImpl = async () => decodeSqliteOpRefs(await adapter.opRefsAll());
  const opRefsChildrenImpl = async (parent: string) =>
    decodeSqliteOpRefs(await adapter.opRefsChildren(nodeIdToBytes16(parent)));
  const opsByOpRefsImpl = async (opRefs: Uint8Array[]) =>
    decodeSqliteOps(
      await adapter.opsByOpRefs(
        opRefs.map((r) => (r instanceof Uint8Array ? r : Uint8Array.from(r))),
      ),
    );
  const treeChildrenImpl = async (parent: string) =>
    decodeSqliteNodeIds(await adapter.treeChildren(nodeIdToBytes16(parent)));
  const treeDumpImpl = async () => decodeSqliteTreeRows(await adapter.treeDump());
  const treeNodeCountImpl = async () => Number(await adapter.treeNodeCount());
  const headLamportImpl = async () => Number(await adapter.headLamport());
  const replicaMaxCounterImpl = async (replica: Operation['meta']['id']['replica']) =>
    Number(await adapter.replicaMaxCounter(encodeReplica(replica)));

  const localInsertImpl = async (
    replica: ReplicaId,
    parent: string,
    node: string,
    placement: TreecrdtSqlitePlacement,
    payload: Uint8Array | null,
  ) => localWriterFor(replica).insert(parent, node, placement, payload ? { payload } : {});
  const localMoveImpl = async (
    replica: ReplicaId,
    node: string,
    newParent: string,
    placement: TreecrdtSqlitePlacement,
  ) => localWriterFor(replica).move(node, newParent, placement);
  const localDeleteImpl = async (replica: ReplicaId, node: string) =>
    localWriterFor(replica).delete(node);
  const localPayloadImpl = async (replica: ReplicaId, node: string, payload: Uint8Array | null) =>
    localWriterFor(replica).payload(node, payload);

  return ready.then(() => ({
    mode: 'node',
    storage: 'sqlite',
    docId,
    ops: {
      append: async (op) => adapter.appendOp(op, nodeIdToBytes16, encodeReplica),
      appendMany: async (ops) => adapter.appendOps!(ops, nodeIdToBytes16, encodeReplica),
      all: () => opsSinceImpl(0),
      since: opsSinceImpl,
      children: async (parent) => opsByOpRefsImpl(await opRefsChildrenImpl(parent)),
      get: opsByOpRefsImpl,
    },
    opRefs: { all: opRefsAllImpl, children: opRefsChildrenImpl },
    tree: {
      children: treeChildrenImpl,
      childrenPage: async (parent, cursor, limit) =>
        decodeSqliteTreeChildRows(
          await adapter.treeChildrenPage!(nodeIdToBytes16(parent), cursor, limit),
        ),
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
    runner,
    close: async () => {
      if (typeof db?.close === 'function') db.close();
    },
  }));
}

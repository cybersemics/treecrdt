import { existsSync } from 'node:fs';
import fs from 'node:fs/promises';
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
import {
  bytesToHex,
  nodeIdFromBytes16,
  nodeIdToBytes16,
  replicaIdToBytes,
} from '@treecrdt/interface/ids';
import type { Operation, ReplicaId, TreecrdtAdapter } from '@treecrdt/interface';
import {
  createMaterializationDispatcher,
  createTreecrdtEngineLocal,
} from '@treecrdt/interface/engine';
import type { LocalWriteOptions, TreecrdtEngine, WriteOptions } from '@treecrdt/interface/engine';
import {
  createTreecrdtSqliteAuthApi,
  type TreecrdtSqliteAuthApi,
} from '@treecrdt/sync-sqlite/auth';

export type LoadOptions = {
  extensionPath?: string;
  entrypoint?: string;
};

// Minimal shape we need from better-sqlite3 Database.
export type LoadableDatabase = {
  loadExtension: (path: string, entryPoint?: string) => void;
};

export type SqliteNodeStorage = { type: 'memory' } | { type: 'file'; filename: string };
export type SqliteNodeRuntime = { type: 'direct' };
export type SqliteNodeClientOptions = {
  storage?: SqliteNodeStorage;
  runtime?: SqliteNodeRuntime;
  extension?: LoadOptions;
  docId?: string;
  maxBulkOps?: number;
};

export type TreecrdtSqliteNodeDatabaseClient = TreecrdtEngine & {
  mode: 'node';
  storage: 'sqlite';
  docId: string;
  runner: SqliteRunner;
  auth: TreecrdtSqliteAuthApi;
  close: () => Promise<void>;
};

export type SqliteNodeClient = Omit<TreecrdtSqliteNodeDatabaseClient, 'storage'> & {
  runtime: 'direct';
  storage: 'memory' | 'file';
  filename: string;
  drop: () => Promise<void>;
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SQLITE_FILE_SUFFIXES = ['', '-journal', '-wal', '-shm'];

function platformExt(platform = process.platform): '.dylib' | '.so' | '.dll' {
  switch (platform) {
    case 'darwin':
      return '.dylib';
    case 'win32':
      return '.dll';
    default:
      return '.so';
  }
}

function nativeExtensionFileName(platform = process.platform, arch = process.arch): string {
  const ext = platformExt(platform);
  const base = ext === '.dll' ? 'treecrdt_sqlite_ext' : 'libtreecrdt_sqlite_ext';
  return `${base}-${platform}-${arch}${ext}`;
}

/**
 * Resolve the bundled TreeCRDT SQLite extension for this platform.
 * Falls back to the `native/` directory within this package.
 */
export function defaultExtensionPath(): string {
  return path.resolve(__dirname, '../native', nativeExtensionFileName());
}

/**
 * Load the TreeCRDT SQLite extension into a better-sqlite3 Database.
 */
export function loadTreecrdtExtension(db: LoadableDatabase, opts: LoadOptions = {}): string {
  const path = opts.extensionPath ?? defaultExtensionPath();
  const entrypoint = opts.entrypoint ?? 'sqlite3_treecrdt_init';
  if (!existsSync(path)) {
    throw new Error(
      `TreeCRDT SQLite native extension not found at ${path}. This package may not include a native artifact for ${process.platform}/${process.arch}; install a supported package version or pass a custom extensionPath.`,
    );
  }
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

async function loadDatabaseCtor(): Promise<new (filename: string) => any> {
  return (
    (await import('better-sqlite3').catch((err) => {
      throw new Error(
        `better-sqlite3 native binding not available; ensure it is installed/built before using @treecrdt/sqlite-node: ${err}`,
      );
    })) as { default: new (filename: string) => any }
  ).default;
}

function normalizeManagedRuntime(opts: SqliteNodeClientOptions): SqliteNodeRuntime {
  const runtime = opts.runtime ?? { type: 'direct' };
  if (!runtime || typeof runtime !== 'object' || runtime.type !== 'direct') {
    throw new Error('@treecrdt/sqlite-node only supports runtime: { type: "direct" }');
  }
  return runtime;
}

function normalizeManagedStorage(opts: SqliteNodeClientOptions): SqliteNodeStorage {
  const storage = opts.storage ?? { type: 'memory' };
  if (!storage || typeof storage !== 'object') {
    throw new Error(
      '@treecrdt/sqlite-node storage must use object options, e.g. { type: "memory" } or { type: "file", filename }',
    );
  }
  if (storage.type === 'memory') return storage;
  if (storage.type === 'file') {
    if (!storage.filename) {
      throw new Error('@treecrdt/sqlite-node file storage requires a filename');
    }
    return storage;
  }
  throw new Error('@treecrdt/sqlite-node storage.type must be "memory" or "file"');
}

async function removeSqliteFiles(filename: string): Promise<void> {
  await Promise.all(
    SQLITE_FILE_SUFFIXES.map((suffix) => fs.rm(`${filename}${suffix}`, { force: true })),
  );
}

/**
 * Managed Node runtime entrypoint for native SQLite.
 *
 * This mirrors the runtime/storage matrix used by `@treecrdt/wa-sqlite`, but keeps the native
 * Node package explicit: direct runtime only, backed by either an in-memory or file database.
 */
export async function createTreecrdtClient(
  opts: SqliteNodeClientOptions = {},
): Promise<SqliteNodeClient> {
  normalizeManagedRuntime(opts);
  const storage = normalizeManagedStorage(opts);
  const filename = storage.type === 'file' ? storage.filename : ':memory:';

  if (storage.type === 'file') {
    await fs.mkdir(path.dirname(filename), { recursive: true });
  }

  const Database = await loadDatabaseCtor();
  const db = new Database(filename);
  loadTreecrdtExtension(db, opts.extension);

  const client = await createTreecrdtClientFromDatabase(db, {
    docId: opts.docId,
    maxBulkOps: opts.maxBulkOps,
  });

  let closed = false;
  const close = async () => {
    if (closed) return;
    closed = true;
    await client.close();
  };
  const drop = async () => {
    await close();
    if (storage.type === 'file') {
      await removeSqliteFiles(filename);
    }
  };

  return {
    ...client,
    runtime: 'direct',
    storage: storage.type,
    filename,
    close,
    drop,
  };
}

/**
 * Wrap an existing better-sqlite3 Database in the TreeCRDT engine API.
 */
export function createTreecrdtClientFromDatabase(
  db: any,
  opts: { docId?: string; maxBulkOps?: number } = {},
): Promise<TreecrdtSqliteNodeDatabaseClient> {
  const runner = createRunner(db);
  const materialized = createMaterializationDispatcher();
  const adapter = createTreecrdtSqliteAdapter(runner, {
    maxBulkOps: opts.maxBulkOps,
    onMaterialized: materialized.emitEvent,
  });
  const docId = opts.docId ?? 'treecrdt';

  const ready = Promise.resolve(adapter.setDocId(docId));

  const localWriters = new Map<string, TreecrdtSqliteWriter>();
  const localWriterKey = (replica: ReplicaId) => bytesToHex(replica);
  const localWriterFor = (replica: ReplicaId) => {
    const key = localWriterKey(replica);
    const existing = localWriters.get(key);
    if (existing) return existing;
    const next = createTreecrdtSqliteWriter(runner, {
      replica,
      onMaterialized: materialized.emitEvent,
    });
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
  const treeParentImpl = async (node: string) => {
    const result = await adapter.treeParent(nodeIdToBytes16(node));
    return result === null ? null : nodeIdFromBytes16(result);
  };
  const treeExistsImpl = async (node: string) => adapter.treeExists(nodeIdToBytes16(node));
  const treeGetPayloadImpl = async (node: string) => adapter.treePayload(nodeIdToBytes16(node));
  const headLamportImpl = async () => Number(await adapter.headLamport());
  const replicaMaxCounterImpl = async (replica: Operation['meta']['id']['replica']) =>
    Number(await adapter.replicaMaxCounter(encodeReplica(replica)));

  const localInsertImpl = async (
    replica: ReplicaId,
    parent: string,
    node: string,
    placement: TreecrdtSqlitePlacement,
    payload: Uint8Array | null,
    writeOpts?: LocalWriteOptions,
  ) =>
    localWriterFor(replica).insert(parent, node, placement, {
      ...writeOpts,
      ...(payload ? { payload } : {}),
    });
  const localMoveImpl = async (
    replica: ReplicaId,
    node: string,
    newParent: string,
    placement: TreecrdtSqlitePlacement,
    writeOpts?: LocalWriteOptions,
  ) => localWriterFor(replica).move(node, newParent, placement, writeOpts);
  const localDeleteImpl = async (replica: ReplicaId, node: string, writeOpts?: LocalWriteOptions) =>
    localWriterFor(replica).delete(node, writeOpts);
  const localPayloadImpl = async (
    replica: ReplicaId,
    node: string,
    payload: Uint8Array | null,
    writeOpts?: LocalWriteOptions,
  ) => localWriterFor(replica).payload(node, payload, writeOpts);

  const local = createTreecrdtEngineLocal({
    insert: localInsertImpl,
    move: localMoveImpl,
    delete: localDeleteImpl,
    payload: localPayloadImpl,
  });

  return ready.then(() => ({
    mode: 'node',
    storage: 'sqlite',
    docId,
    ops: {
      append: async (op, writeOpts?: WriteOptions) => {
        const outcome = await adapter.appendOp(op, nodeIdToBytes16, encodeReplica);
        materialized.emitOutcome(outcome, writeOpts?.writeId);
      },
      appendMany: async (ops, writeOpts?: WriteOptions) => {
        const outcome = await adapter.appendOps!(ops, nodeIdToBytes16, encodeReplica);
        materialized.emitOutcome(outcome, writeOpts?.writeId);
      },
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
      parent: treeParentImpl,
      exists: treeExistsImpl,
      getPayload: treeGetPayloadImpl,
    },
    meta: { headLamport: headLamportImpl, replicaMaxCounter: replicaMaxCounterImpl },
    auth: createTreecrdtSqliteAuthApi({ runner, docId }),
    local,
    onMaterialized: materialized.onMaterialized,
    runner,
    close: async () => {
      if (typeof db?.close === 'function') db.close();
    },
  }));
}

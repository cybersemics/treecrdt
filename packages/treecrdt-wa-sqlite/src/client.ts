import { clearOpfsStorage, detectOpfsSupport } from './opfs.js';
import type { Operation, ReplicaId } from '@treecrdt/interface';
import {
  createTreecrdtSqliteWriter,
  decodeSqliteNodeIds,
  decodeSqliteOpRefs,
  decodeSqliteOps,
  decodeSqliteTreeChildRows,
  decodeSqliteTreeRows,
  type SqliteTreeChildRow,
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
import type { TreecrdtEngine, WriteOptions } from '@treecrdt/interface/engine';
import { createMaterializationDispatcher } from '@treecrdt/interface/engine';
import { dbGetText } from './sql.js';
import type { Database } from './index.js';
import type {
  RpcMethod,
  RpcParams,
  RpcPushMessage,
  RpcRequest,
  RpcResponse,
  RpcResult,
} from './rpc.js';
import { openTreecrdtDb } from './open.js';

export const CLIENT_CLOSED_ERROR = 'TreecrdtClient was closed';

export type StorageMode = 'memory' | 'opfs';
export type ClientMode = 'direct' | 'worker';

export type TreecrdtClient = TreecrdtEngine & {
  mode: ClientMode;
  storage: StorageMode;
  runner: SqliteRunner;
  drop: () => Promise<void>;
};

export type ClientOptions = {
  storage?: StorageMode | 'auto';
  baseUrl?: string; // where wa-sqlite assets live; defaults to import.meta.env.BASE_URL + wa-sqlite/
  filename?: string; // only for opfs; defaults to /treecrdt-playground.db
  preferWorker?: boolean; // when true (default for opfs), use a worker instead of main-thread SQLite
  docId?: string; // used for v0 sync opRef derivation inside the extension
};

export async function createTreecrdtClient(opts: ClientOptions = {}): Promise<TreecrdtClient> {
  const storage = opts.storage === 'memory' ? 'memory' : opts.storage === 'opfs' ? 'opfs' : 'auto';
  const requireOpfs = opts.storage === 'opfs';
  const docId = opts.docId ?? 'treecrdt';
  const rawBase =
    opts.baseUrl ??
    (typeof import.meta !== 'undefined' && (import.meta as any).env?.BASE_URL
      ? (import.meta as any).env.BASE_URL
      : '/');
  const baseUrl = rawBase.endsWith('/') ? rawBase : `${rawBase}/`;
  const support = detectOpfsSupport();

  const shouldUseOpfs = storage === 'opfs' || (storage === 'auto' && support.available);
  const preferWorker = opts.preferWorker ?? shouldUseOpfs; // default to worker when targeting OPFS

  // If OPFS requested, default to worker path to avoid main-thread sync handles.
  if (shouldUseOpfs) {
    if (preferWorker) {
      return createWorkerClient({
        baseUrl,
        filename: opts.filename,
        storage: 'opfs',
        requireOpfs,
        docId,
      });
    }
    if (!support.available) {
      throw new Error(
        `OPFS unavailable in this environment: ${support.reason ?? 'unknown reason'}`,
      );
    }
  }

  // Direct path.
  return createDirectClient({
    baseUrl,
    filename: opts.filename,
    storage: shouldUseOpfs ? 'opfs' : 'memory',
    requireOpfs,
    docId,
  });
}

// --- Worker client

type WorkerProxy = {
  postMessage(msg: RpcRequest, transfer?: Transferable[]): void;
  terminate: () => void;
  addEventListener: (type: 'message' | 'error', fn: (ev: any) => void) => void;
  removeEventListener: (type: 'message' | 'error', fn: (ev: any) => void) => void;
};

type RpcCall = <M extends RpcMethod>(method: M, params: RpcParams<M>) => Promise<RpcResult<M>>;

async function createWorkerClient(opts: {
  baseUrl: string;
  filename?: string;
  storage: StorageMode;
  docId: string;
  requireOpfs?: boolean;
}): Promise<TreecrdtClient> {
  const materialized = createMaterializationDispatcher();
  // Keep the URL inline so Vite detects and bundles the worker properly.
  const worker = new Worker(new URL('./worker.js', import.meta.url), {
    type: 'module',
  }) as unknown as WorkerProxy;
  let nextId = 1;
  const pending = new Map<
    number,
    { resolve: (value: any) => void; reject: (err: Error) => void }
  >();
  let terminalError: Error | null = null;
  let closed = false;

  const closedError = new Error(CLIENT_CLOSED_ERROR);

  const call = <M extends RpcMethod>(method: M, params: RpcParams<M>): Promise<RpcResult<M>> => {
    if (closed) return Promise.reject(closedError);
    const id = nextId++;
    if (terminalError) return Promise.reject(terminalError);
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      worker.postMessage({ id, method, params } satisfies RpcRequest<M>);
    });
  };

  const onMessage = (ev: MessageEvent<RpcResponse | RpcPushMessage>) => {
    const data = ev.data;
    if ('type' in data && data.type === 'materialized') {
      materialized.emitEvent(data.event);
      return;
    }
    const response = data as RpcResponse;
    const handler = pending.get(response.id as number);
    if (!handler) return;
    pending.delete(response.id as number);
    if (response.ok) handler.resolve(response.result);
    else handler.reject(new Error(response.error || 'worker error'));
  };
  const onError = (ev: ErrorEvent) => {
    const err = new Error(ev.message || 'worker error');
    terminalError = err;
    for (const { reject } of pending.values()) reject(err);
    pending.clear();
  };
  worker.addEventListener('message', onMessage);
  worker.addEventListener('error', onError);

  // init
  const initResult = (await call('init', [
    opts.baseUrl,
    opts.filename,
    opts.storage,
    opts.docId,
  ])) as { storage?: StorageMode; opfsError?: string } | undefined;
  const effectiveStorage: StorageMode = initResult?.storage === 'opfs' ? 'opfs' : 'memory';
  const cleanup = () => {
    closed = true;
    for (const { reject } of pending.values()) reject(closedError);
    pending.clear();
    worker.removeEventListener('error', onError);
    worker.removeEventListener('message', onMessage);
    worker.terminate();
  };

  if (opts.requireOpfs && effectiveStorage !== 'opfs') {
    const reason = initResult?.opfsError ? `: ${initResult.opfsError}` : '';
    try {
      if (!terminalError) await call('close', [] as RpcParams<'close'>);
    } catch {
      // ignore close errors on init failure
    } finally {
      cleanup();
    }
    throw new Error(`OPFS requested but could not be initialized${reason}`);
  }

  const closeImpl = async () => {
    if (closed) return;
    try {
      if (!terminalError) await call('close', [] as RpcParams<'close'>);
      cleanup();
    } finally {
      // noop: cleanup already handles terminal teardown, and repeated close is idempotent
    }
  };

  const dropImpl = async () => {
    if (closed) return;
    try {
      if (!terminalError) await call('drop', [] as RpcParams<'drop'>);
      cleanup();
    } finally {
      // noop: cleanup already handles terminal teardown, and repeated drop is idempotent
    }
  };

  return makeTreecrdtClientFromCall({
    mode: 'worker',
    storage: effectiveStorage,
    docId: opts.docId,
    call,
    materialized,
    close: closeImpl,
    drop: dropImpl,
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
  const materialized = createMaterializationDispatcher();
  const { baseUrl, storage, requireOpfs } = opts;
  const opened = await openTreecrdtDb({
    baseUrl,
    filename: opts.filename,
    storage,
    docId: opts.docId,
    requireOpfs,
    onMaterialized: materialized.emitEvent,
  });
  const db = opened.db;
  const finalStorage: StorageMode = opened.storage;
  const filename = opened.filename;
  const adapter = opened.api;
  const runner: SqliteRunner = {
    exec: (sql) => db.exec(sql),
    getText: (sql, params = []) => dbGetText(db, sql, params),
  };
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
  const wrapError = (stage: string, err: unknown) =>
    new Error(
      JSON.stringify({
        stage,
        storage: finalStorage,
        filename,
        baseUrl,
        message: err instanceof Error ? err.message : String(err),
      }),
    );
  let closed = false;
  const closedError = new Error(CLIENT_CLOSED_ERROR);

  const call: RpcCall = async (method, params) => {
    if (closed) throw closedError;
    try {
      switch (method) {
        case 'sqlExec': {
          const [sql] = params as RpcParams<'sqlExec'>;
          await db.exec(sql);
          return undefined as any;
        }
        case 'sqlGetText': {
          const [sql, rawParams] = params as RpcParams<'sqlGetText'>;
          return dbGetText(db, sql, (rawParams ?? []) as unknown[]) as any;
        }
        case 'append': {
          const [op] = params as RpcParams<'append'>;
          return (await adapter.appendOp(op, nodeIdToBytes16, encodeReplica)) as any;
        }
        case 'appendMany': {
          const [ops] = params as RpcParams<'appendMany'>;
          return (await adapter.appendOps!(ops, nodeIdToBytes16, encodeReplica)) as any;
        }
        case 'opsSince': {
          const [lamport, root] = params as RpcParams<'opsSince'>;
          return (await adapter.opsSince(lamport, root)) as any;
        }
        case 'opRefsAll':
          return (await adapter.opRefsAll()) as any;
        case 'opRefsChildren': {
          const [parent] = params as RpcParams<'opRefsChildren'>;
          return (await adapter.opRefsChildren(nodeIdToBytes16(parent))) as any;
        }
        case 'opsByOpRefs': {
          const [opRefs] = params as RpcParams<'opsByOpRefs'>;
          return (await adapter.opsByOpRefs(opRefs.map((r) => Uint8Array.from(r)))) as any;
        }
        case 'treeChildren': {
          const [parent] = params as RpcParams<'treeChildren'>;
          return (await adapter.treeChildren(nodeIdToBytes16(parent))) as any;
        }
        case 'treeChildrenPage': {
          const [parent, cursor, limit] = params as RpcParams<'treeChildrenPage'>;
          const cursorBytes = cursor
            ? {
                orderKey: Uint8Array.from(cursor.orderKey),
                node: Uint8Array.from(cursor.node),
              }
            : null;
          return (await adapter.treeChildrenPage!(
            nodeIdToBytes16(parent),
            cursorBytes,
            limit,
          )) as any;
        }
        case 'treeDump':
          return (await adapter.treeDump()) as any;
        case 'treePayload': {
          const [node] = params as RpcParams<'treePayload'>;
          const payload = await adapter.treePayload(nodeIdToBytes16(node));
          return (payload === null ? null : Array.from(payload)) as any;
        }
        case 'treeNodeCount':
          return (await adapter.treeNodeCount()) as any;
        case 'treeParent': {
          const [node] = params as RpcParams<'treeParent'>;
          const result = await adapter.treeParent(nodeIdToBytes16(node));
          return result ? Array.from(result) : null;
        }
        case 'treeExists': {
          const [node] = params as RpcParams<'treeExists'>;
          return (await adapter.treeExists(nodeIdToBytes16(node))) as any;
        }
        case 'headLamport':
          return (await adapter.headLamport()) as any;
        case 'replicaMaxCounter': {
          const [rawReplica] = params as RpcParams<'replicaMaxCounter'>;
          return (await adapter.replicaMaxCounter(Uint8Array.from(rawReplica))) as any;
        }
        case 'localInsert': {
          const [replica, parent, node, placement, payload] = params as RpcParams<'localInsert'>;
          return (await localWriterFor(Uint8Array.from(replica)).insert(
            parent,
            node,
            placement,
            payload ? { payload } : {},
          )) as any;
        }
        case 'localMove': {
          const [replica, node, newParent, placement] = params as RpcParams<'localMove'>;
          return (await localWriterFor(Uint8Array.from(replica)).move(
            node,
            newParent,
            placement,
          )) as any;
        }
        case 'localDelete': {
          const [replica, node] = params as RpcParams<'localDelete'>;
          return (await localWriterFor(Uint8Array.from(replica)).delete(node)) as any;
        }
        case 'localPayload': {
          const [replica, node, payload] = params as RpcParams<'localPayload'>;
          return (await localWriterFor(Uint8Array.from(replica)).payload(node, payload)) as any;
        }
        case 'close': {
          if (db.close) await db.close();
          return undefined as any;
        }
        case 'drop': {
          if (db.close) await db.close();
          if (finalStorage === 'opfs') {
            await clearOpfsStorage(filename);
          }
          return undefined as any;
        }
        default:
          throw new Error(`unsupported direct method: ${method}`);
      }
    } catch (err) {
      throw wrapError(method, err);
    }
  };

  return makeTreecrdtClientFromCall({
    mode: 'direct',
    storage: finalStorage,
    docId: opts.docId,
    call,
    materialized,
    close: async () => {
      if (closed) return;
      if (db.close) await db.close();
      closed = true;
    },
    drop: async () => {
      if (closed) return;
      if (db.close) await db.close();
      if (finalStorage === 'opfs') {
        await clearOpfsStorage(filename);
      }
      closed = true;
    },
  });
}

// --- helpers

function makeTreecrdtClientFromCall(opts: {
  mode: ClientMode;
  storage: StorageMode;
  docId: string;
  call: RpcCall;
  materialized: ReturnType<typeof createMaterializationDispatcher>;
  close: () => Promise<void>;
  drop: () => Promise<void>;
}): TreecrdtClient {
  const call = opts.call;
  const materialized = opts.materialized;
  let closePromise: Promise<void> | null = null;

  const runner: SqliteRunner = {
    exec: (sql) => call('sqlExec', [sql]).then(() => undefined),
    getText: (sql, params = []) => call('sqlGetText', [sql, params]),
  };

  const opsSinceImpl = async (lamport: number, root?: string) => {
    const rows = await call('opsSince', [lamport, root]);
    return decodeSqliteOps(rows);
  };
  const opRefsAllImpl = async () => decodeSqliteOpRefs(await call('opRefsAll', []));
  const opRefsChildrenImpl = async (parent: string) =>
    decodeSqliteOpRefs(await call('opRefsChildren', [parent]));
  const opsByOpRefsImpl = async (opRefs: Uint8Array[]) =>
    decodeSqliteOps(await call('opsByOpRefs', [opRefs.map((r) => Array.from(r))]));
  const treeChildrenImpl = async (parent: string) =>
    decodeSqliteNodeIds(await call('treeChildren', [parent]));
  const treeChildrenPageImpl = async (
    parent: string,
    cursor: { orderKey: Uint8Array; node: Uint8Array } | null,
    limit: number,
  ): Promise<SqliteTreeChildRow[]> => {
    const rpcCursor = cursor
      ? { orderKey: Array.from(cursor.orderKey), node: Array.from(cursor.node) }
      : null;
    return decodeSqliteTreeChildRows(await call('treeChildrenPage', [parent, rpcCursor, limit]));
  };
  const treeDumpImpl = async () => decodeSqliteTreeRows(await call('treeDump', []));
  const treeNodeCountImpl = async () => Number(await call('treeNodeCount', []));
  const treeParentImpl = async (node: string) => {
    const result = await call('treeParent', [node]);
    if (result === null) return null;
    return nodeIdFromBytes16(Uint8Array.from(result));
  };
  const treeExistsImpl = async (node: string) => Boolean(await call('treeExists', [node]));
  const treeGetPayloadImpl = async (node: string) => {
    const result = await call('treePayload', [node]);
    return result === null ? null : Uint8Array.from(result);
  };
  const headLamportImpl = async () => Number(await call('headLamport', []));
  const replicaMaxCounterImpl = async (replica: Operation['meta']['id']['replica']) =>
    Number(await call('replicaMaxCounter', [Array.from(encodeReplica(replica))]));
  const localInsertImpl = async (
    replica: ReplicaId,
    parent: string,
    node: string,
    placement: TreecrdtSqlitePlacement,
    payload: Uint8Array | null,
  ) => {
    const rid = Array.from(replica);
    return (await call('localInsert', [
      rid,
      parent,
      node,
      placement,
      payload,
    ])) as unknown as Operation;
  };
  const localMoveImpl = async (
    replica: ReplicaId,
    node: string,
    newParent: string,
    placement: TreecrdtSqlitePlacement,
  ) => {
    const rid = Array.from(replica);
    return (await call('localMove', [rid, node, newParent, placement])) as unknown as Operation;
  };
  const localDeleteImpl = async (replica: ReplicaId, node: string) => {
    const rid = Array.from(replica);
    return (await call('localDelete', [rid, node])) as unknown as Operation;
  };
  const localPayloadImpl = async (replica: ReplicaId, node: string, payload: Uint8Array | null) => {
    const rid = Array.from(replica);
    return (await call('localPayload', [rid, node, payload])) as unknown as Operation;
  };

  const closeImpl = async () => {
    if (closePromise) return await closePromise;
    closePromise = (async () => {
      try {
        await opts.close();
      } catch {
        // Client teardown is best-effort. Fast refresh and overlapping resets can race a prior
        // close, and the underlying sqlite handle may already be gone by the time this runs.
      }
    })();
    await closePromise;
  };

  return {
    mode: opts.mode,
    storage: opts.storage,
    docId: opts.docId,
    runner,
    ops: {
      append: async (op, writeOpts?: WriteOptions) => {
        const outcome = await call('append', [op, writeOpts]);
        materialized.emitOutcome(outcome, writeOpts?.writeId);
      },
      appendMany: async (ops, writeOpts?: WriteOptions) => {
        const outcome = await call('appendMany', [ops, writeOpts]);
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
      childrenPage: treeChildrenPageImpl,
      dump: treeDumpImpl,
      nodeCount: treeNodeCountImpl,
      parent: treeParentImpl,
      exists: treeExistsImpl,
      getPayload: treeGetPayloadImpl,
    },
    meta: { headLamport: headLamportImpl, replicaMaxCounter: replicaMaxCounterImpl },
    local: {
      insert: localInsertImpl,
      move: localMoveImpl,
      delete: localDeleteImpl,
      payload: localPayloadImpl,
    },
    onMaterialized: materialized.onMaterialized,
    close: closeImpl,
    drop: opts.drop,
  };
}

function encodeReplica(replica: Operation['meta']['id']['replica']): Uint8Array {
  return replicaIdToBytes(replica);
}

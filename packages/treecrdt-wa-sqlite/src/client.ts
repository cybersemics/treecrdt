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
import type {
  LocalWriteOptions,
  MaterializationEvent,
  TreecrdtEngine,
  WriteOptions,
} from '@treecrdt/interface/engine';
import {
  createMaterializationDispatcher,
  createTreecrdtEngineLocal,
} from '@treecrdt/interface/engine';
import type { TreecrdtSqliteAuthApi } from '@treecrdt/sync-sqlite/auth';
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
export type RuntimeMode = 'direct' | 'dedicated-worker' | 'shared-worker';
export type TreecrdtStorage =
  | { type: 'memory' }
  | { type: 'opfs'; filename?: string; fallback?: 'throw' | 'memory' }
  | { type: 'auto'; filename?: string; fallback?: 'memory' | 'throw' };
export type TreecrdtRuntime =
  | { type: 'auto' }
  | { type: 'direct' }
  | { type: 'dedicated-worker'; workerUrl?: string | URL }
  | { type: 'shared-worker'; workerUrl?: string | URL; name?: string };
export type TreecrdtAssets = {
  baseUrl?: string;
};

export type TreecrdtClient = TreecrdtEngine & {
  mode: ClientMode;
  runtime: RuntimeMode;
  storage: StorageMode;
  runner: SqliteRunner;
  auth: TreecrdtClientAuthApi;
  drop: () => Promise<void>;
};

type TreecrdtSqliteAuthModule = typeof import('@treecrdt/sync-sqlite/auth');

export type TreecrdtClientAuthApi = {
  createSession: (
    ...args: Parameters<TreecrdtSqliteAuthApi['createSession']>
  ) => Promise<ReturnType<TreecrdtSqliteAuthApi['createSession']>>;
  describeCapabilityToken: TreecrdtSqliteAuthApi['describeCapabilityToken'];
  evaluateScope: (
    ...args: Parameters<TreecrdtSqliteAuthApi['evaluateScope']>
  ) => Promise<Awaited<ReturnType<TreecrdtSqliteAuthApi['evaluateScope']>>>;
};

export type ClientOptions = {
  storage?: TreecrdtStorage;
  runtime?: TreecrdtRuntime;
  assets?: TreecrdtAssets;
  docId?: string; // used for v0 sync opRef derivation inside the extension
};

type NormalizedStorageOptions = {
  type: StorageMode | 'auto';
  filename?: string;
  requireOpfs: boolean;
  fallback: 'memory' | 'throw';
};

type NormalizedRuntimeOptions = TreecrdtRuntime;

function normalizeStorageOptions(opts: ClientOptions): NormalizedStorageOptions {
  const raw = opts.storage ?? { type: 'auto' };
  if (!raw || typeof raw !== 'object') {
    throw new Error(
      'createTreecrdtClient storage must use object options, e.g. { type: "memory" } or { type: "opfs" }',
    );
  }

  if (raw.type === 'memory') {
    return { type: 'memory', requireOpfs: false, fallback: 'memory' };
  }
  if (raw.type === 'opfs') {
    const fallback = raw.fallback ?? 'throw';
    return {
      type: 'opfs',
      filename: raw.filename,
      requireOpfs: fallback === 'throw',
      fallback,
    };
  }
  if (raw.type !== 'auto') {
    throw new Error('createTreecrdtClient storage.type must be "memory", "opfs", or "auto"');
  }
  const fallback = raw.fallback ?? 'memory';
  return {
    type: 'auto',
    filename: raw.filename,
    requireOpfs: fallback === 'throw',
    fallback,
  };
}

function normalizeRuntimeOptions(opts: ClientOptions): NormalizedRuntimeOptions {
  return opts.runtime ?? { type: 'auto' };
}

export async function createTreecrdtClient(opts: ClientOptions = {}): Promise<TreecrdtClient> {
  const storage = normalizeStorageOptions(opts);
  const runtime = normalizeRuntimeOptions(opts);
  const docId = opts.docId ?? 'treecrdt';
  const rawBase =
    opts.assets?.baseUrl ??
    (typeof import.meta !== 'undefined' && (import.meta as any).env?.BASE_URL
      ? (import.meta as any).env.BASE_URL
      : '/');
  const baseUrl = rawBase.endsWith('/') ? rawBase : `${rawBase}/`;
  const support = detectOpfsSupport();

  const shouldUseOpfs = storage.type === 'opfs' || (storage.type === 'auto' && support.available);
  if (storage.type === 'auto' && !support.available && storage.fallback === 'throw') {
    throw new Error(`OPFS unavailable in this environment: ${support.reason ?? 'unknown reason'}`);
  }
  const resolvedRuntime = resolveRuntimeMode(runtime, shouldUseOpfs);

  // If OPFS is requested, default runtime:auto to a worker path to avoid main-thread sync handles.
  if (shouldUseOpfs && resolvedRuntime === 'direct' && !support.available && storage.requireOpfs) {
    throw new Error(`OPFS unavailable in this environment: ${support.reason ?? 'unknown reason'}`);
  }

  if (resolvedRuntime === 'shared-worker') {
    return createSharedWorkerClient({
      baseUrl,
      filename: storage.filename,
      storage: shouldUseOpfs ? 'opfs' : 'memory',
      requireOpfs: storage.requireOpfs,
      docId,
      workerUrl: runtime.type === 'shared-worker' ? runtime.workerUrl : undefined,
      name:
        runtime.type === 'shared-worker'
          ? (runtime.name ??
            defaultSharedWorkerName(docId, shouldUseOpfs ? storage.filename : ':memory:'))
          : defaultSharedWorkerName(docId, shouldUseOpfs ? storage.filename : ':memory:'),
    });
  }

  if (resolvedRuntime === 'dedicated-worker') {
    return createWorkerClient({
      baseUrl,
      filename: storage.filename,
      storage: shouldUseOpfs ? 'opfs' : 'memory',
      requireOpfs: storage.requireOpfs,
      docId,
      workerUrl: runtime.type === 'dedicated-worker' ? runtime.workerUrl : undefined,
    });
  }

  return createDirectClient({
    baseUrl,
    filename: storage.filename,
    storage: shouldUseOpfs ? 'opfs' : 'memory',
    requireOpfs: storage.requireOpfs,
    docId,
  });
}

function resolveRuntimeMode(runtime: TreecrdtRuntime, shouldUseOpfs: boolean): RuntimeMode {
  if (runtime.type === 'direct') return 'direct';
  if (runtime.type === 'dedicated-worker') return 'dedicated-worker';
  if (runtime.type === 'shared-worker') {
    if (typeof SharedWorker === 'undefined') {
      throw new Error('TreeCRDT shared-worker runtime is unavailable in this environment');
    }
    return 'shared-worker';
  }
  if (shouldUseOpfs) return 'dedicated-worker';
  return 'direct';
}

function defaultSharedWorkerName(docId: string, filename?: string): string {
  return `treecrdt:${docId}:${filename ?? '/treecrdt.db'}`;
}

// --- Worker client

type WorkerProxy = {
  postMessage(msg: RpcRequest, transfer?: Transferable[]): void;
  terminate: () => void;
  addEventListener: (type: 'message' | 'error', fn: (ev: any) => void) => void;
  removeEventListener: (type: 'message' | 'error', fn: (ev: any) => void) => void;
};

type MessagePortProxy = {
  postMessage(msg: RpcRequest, transfer?: Transferable[]): void;
  start: () => void;
  close: () => void;
  addEventListener: (type: 'message' | 'messageerror', fn: (ev: any) => void) => void;
  removeEventListener: (type: 'message' | 'messageerror', fn: (ev: any) => void) => void;
};

type RpcCall = <M extends RpcMethod>(method: M, params: RpcParams<M>) => Promise<RpcResult<M>>;
type ClientMaterializationDispatcher = ReturnType<typeof createMaterializationDispatcher> & {
  enableCrossTab: (scope: CrossTabMaterializationScope) => void;
  emitIncomingEvent: (event: MaterializationEvent) => void;
  close: () => void;
};
type ClientMaterializationDispatcherOptions = {
  broadcast?: (event: MaterializationEvent) => void;
};
type CrossTabMaterializationScope = {
  docId: string;
  filename: string;
};
type CrossTabMaterializationMessage = {
  type: 'treecrdt-materialized-v1';
  sourceId: string;
  docId: string;
  filename: string;
  event: MaterializationEvent;
};

const CROSS_TAB_MATERIALIZED_MESSAGE = 'treecrdt-materialized-v1';

let sqliteAuthModulePromise: Promise<TreecrdtSqliteAuthModule> | null = null;

function loadSqliteAuthModule(): Promise<TreecrdtSqliteAuthModule> {
  // Auth is an opt-in capability path for browser clients. Apps that only open
  // local trees do not need auth sessions or proof material until they call client.auth.*.
  sqliteAuthModulePromise ??= import('@treecrdt/sync-sqlite/auth');
  return sqliteAuthModulePromise;
}

function createLazyAuthApi(opts: { runner: SqliteRunner; docId: string }): TreecrdtClientAuthApi {
  let authApiPromise: Promise<TreecrdtSqliteAuthApi> | null = null;
  const getAuthApi = () => {
    authApiPromise ??= loadSqliteAuthModule().then(({ createTreecrdtSqliteAuthApi }) =>
      createTreecrdtSqliteAuthApi(opts),
    );
    return authApiPromise;
  };

  return {
    createSession: async (...args) => (await getAuthApi()).createSession(...args),
    describeCapabilityToken: async (...args) =>
      (await getAuthApi()).describeCapabilityToken(...args),
    evaluateScope: async (...args) => await (await getAuthApi()).evaluateScope(...args),
  };
}

function createClientMaterializationDispatcher(
  opts: ClientMaterializationDispatcherOptions = {},
): ClientMaterializationDispatcher {
  const dispatcher = createMaterializationDispatcher();
  const clientId = randomClientId();
  let channel: BroadcastChannel | null = null;
  let scope: CrossTabMaterializationScope | null = null;

  const close = () => {
    channel?.close();
    channel = null;
    scope = null;
  };

  const eventForPeers = (event: MaterializationEvent): MaterializationEvent => {
    const { writeIds: _writeIds, ...nextEvent } = event;
    return nextEvent;
  };

  const broadcast = (event: MaterializationEvent) => {
    if (!channel || !scope || event.changes.length === 0) return;
    channel.postMessage({
      type: CROSS_TAB_MATERIALIZED_MESSAGE,
      sourceId: clientId,
      docId: scope.docId,
      filename: scope.filename,
      event: eventForPeers(event),
    } satisfies CrossTabMaterializationMessage);
  };

  const emitEvent = (event: MaterializationEvent) => {
    if (event.changes.length === 0) return;
    dispatcher.emitEvent(event);
    opts.broadcast?.(eventForPeers(event));
    broadcast(event);
  };

  const emitOutcome: ClientMaterializationDispatcher['emitOutcome'] = (outcome, writeId) => {
    if (outcome.changes.length === 0) return;
    emitEvent({
      ...outcome,
      ...(writeId ? { writeIds: [writeId] } : {}),
    });
  };

  const enableCrossTab = (nextScope: CrossTabMaterializationScope) => {
    if (typeof BroadcastChannel === 'undefined') return;
    close();
    scope = nextScope;
    channel = new BroadcastChannel(materializationChannelName(nextScope));
    channel.onmessage = (ev: MessageEvent<CrossTabMaterializationMessage>) => {
      const msg = ev.data;
      if (!msg || msg.type !== CROSS_TAB_MATERIALIZED_MESSAGE) return;
      if (msg.sourceId === clientId) return;
      if (msg.docId !== nextScope.docId || msg.filename !== nextScope.filename) return;
      dispatcher.emitEvent(msg.event);
    };
  };

  return {
    emitEvent,
    emitOutcome,
    emitIncomingEvent: dispatcher.emitEvent,
    onMaterialized: dispatcher.onMaterialized,
    enableCrossTab,
    close,
  };
}

function materializationChannelName(scope: CrossTabMaterializationScope): string {
  return `${CROSS_TAB_MATERIALIZED_MESSAGE}:${scope.docId}:${scope.filename}`;
}

function randomClientId(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

async function createWorkerClient(opts: {
  baseUrl: string;
  filename?: string;
  storage: StorageMode;
  docId: string;
  requireOpfs?: boolean;
  workerUrl?: string | URL;
}): Promise<TreecrdtClient> {
  const materialized = createClientMaterializationDispatcher();
  // Keep the URL inline so Vite detects and bundles the worker properly.
  const worker = (opts.workerUrl
    ? new Worker(opts.workerUrl, { type: 'module' })
    : new Worker(new URL('./worker.js', import.meta.url), {
        type: 'module',
      })) as unknown as WorkerProxy;
  let nextId = 1;
  const pending = new Map<
    number,
    { resolve: (value: any) => void; reject: (err: Error) => void }
  >();
  let terminalError: Error | null = null;
  let closed = false;
  let callQueue: Promise<void> = Promise.resolve();

  const closedError = new Error(CLIENT_CLOSED_ERROR);
  const settleQueue = <T>(promise: Promise<T>): Promise<void> =>
    promise.then(
      () => undefined,
      () => undefined,
    );

  const callRaw = <M extends RpcMethod>(method: M, params: RpcParams<M>): Promise<RpcResult<M>> => {
    if (closed) return Promise.reject(closedError);
    const id = nextId++;
    if (terminalError) return Promise.reject(terminalError);
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      worker.postMessage({ id, method, params } satisfies RpcRequest<M>);
    });
  };
  const call = <M extends RpcMethod>(method: M, params: RpcParams<M>): Promise<RpcResult<M>> => {
    const run = callQueue.then(() => callRaw(method, params));
    callQueue = settleQueue(run);
    return run;
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
  ])) as { storage?: StorageMode; filename?: string; opfsError?: string } | undefined;
  const effectiveStorage: StorageMode = initResult?.storage === 'opfs' ? 'opfs' : 'memory';
  const effectiveFilename =
    initResult?.filename ??
    (effectiveStorage === 'opfs' ? (opts.filename ?? '/treecrdt.db') : ':memory:');
  if (effectiveStorage === 'opfs') {
    materialized.enableCrossTab({ docId: opts.docId, filename: effectiveFilename });
  }
  const cleanup = () => {
    closed = true;
    materialized.close();
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
    runtime: 'dedicated-worker',
    storage: effectiveStorage,
    docId: opts.docId,
    call,
    materialized,
    close: closeImpl,
    drop: dropImpl,
  });
}

// --- Shared worker client (one SQLite backend shared by same-origin tabs)

async function createSharedWorkerClient(opts: {
  baseUrl: string;
  filename?: string;
  storage: StorageMode;
  docId: string;
  name: string;
  requireOpfs?: boolean;
  workerUrl?: string | URL;
}): Promise<TreecrdtClient> {
  const sharedWorker = opts.workerUrl
    ? new SharedWorker(opts.workerUrl, { name: opts.name, type: 'module' } as WorkerOptions & {
        name: string;
      })
    : await createDefaultSharedWorker(opts.name);
  const port = sharedWorker.port as unknown as MessagePortProxy;
  let nextId = 1;
  const pending = new Map<
    number,
    { resolve: (value: any) => void; reject: (err: Error) => void }
  >();
  let terminalError: Error | null = null;
  let closed = false;
  let callQueue: Promise<void> = Promise.resolve();

  const closedError = new Error(CLIENT_CLOSED_ERROR);
  const settleQueue = <T>(promise: Promise<T>): Promise<void> =>
    promise.then(
      () => undefined,
      () => undefined,
    );

  const callRaw = <M extends RpcMethod>(method: M, params: RpcParams<M>): Promise<RpcResult<M>> => {
    if (closed) return Promise.reject(closedError);
    const id = nextId++;
    if (terminalError) return Promise.reject(terminalError);
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      port.postMessage({ id, method, params } satisfies RpcRequest<M>);
    });
  };
  const call = <M extends RpcMethod>(method: M, params: RpcParams<M>): Promise<RpcResult<M>> => {
    const run = callQueue.then(() => callRaw(method, params));
    callQueue = settleQueue(run);
    return run;
  };
  const materialized = createClientMaterializationDispatcher({
    broadcast: (event) => {
      void call('broadcastMaterialized', [event]).catch(() => {
        // Closing tabs can race a final materialization notification.
      });
    },
  });

  const onMessage = (ev: MessageEvent<RpcResponse | RpcPushMessage>) => {
    const data = ev.data;
    if ('type' in data && data.type === 'materialized') {
      materialized.emitIncomingEvent(data.event);
      return;
    }
    const response = data as RpcResponse;
    const handler = pending.get(response.id as number);
    if (!handler) return;
    pending.delete(response.id as number);
    if (response.ok) handler.resolve(response.result);
    else handler.reject(new Error(response.error || 'shared worker error'));
  };
  const onMessageError = () => {
    const err = new Error('shared worker message error');
    terminalError = err;
    for (const { reject } of pending.values()) reject(err);
    pending.clear();
  };
  port.addEventListener('message', onMessage);
  port.addEventListener('messageerror', onMessageError);
  port.start();

  const initResult = (await call('init', [
    opts.baseUrl,
    opts.filename,
    opts.storage,
    opts.docId,
  ])) as { storage?: StorageMode; filename?: string; opfsError?: string } | undefined;
  const effectiveStorage: StorageMode = initResult?.storage === 'opfs' ? 'opfs' : 'memory';
  const cleanup = () => {
    closed = true;
    materialized.close();
    for (const { reject } of pending.values()) reject(closedError);
    pending.clear();
    port.removeEventListener('message', onMessage);
    port.removeEventListener('messageerror', onMessageError);
    port.close();
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
      // noop
    }
  };

  const dropImpl = async () => {
    if (closed) return;
    try {
      if (!terminalError) await call('drop', [] as RpcParams<'drop'>);
      cleanup();
    } finally {
      // noop
    }
  };

  return makeTreecrdtClientFromCall({
    mode: 'worker',
    runtime: 'shared-worker',
    storage: effectiveStorage,
    docId: opts.docId,
    call,
    materialized,
    close: closeImpl,
    drop: dropImpl,
  });
}

async function createDefaultSharedWorker(name: string): Promise<SharedWorker> {
  return new SharedWorker(new URL('./shared-worker.js', import.meta.url), {
    name,
    type: 'module',
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
  const materialized = createClientMaterializationDispatcher();
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
  if (finalStorage === 'opfs') {
    materialized.enableCrossTab({ docId: opts.docId, filename });
  }
  const adapter = opened.api;
  const runner: SqliteRunner = {
    exec: (sql) => db.exec(sql),
    getText: (sql, params = []) => dbGetText(db, sql, params),
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
  let callQueue: Promise<void> = Promise.resolve();
  const settleQueue = <T>(promise: Promise<T>): Promise<void> =>
    promise.then(
      () => undefined,
      () => undefined,
    );

  const runDirectCall: RpcCall = async (method, params) => {
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
  const call: RpcCall = (method, params) => {
    const run = callQueue.then(() => runDirectCall(method, params));
    callQueue = settleQueue(run);
    return run;
  };

  return makeTreecrdtClientFromCall({
    mode: 'direct',
    runtime: 'direct',
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
  runtime: RuntimeMode;
  storage: StorageMode;
  docId: string;
  call: RpcCall;
  materialized: ClientMaterializationDispatcher;
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

  const closeImpl = async () => {
    if (closePromise) return await closePromise;
    closePromise = (async () => {
      try {
        await opts.close();
      } catch {
        // Client teardown is best-effort. Fast refresh and overlapping resets can race a prior
        // close, and the underlying sqlite handle may already be gone by the time this runs.
      } finally {
        materialized.close();
      }
    })();
    await closePromise;
  };
  let dropPromise: Promise<void> | null = null;
  const dropImpl = async () => {
    if (dropPromise) return await dropPromise;
    dropPromise = (async () => {
      try {
        await opts.drop();
      } finally {
        materialized.close();
      }
    })();
    await dropPromise;
  };

  return {
    mode: opts.mode,
    runtime: opts.runtime,
    storage: opts.storage,
    docId: opts.docId,
    runner,
    ops: {
      append: async (op, writeOpts?: WriteOptions) => {
        const outcome = await call('append', [op]);
        materialized.emitOutcome(outcome, writeOpts?.writeId);
      },
      appendMany: async (ops, writeOpts?: WriteOptions) => {
        const outcome = await call('appendMany', [ops]);
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
    auth: createLazyAuthApi({ runner, docId: opts.docId }),
    local,
    onMaterialized: materialized.onMaterialized,
    close: closeImpl,
    drop: dropImpl,
  };
}

function encodeReplica(replica: Operation['meta']['id']['replica']): Uint8Array {
  return replicaIdToBytes(replica);
}

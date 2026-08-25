import * as Comlink from 'comlink';
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
import { bytesToHex, nodeIdFromBytes16, replicaIdToBytes } from '@treecrdt/interface/ids';
import type {
  LocalWriteOptions,
  MaterializationOutcome,
  WriteOptions,
} from '@treecrdt/interface/engine';
import { createTreecrdtEngineLocal } from '@treecrdt/interface/engine';
import type { MaterializationListener } from './session.js';
import type { ResolvedClientOptions, RuntimeConnection } from './runtime/types.js';
import { defaultSharedWorkerName, resolveBrowserEnvironment } from './runtime/resolve.js';
import { directRuntimeStrategy, type OpenDbFn } from './runtime/direct.js';
import { dedicatedWorkerStrategy } from './runtime/dedicated-worker.js';
import { sharedWorkerStrategy } from './runtime/shared-worker.js';
import { createClientMaterializationDispatcher } from './materialization.js';
import type { ClientOptions, TreecrdtClient } from './types.js';

export type { OpenDbFn } from './runtime/direct.js';

export const CLIENT_CLOSED_ERROR = 'TreecrdtClient was closed';

// Keep long browser appendMany calls from monopolizing the worker queue.
const APPEND_MANY_CHUNK_SIZE = 2500;

/**
 * Browser client factory. Callers must pass a platform opener so this shared module
 * never imports the Vite `?url` WASM loader (which would break the Node entry).
 */
export async function createBrowserTreecrdtClient(
  opts: ClientOptions = {},
  openDb: OpenDbFn,
): Promise<TreecrdtClient> {
  const env = resolveBrowserEnvironment(opts);
  const resolved: ResolvedClientOptions = {
    baseUrl: env.baseUrl,
    filename: env.storage.filename,
    storage: env.shouldUseOpfs ? 'opfs' : 'memory',
    fallback: env.storage.fallback,
    requireOpfs: env.storage.requireOpfs,
    docId: env.docId,
    openDb,
    workerUrl:
      env.runtime.type === 'dedicated-worker' || env.runtime.type === 'shared-worker'
        ? env.runtime.workerUrl
        : undefined,
    sharedWorkerName:
      env.runtime.type === 'shared-worker'
        ? (env.runtime.name ??
          defaultSharedWorkerName(env.docId, env.shouldUseOpfs ? env.storage.filename : ':memory:'))
        : defaultSharedWorkerName(env.docId, env.shouldUseOpfs ? env.storage.filename : ':memory:'),
  };

  const strategy =
    env.resolvedRuntime === 'shared-worker'
      ? sharedWorkerStrategy
      : env.resolvedRuntime === 'dedicated-worker'
        ? dedicatedWorkerStrategy
        : directRuntimeStrategy;

  return createClientFromBackend(await strategy.connect(resolved));
}

/** Direct in-process client with an injected opener (Node entry + unit tests). */
export async function buildDirectClient(
  opts: {
    baseUrl?: string;
    filename?: string;
    storage: 'memory' | 'opfs';
    docId: string;
    requireOpfs?: boolean;
    fallback?: 'memory' | 'throw';
  },
  openDb: OpenDbFn,
): Promise<TreecrdtClient> {
  return createClientFromBackend(
    await directRuntimeStrategy.connect({
      baseUrl: opts.baseUrl,
      filename: opts.filename,
      storage: opts.storage,
      fallback: opts.fallback ?? (opts.requireOpfs ? 'throw' : 'memory'),
      requireOpfs: opts.requireOpfs ?? false,
      docId: opts.docId,
      openDb,
    }),
  );
}

/** Builds the public TreecrdtClient façade over a connected session (local or Comlink). */
export async function createClientFromBackend(
  runtime: RuntimeConnection,
): Promise<TreecrdtClient> {
  const { connection, mode, runtime: runtimeMode, storage, filename, docId, local, dispose } =
    runtime;
  const session = connection.session;

  const materialized = createClientMaterializationDispatcher({
    broadcast: (event) => {
      void Promise.resolve(connection.notifyMaterialized(event)).catch(() => {
        // Closing tabs can race a final materialization notification.
      });
    },
  });

  if (storage === 'opfs' && runtimeMode !== 'shared-worker') {
    materialized.enableCrossTab({ docId, filename });
  }

  const materializationListener: MaterializationListener = (event) => {
    materialized.emitIncomingEvent(event);
  };
  const subscribedListener = local
    ? materializationListener
    : Comlink.proxy(materializationListener);
  await Promise.resolve(connection.subscribeMaterialized(subscribedListener));

  let closePromise: Promise<void> | null = null;
  let dropPromise: Promise<void> | null = null;
  let closed = false;
  const closedError = new Error(CLIENT_CLOSED_ERROR);

  const guard = async <T>(work: () => Promise<T>): Promise<T> => {
    if (closed) throw closedError;
    return work();
  };

  const runner: SqliteRunner = {
    exec: (sql) => guard(() => session.sqlExec(sql)),
    getText: (sql, params = []) => guard(() => session.sqlGetText(sql, params as any)),
  };

  const localWriters = new Map<string, TreecrdtSqliteWriter>();
  const localWriterFor = (replica: ReplicaId) => {
    const key = bytesToHex(replica);
    const existing = localWriters.get(key);
    if (existing) return existing;
    const next = createTreecrdtSqliteWriter(runner, {
      replica,
      onMaterialized: materialized.emitEvent,
    });
    localWriters.set(key, next);
    return next;
  };

  const appendMany = async (operations: Operation[], writeOpts?: WriteOptions) => {
    if (operations.length <= APPEND_MANY_CHUNK_SIZE) {
      const outcome = await guard(() => session.appendMany(operations));
      materialized.emitOutcome(outcome, writeOpts?.writeId);
      return;
    }

    const outcomes: MaterializationOutcome[] = [];
    for (let start = 0; start < operations.length; start += APPEND_MANY_CHUNK_SIZE) {
      outcomes.push(
        await guard(() =>
          session.appendMany(operations.slice(start, start + APPEND_MANY_CHUNK_SIZE)),
        ),
      );
    }
    materialized.emitOutcome(mergeMaterializationOutcomes(outcomes), writeOpts?.writeId);
  };

  const localEngine = createTreecrdtEngineLocal({
    insert: (
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
      }),
    move: (
      replica: ReplicaId,
      node: string,
      newParent: string,
      placement: TreecrdtSqlitePlacement,
      writeOpts?: LocalWriteOptions,
    ) => localWriterFor(replica).move(node, newParent, placement, writeOpts),
    delete: (replica: ReplicaId, node: string, writeOpts?: LocalWriteOptions) =>
      localWriterFor(replica).delete(node, writeOpts),
    payload: (
      replica: ReplicaId,
      node: string,
      payload: Uint8Array | null,
      writeOpts?: LocalWriteOptions,
    ) => localWriterFor(replica).payload(node, payload, writeOpts),
  });

  const releaseSubscription = async () => {
    try {
      await Promise.resolve(connection.unsubscribeMaterialized(subscribedListener));
    } catch {
      // Closing remotes can already have released the proxy.
    }
  };

  return {
    mode,
    runtime: runtimeMode,
    storage,
    docId,
    runner,
    ops: {
      append: async (op, writeOpts?: WriteOptions) => {
        const outcome = await guard(() => session.append(op));
        materialized.emitOutcome(outcome, writeOpts?.writeId);
      },
      appendMany,
      all: () => guard(async () => decodeSqliteOps(await session.opsSince(0))),
      since: (lamport, root?) =>
        guard(async () => decodeSqliteOps(await session.opsSince(lamport, root))),
      children: (parent) =>
        guard(async () => {
          const opRefs = decodeSqliteOpRefs(await session.opRefsChildren(parent));
          return decodeSqliteOps(await session.opsByOpRefs(opRefs));
        }),
      get: (opRefs) => guard(async () => decodeSqliteOps(await session.opsByOpRefs(opRefs))),
    },
    opRefs: {
      all: () => guard(async () => decodeSqliteOpRefs(await session.opRefsAll())),
      children: (parent) =>
        guard(async () => decodeSqliteOpRefs(await session.opRefsChildren(parent))),
    },
    tree: {
      children: (parent) =>
        guard(async () => decodeSqliteNodeIds(await session.treeChildren(parent))),
      childrenPage: (
        parent: string,
        cursor: { orderKey: Uint8Array; node: Uint8Array } | null,
        limit: number,
      ): Promise<SqliteTreeChildRow[]> =>
        guard(async () =>
          decodeSqliteTreeChildRows(await session.treeChildrenPage(parent, cursor, limit)),
        ),
      dump: () => guard(async () => decodeSqliteTreeRows(await session.treeDump())),
      nodeCount: () => guard(async () => Number(await session.treeNodeCount())),
      parent: async (node) => {
        const result = await guard(() => session.treeParent(node));
        if (result === null) return null;
        return nodeIdFromBytes16(toBytes(result));
      },
      exists: (node) => guard(async () => Boolean(await session.treeExists(node))),
      getPayload: async (node) => {
        const result = await guard(() => session.treePayload(node));
        return result === null ? null : toBytes(result);
      },
    },
    meta: {
      headLamport: () => guard(async () => Number(await session.headLamport())),
      replicaMaxCounter: (replica) =>
        guard(async () => Number(await session.replicaMaxCounter(replicaIdToBytes(replica)))),
    },
    local: localEngine,
    onMaterialized: materialized.onMaterialized,
    close: async () => {
      if (closePromise) return await closePromise;
      if (dropPromise) {
        closePromise = dropPromise.then(
          () => undefined,
          () => undefined,
        );
        return await closePromise;
      }
      closePromise = (async () => {
        closed = true;
        try {
          await connection.close();
        } catch {
          // Client teardown is best-effort. Fast refresh and overlapping resets can race a prior
          // close, and the underlying sqlite handle may already be gone by the time this runs.
        } finally {
          await releaseSubscription();
          materialized.close();
          await dispose();
        }
      })();
      await closePromise;
    },
    drop: async () => {
      if (dropPromise) return await dropPromise;
      if (closePromise) {
        dropPromise = closePromise.then(() => undefined);
        return await dropPromise;
      }
      dropPromise = (async () => {
        closed = true;
        try {
          await connection.drop();
        } finally {
          await releaseSubscription();
          materialized.close();
          await dispose();
        }
      })();
      await dropPromise;
    },
  };
}

function toBytes(bytes: Uint8Array | number[]): Uint8Array {
  return bytes instanceof Uint8Array ? bytes : Uint8Array.from(bytes);
}

function mergeMaterializationOutcomes(outcomes: MaterializationOutcome[]): MaterializationOutcome {
  const last = outcomes[outcomes.length - 1];
  return {
    headSeq: last?.headSeq ?? 0,
    changes: outcomes.flatMap((outcome) => outcome.changes),
  };
}

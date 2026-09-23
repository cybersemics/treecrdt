import * as Comlink from 'comlink';
import type { Operation, ReplicaId } from '@treecrdt/interface';
import {
  createTreecrdtSqliteWriter,
  decodeSqliteNodeIds,
  decodeSqliteOpRefs,
  decodeSqliteOps,
  decodeSqliteTreeRows,
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
import { createTreecrdtEngineLocal, createTreecrdtTreeNodes } from '@treecrdt/interface/engine';
import type { MaterializationListener } from './session.js';
import type { RuntimeConnection } from './runtime/types.js';
import { resolveBrowserEnvironment } from './runtime/resolve.js';
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
  opts: ClientOptions,
  openDb: OpenDbFn,
): Promise<TreecrdtClient> {
  const env = resolveBrowserEnvironment(opts);
  const strategy =
    env.runtime === 'shared-worker'
      ? sharedWorkerStrategy
      : env.runtime === 'dedicated-worker'
        ? dedicatedWorkerStrategy
        : directRuntimeStrategy;
  return createClientFromBackend(
    await strategy.connect({
      baseUrl: env.baseUrl,
      filename: env.filename,
      storage: env.storage,
      docId: env.docId,
      openDb,
    }),
  );
}

/** Direct in-process client with an injected opener (Node entry + unit tests). */
export async function buildDirectClient(
  opts: { baseUrl?: string; filename?: string; storage: 'memory' | 'opfs'; docId: string },
  openDb: OpenDbFn,
): Promise<TreecrdtClient> {
  return createClientFromBackend(await directRuntimeStrategy.connect({ ...opts, openDb }));
}

/** Builds the public TreecrdtClient façade over a connected session (local or Comlink). */
export async function createClientFromBackend(runtime: RuntimeConnection): Promise<TreecrdtClient> {
  const {
    connection,
    mode,
    runtime: runtimeMode,
    storage,
    filename,
    docId,
    local,
    dispose,
  } = runtime;
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
    if (runtimeMode === 'shared-worker') materialized.emitIncomingEvent(event);
    else materialized.emitEvent(event);
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
      const outcome = await guard(() => session.operations.appendMany(operations));
      materialized.emitOutcome(outcome, writeOpts?.writeId);
      return;
    }

    const outcomes: MaterializationOutcome[] = [];
    for (let start = 0; start < operations.length; start += APPEND_MANY_CHUNK_SIZE) {
      outcomes.push(
        await guard(() =>
          session.operations.appendMany(operations.slice(start, start + APPEND_MANY_CHUNK_SIZE)),
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
        const outcome = await guard(() => session.operations.append(op));
        materialized.emitOutcome(outcome, writeOpts?.writeId);
      },
      appendMany,
      all: () => guard(async () => decodeSqliteOps(await session.operations.since(0))),
      since: (lamport, root?) =>
        guard(async () => decodeSqliteOps(await session.operations.since(lamport, root))),
      children: (parent) =>
        guard(async () => {
          const opRefs = decodeSqliteOpRefs(await session.operations.refsChildren(parent));
          return decodeSqliteOps(await session.operations.byRefs(opRefs));
        }),
      get: (opRefs) => guard(async () => decodeSqliteOps(await session.operations.byRefs(opRefs))),
    },
    opRefs: {
      all: () => guard(async () => decodeSqliteOpRefs(await session.operations.refsAll())),
      children: (parent) =>
        guard(async () => decodeSqliteOpRefs(await session.operations.refsChildren(parent))),
    },
    tree: {
      ...createTreecrdtTreeNodes({
        exists: (node) => guard(async () => Boolean(await session.tree.exists(node))),
        parent: async (node) => {
          const result = await guard(() => session.tree.parent(node));
          if (result === null) return null;
          return nodeIdFromBytes16(toBytes(result));
        },
        payload: async (node) => {
          const result = await guard(() => session.tree.payload(node));
          return result === null ? null : toBytes(result);
        },
        children: (parent) =>
          guard(async () => decodeSqliteNodeIds(await session.tree.children(parent))),
      }),
      dump: () => guard(async () => decodeSqliteTreeRows(await session.tree.dump())),
      nodeCount: () => guard(async () => Number(await session.tree.nodeCount())),
    },
    meta: {
      headLamport: () => guard(async () => Number(await session.operations.headLamport())),
      replicaMaxCounter: (replica) =>
        guard(async () =>
          Number(await session.operations.replicaMaxCounter(replicaIdToBytes(replica))),
        ),
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

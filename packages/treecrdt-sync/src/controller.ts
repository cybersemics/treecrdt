import type { Operation } from '@treecrdt/interface';
import type { Filter, SyncOnceOptions, SyncSubscribeOptions } from '@treecrdt/sync-protocol';
import { connectTreecrdtWebSocketSync } from './connect.js';
import type {
  ConnectTreecrdtWebSocketSyncOptions,
  TreecrdtWebSocketSync,
  TreecrdtWebSocketSyncClient,
} from './types.js';

export type TreecrdtSyncControllerState =
  | 'idle'
  | 'starting'
  | 'live'
  | 'stopped'
  | 'error'
  | 'closed';

export type TreecrdtSyncControllerStatus = {
  state: TreecrdtSyncControllerState;
  pendingOps: number;
  error?: unknown;
};

export type TreecrdtSyncControllerOptions = {
  /**
   * Initial reconciliation to run before the controller is considered live-ready.
   * Pass `false` to skip initial reconciliation.
   */
  initialSync?: false | { filter?: Filter; opts?: SyncOnceOptions };
  /**
   * Live subscription options. Pass `false` for explicit push/reconcile only.
   */
  live?: false | SyncSubscribeOptions;
  /**
   * Optional safety-net reconciliation while the controller is running.
   */
  reconcileIntervalMs?: number;
  onStatus?: (status: TreecrdtSyncControllerStatus) => void;
  onError?: (error: unknown) => void;
};

export type ConnectTreecrdtSyncControllerOptions = ConnectTreecrdtWebSocketSyncOptions & {
  controller?: TreecrdtSyncControllerOptions;
};

export type TreecrdtSyncController = {
  readonly status: TreecrdtSyncControllerStatus;
  readonly pendingOpCount: number;
  start: () => Promise<void>;
  stopLive: () => void;
  pushLocalOps: (ops?: readonly Operation[]) => Promise<void>;
  flushPendingOps: () => Promise<void>;
  syncOnce: (filter?: Filter, opts?: SyncOnceOptions) => Promise<void>;
  onChange: TreecrdtWebSocketSync['onChange'];
  close: () => Promise<void>;
};

function statusSnapshot(
  state: TreecrdtSyncControllerState,
  pendingOps: number,
  error?: unknown,
): TreecrdtSyncControllerStatus {
  return error === undefined ? { state, pendingOps } : { state, pendingOps, error };
}

/**
 * Wrap a low-level sync handle with app-facing lifecycle semantics.
 *
 * `pushLocalOps` is safe before `start()`: ops are queued and flushed during startup without
 * relying on app code to remember whether the transport is ready yet. Failed flushes keep ops
 * queued for an explicit retry or the next successful start.
 */
export function createTreecrdtSyncController(
  sync: TreecrdtWebSocketSync,
  options: TreecrdtSyncControllerOptions = {},
): TreecrdtSyncController {
  let state: TreecrdtSyncControllerState = 'idle';
  let lastError: unknown;
  let readyToFlush = false;
  let startPromise: Promise<void> | null = null;
  let flushPromise: Promise<void> | null = null;
  let closed = false;
  let reconcileTimer: ReturnType<typeof setInterval> | null = null;
  const pendingOps: Operation[] = [];

  const emitStatus = () => {
    options.onStatus?.(statusSnapshot(state, pendingOps.length, lastError));
  };

  const setState = (next: TreecrdtSyncControllerState, error?: unknown) => {
    state = next;
    if (error !== undefined) lastError = error;
    else if (next !== 'error') lastError = undefined;
    emitStatus();
  };

  const reportError = (error: unknown) => {
    lastError = error;
    try {
      options.onError?.(error);
    } finally {
      setState('error', error);
    }
  };

  const assertOpen = () => {
    if (closed) throw new Error('TreecrdtSyncController: closed');
  };

  const clearReconcileTimer = () => {
    if (reconcileTimer !== null) clearInterval(reconcileTimer);
    reconcileTimer = null;
  };

  const runPeriodicReconcile = async () => {
    if (closed || !readyToFlush) return;
    try {
      const initialSync = options.initialSync;
      await sync.syncOnce(
        initialSync ? initialSync.filter : { all: {} },
        initialSync ? initialSync.opts : undefined,
      );
      await controller.flushPendingOps();
    } catch (error) {
      reportError(error);
    }
  };

  const scheduleReconcile = () => {
    clearReconcileTimer();
    const intervalMs = options.reconcileIntervalMs;
    if (intervalMs === undefined) return;
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
      throw new Error(`invalid reconcileIntervalMs: ${intervalMs}`);
    }
    reconcileTimer = setInterval(() => {
      void runPeriodicReconcile();
    }, intervalMs);
  };

  const flushPendingOps = async () => {
    assertOpen();
    if (!readyToFlush || pendingOps.length === 0) return;
    if (flushPromise) return await flushPromise;

    flushPromise = (async () => {
      while (readyToFlush && pendingOps.length > 0) {
        const batch = pendingOps.slice();
        try {
          await sync.pushLocalOps(batch);
          pendingOps.splice(0, batch.length);
          if (state === 'error') setState('live');
          else emitStatus();
        } catch (error) {
          reportError(error);
          throw error;
        }
      }
    })().finally(() => {
      flushPromise = null;
    });

    await flushPromise;
  };

  const start = async () => {
    assertOpen();
    if (readyToFlush) return;
    if (startPromise) return await startPromise;

    startPromise = (async () => {
      setState('starting');
      try {
        readyToFlush = true;
        await flushPendingOps();
        readyToFlush = false;

        const initialSync = options.initialSync;
        if (initialSync !== false) {
          await sync.syncOnce(initialSync?.filter ?? { all: {} }, initialSync?.opts);
        }
        if (options.live !== false) {
          await sync.startLive(options.live ?? {});
        }
        readyToFlush = true;
        setState('live');
        scheduleReconcile();
        await flushPendingOps();
      } catch (error) {
        readyToFlush = false;
        clearReconcileTimer();
        reportError(error);
        throw error;
      } finally {
        startPromise = null;
      }
    })();

    await startPromise;
  };

  const stopLive = () => {
    if (closed) return;
    readyToFlush = false;
    clearReconcileTimer();
    sync.stopLive();
    setState('stopped');
  };

  const pushLocalOps = async (ops: readonly Operation[] = []) => {
    assertOpen();
    if (ops.length === 0) return;
    pendingOps.push(...ops);
    emitStatus();
    if (readyToFlush) await flushPendingOps();
  };

  const syncOnce = async (filter?: Filter, opts?: SyncOnceOptions) => {
    assertOpen();
    await sync.syncOnce(filter, opts);
    if (readyToFlush) await flushPendingOps();
  };

  const close = async () => {
    if (closed) return;
    closed = true;
    readyToFlush = false;
    clearReconcileTimer();
    try {
      sync.stopLive();
    } catch {
      // ignore
    }
    try {
      await sync.close();
    } finally {
      setState('closed');
    }
  };

  const controller: TreecrdtSyncController = {
    get status() {
      return statusSnapshot(state, pendingOps.length, lastError);
    },
    get pendingOpCount() {
      return pendingOps.length;
    },
    start,
    stopLive,
    pushLocalOps,
    flushPendingOps,
    syncOnce,
    onChange: sync.onChange,
    close,
  };

  emitStatus();
  return controller;
}

export async function connectTreecrdtSyncController(
  client: TreecrdtWebSocketSyncClient,
  options: ConnectTreecrdtSyncControllerOptions,
): Promise<TreecrdtSyncController> {
  const { controller: controllerOptions, ...connectOptions } = options;
  const sync = await connectTreecrdtWebSocketSync(client, connectOptions);
  return createTreecrdtSyncController(sync, controllerOptions);
}

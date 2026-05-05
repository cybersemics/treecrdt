import type { Operation } from '@treecrdt/interface';
import type {
  Filter,
  SyncMessage,
  SyncOnceOptions,
  SyncPeer,
  SyncPushOptions,
  SyncSubscribeOptions,
} from '@treecrdt/sync-protocol';
import type { DuplexTransport } from '@treecrdt/sync-protocol/transport';
import { connectTreecrdtWebSocketSync } from './connect.js';
import type {
  ConnectTreecrdtWebSocketSyncOptions,
  TreecrdtWebSocketSync,
  TreecrdtWebSocketSyncClient,
} from './types.js';

export type SyncControllerState = 'idle' | 'starting' | 'live' | 'stopped' | 'error' | 'closed';

export type SyncControllerStatus = {
  state: SyncControllerState;
  pendingOps: number;
  error?: unknown;
};

export type SyncControllerOptions = {
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
  onStatus?: (status: SyncControllerStatus) => void;
  onError?: (error: unknown) => void;
};

export type ConnectSyncControllerOptions = ConnectTreecrdtWebSocketSyncOptions & {
  controller?: SyncControllerOptions;
};

export type OutboundSyncStatus = {
  peerCount: number;
  pendingOps: number;
  needsFullSync: boolean;
  running: boolean;
  scheduled: boolean;
};

export type OutboundSyncRunPushContext<Op = Operation> = {
  localPeer: SyncPeer<Op>;
  peerId: string;
  transport: DuplexTransport<SyncMessage<Op>>;
  ops: readonly Op[];
};

export type OutboundSyncRunSyncContext<Op = Operation> = {
  localPeer: SyncPeer<Op>;
  peerId: string;
  transport: DuplexTransport<SyncMessage<Op>>;
  filter: Filter;
};

export type OutboundSyncOptions<Op = Operation> = {
  localPeer: SyncPeer<Op>;
  /**
   * Stable key used to coalesce repeated local write hints before upload.
   */
  opKey?: (op: Op) => string;
  /**
   * Allows apps to keep queued work while offline instead of turning transient offline state into
   * sync errors.
   */
  isOnline?: () => boolean;
  /**
   * Select which attached transports should receive queued local writes. Useful when one SyncPeer
   * owns both local-tab mesh transports and a remote websocket transport.
   */
  shouldSyncPeer?: (peerId: string) => boolean;
  /**
   * Filters to reconcile when callers request a fallback sync without exact local ops.
   */
  getFallbackFilters?: () => readonly Filter[];
  /**
   * Override low-level push execution for app-specific timeouts, batching, or logging.
   */
  runPush?: (ctx: OutboundSyncRunPushContext<Op>) => Promise<void>;
  /**
   * Override fallback reconciliation for app-specific timeouts or syncOnce options.
   */
  runSync?: (ctx: OutboundSyncRunSyncContext<Op>) => Promise<void>;
  pushOptions?: (peerId: string) => SyncPushOptions | undefined;
  syncOptions?: (peerId: string, filter: Filter) => SyncOnceOptions | undefined;
  onWorkStart?: () => void;
  onWorkEnd?: () => void;
  onError?: (ctx: { peerId: string; error: unknown }) => void;
  onStatus?: (status: OutboundSyncStatus) => void;
};

export type OutboundSync<Op = Operation> = {
  readonly status: OutboundSyncStatus;
  readonly pendingOpCount: number;
  readonly peerCount: number;
  addPeer: (peerId: string, transport: DuplexTransport<SyncMessage<Op>>) => void;
  removePeer: (peerId: string) => void;
  clearPeers: () => void;
  queue: (ops?: readonly Op[]) => void;
  flush: () => Promise<void>;
  close: () => void;
};

export type SyncController = {
  readonly status: SyncControllerStatus;
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
  state: SyncControllerState,
  pendingOps: number,
  error?: unknown,
): SyncControllerStatus {
  return error === undefined ? { state, pendingOps } : { state, pendingOps, error };
}

function outboundSyncStatusSnapshot<Op>(
  peers: ReadonlyMap<string, DuplexTransport<SyncMessage<Op>>>,
  pendingOps: readonly Op[],
  needsFullSync: boolean,
  running: boolean,
  scheduled: boolean,
): OutboundSyncStatus {
  return {
    peerCount: peers.size,
    pendingOps: pendingOps.length,
    needsFullSync,
    running,
    scheduled,
  };
}

/**
 * Wrap a low-level sync handle with app-facing lifecycle semantics.
 *
 * `pushLocalOps` is safe before `start()`: ops are queued and flushed during startup without
 * relying on app code to remember whether the transport is ready yet. Failed flushes keep ops
 * queued for an explicit retry or the next successful start.
 */
function createSingleTransportSyncController(
  sync: TreecrdtWebSocketSync,
  options: SyncControllerOptions = {},
): SyncController {
  let state: SyncControllerState = 'idle';
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

  const setState = (next: SyncControllerState, error?: unknown) => {
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
    if (closed) throw new Error('SyncController: closed');
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

  const controller: SyncController = {
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

/**
 * Queue local writes for a single {@link SyncPeer} that is attached to multiple transports.
 *
 * Apps can use one low-level peer for local-tab mesh subscriptions and remote websocket upload at
 * the same time. This controller centralizes the remote upload/reconcile queue so UI code only
 * registers peer transports and reports local ops returned by the edit API.
 */
export function createOutboundSync<Op = Operation>(
  options: OutboundSyncOptions<Op>,
): OutboundSync<Op> {
  const peers = new Map<string, DuplexTransport<SyncMessage<Op>>>();
  const pendingOps: Op[] = [];
  const pendingOpKeys = new Set<string>();
  let needsFullSync = false;
  let running = false;
  let scheduled = false;
  let closed = false;

  const emitStatus = () => {
    options.onStatus?.(
      outboundSyncStatusSnapshot(peers, pendingOps, needsFullSync, running, scheduled),
    );
  };

  const addPendingOps = (ops: readonly Op[]) => {
    for (const op of ops) {
      const key = options.opKey?.(op);
      if (key !== undefined) {
        if (pendingOpKeys.has(key)) continue;
        pendingOpKeys.add(key);
      }
      pendingOps.push(op);
    }
  };

  const restorePendingOps = (ops: readonly Op[]) => {
    if (ops.length === 0) return;
    const existing = pendingOps.splice(0, pendingOps.length);
    pendingOpKeys.clear();
    addPendingOps(ops);
    addPendingOps(existing);
  };

  const takePendingOps = () => {
    const ops = pendingOps.splice(0, pendingOps.length);
    pendingOpKeys.clear();
    return ops;
  };

  const selectedPeers = () =>
    Array.from(peers.entries()).filter(([peerId]) => options.shouldSyncPeer?.(peerId) ?? true);

  const runPush =
    options.runPush ??
    ((ctx: OutboundSyncRunPushContext<Op>) =>
      ctx.localPeer.pushOps(ctx.transport, ctx.ops, options.pushOptions?.(ctx.peerId)));

  const runSync =
    options.runSync ??
    ((ctx: OutboundSyncRunSyncContext<Op>) =>
      ctx.localPeer.syncOnce(
        ctx.transport,
        ctx.filter,
        options.syncOptions?.(ctx.peerId, ctx.filter),
      ));

  const scheduleFlush = () => {
    if (closed) return;
    if (running) {
      scheduled = true;
      emitStatus();
      return;
    }
    if (scheduled) {
      emitStatus();
      return;
    }
    scheduled = true;
    emitStatus();
    queueMicrotask(() => {
      void controller.flush();
    });
  };

  const flush = async () => {
    if (closed) return;
    if (running) {
      scheduled = true;
      emitStatus();
      return;
    }
    if (!scheduled && (pendingOps.length > 0 || needsFullSync)) scheduled = true;
    if (!scheduled) {
      emitStatus();
      return;
    }

    running = true;
    options.onWorkStart?.();
    emitStatus();
    try {
      while (scheduled && !closed) {
        scheduled = false;
        if (options.isOnline && !options.isOnline()) {
          emitStatus();
          return;
        }

        const targets = selectedPeers();
        if (targets.length === 0) {
          emitStatus();
          return;
        }

        const ops = takePendingOps();
        const syncNeeded = needsFullSync;
        needsFullSync = false;
        if (!syncNeeded && ops.length === 0) {
          emitStatus();
          continue;
        }

        let failed = false;
        for (const [peerId, transport] of targets) {
          try {
            if (ops.length > 0) {
              await runPush({ localPeer: options.localPeer, peerId, transport, ops });
            } else {
              const filters = options.getFallbackFilters?.() ?? [{ all: {} }];
              for (const filter of filters) {
                await runSync({ localPeer: options.localPeer, peerId, transport, filter });
              }
            }
          } catch (error) {
            failed = true;
            options.onError?.({ peerId, error });
          }
        }

        if (failed) {
          restorePendingOps(ops);
          if (syncNeeded) needsFullSync = true;
          emitStatus();
          return;
        }

        emitStatus();
      }
    } finally {
      running = false;
      options.onWorkEnd?.();
      emitStatus();
    }
  };

  const controller: OutboundSync<Op> = {
    get status() {
      return outboundSyncStatusSnapshot(peers, pendingOps, needsFullSync, running, scheduled);
    },
    get pendingOpCount() {
      return pendingOps.length;
    },
    get peerCount() {
      return peers.size;
    },
    addPeer: (peerId, transport) => {
      if (closed) return;
      peers.set(peerId, transport);
      emitStatus();
      if (pendingOps.length > 0 || needsFullSync) scheduleFlush();
    },
    removePeer: (peerId) => {
      peers.delete(peerId);
      emitStatus();
    },
    clearPeers: () => {
      peers.clear();
      emitStatus();
    },
    queue: (ops = []) => {
      if (closed) return;
      if (ops.length > 0) addPendingOps(ops);
      else needsFullSync = true;
      scheduleFlush();
    },
    flush,
    close: () => {
      closed = true;
      scheduled = false;
      needsFullSync = false;
      pendingOps.splice(0, pendingOps.length);
      pendingOpKeys.clear();
      peers.clear();
      emitStatus();
    },
  };

  emitStatus();
  return controller;
}

/**
 * Create the app-facing sync controller.
 */
export function createSyncController(
  sync: TreecrdtWebSocketSync,
  options?: SyncControllerOptions,
): SyncController;
export function createSyncController(
  sync: TreecrdtWebSocketSync,
  options: SyncControllerOptions = {},
): SyncController {
  return createSingleTransportSyncController(sync, options);
}

export async function connectSyncController(
  client: TreecrdtWebSocketSyncClient,
  options: ConnectSyncControllerOptions,
): Promise<SyncController> {
  const { controller: controllerOptions, ...connectOptions } = options;
  const sync = await connectTreecrdtWebSocketSync(client, connectOptions);
  return createSyncController(sync, controllerOptions);
}

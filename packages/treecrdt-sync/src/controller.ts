import type { Operation } from '@treecrdt/interface';
import { bytesToHex } from '@treecrdt/interface/ids';
import type {
  OutboundSync,
  OutboundSyncFlushResult,
  OutboundSyncOptions,
  OutboundSyncPushTarget,
  OutboundSyncStatus,
} from './types.js';

function abortReason(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) return signal.reason;
  const error = new Error(
    signal.reason === undefined ? 'outbound push aborted' : String(signal.reason),
  );
  error.name = 'AbortError';
  return error;
}

async function pushWithTimeout<T>(
  run: (signal: AbortSignal) => Promise<T>,
  controller: AbortController,
  ms: number | undefined,
  parentSignal?: AbortSignal,
): Promise<T> {
  if (ms !== undefined && (!Number.isFinite(ms) || ms <= 0)) {
    throw new Error(`invalid pushTimeoutMs: ${ms}`);
  }

  const forwardAbort = () => controller.abort(parentSignal?.reason);
  if (parentSignal?.aborted) forwardAbort();
  else parentSignal?.addEventListener('abort', forwardAbort, { once: true });

  const timer =
    ms === undefined
      ? undefined
      : setTimeout(() => controller.abort(new Error(`outbound push timed out after ${ms}ms`)), ms);
  try {
    if (controller.signal.aborted) throw abortReason(controller.signal);
    return await run(controller.signal);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    parentSignal?.removeEventListener('abort', forwardAbort);
  }
}

function defaultOpKey(op: unknown): string | undefined {
  const id = (op as { meta?: { id?: { replica?: unknown; counter?: unknown } } })?.meta?.id;
  if (!(id?.replica instanceof Uint8Array)) return undefined;
  if (typeof id.counter !== 'number') return undefined;
  return `${bytesToHex(id.replica)}:${id.counter}`;
}

/**
 * Queue exact committed local operations for one replaceable remote destination.
 *
 * The target is a structural push function rather than a `SyncPeer`/transport pair, so the same
 * controller works above `TreecrdtWebSocketSync` and in lower-level integrations.
 */
export function createOutboundSync<Op = Operation>(
  options: OutboundSyncOptions<Op> = {},
): OutboundSync<Op> {
  const pendingOps: Op[] = [];
  const pendingOpKeys = new Set<string>();
  let targetRegistration: { target: OutboundSyncPushTarget<Op>; revision: number } | undefined;
  let targetRevision = 0;
  let flushing = false;
  let closed = false;
  let activePushController: AbortController | undefined;
  let activeFlush: Promise<OutboundSyncFlushResult> | undefined;
  let closeBarrier: Promise<void> | undefined;
  let autoFlushQueued = false;
  let autoFlushRequested = false;
  let autoFlushTargetRevision: number | undefined;
  let autoFlushWaitingFor: Promise<OutboundSyncFlushResult> | undefined;

  const statusSnapshot = (): OutboundSyncStatus => ({
    hasTarget: targetRegistration !== undefined,
    pendingOps: pendingOps.length,
    flushing,
    closed,
  });

  const emitStatus = () => {
    try {
      options.onStatus?.(statusSnapshot());
    } catch {
      // Observers must not interrupt queue bookkeeping.
    }
  };

  const reportError = (error: unknown) => {
    try {
      options.onError?.(error);
    } catch {
      // Observers must not turn a handled push failure into an unhandled rejection.
    }
  };

  const addPendingOps = (ops: readonly Op[]) => {
    if (closed) return;
    for (const op of ops) {
      const key = options.opKey?.(op) ?? defaultOpKey(op);
      if (closed) return;
      if (key !== undefined) {
        if (pendingOpKeys.has(key)) continue;
        pendingOpKeys.add(key);
      }
      pendingOps.push(op);
    }
  };

  const restorePendingOps = (ops: readonly Op[]) => {
    if (ops.length === 0 || closed) return;
    const newerOps = pendingOps.splice(0, pendingOps.length);
    pendingOpKeys.clear();
    addPendingOps(ops);
    addPendingOps(newerOps);
  };

  const takePendingOps = () => {
    const ops = pendingOps.splice(0, pendingOps.length);
    pendingOpKeys.clear();
    return ops;
  };

  const queueAutoFlush = (targetChangedRevision?: number) => {
    if (closed) return;
    autoFlushRequested = true;
    if (targetChangedRevision !== undefined) {
      autoFlushTargetRevision = targetChangedRevision;
    }
    if (autoFlushQueued || autoFlushWaitingFor) return;
    autoFlushQueued = true;
    queueMicrotask(() => {
      autoFlushQueued = false;
      if (closed || !autoFlushRequested) return;
      if (activeFlush) {
        const observedFlush = activeFlush;
        autoFlushWaitingFor = observedFlush;
        void observedFlush.then(
          (result) => {
            if (autoFlushWaitingFor !== observedFlush) return;
            autoFlushWaitingFor = undefined;
            const shouldRetry =
              !closed &&
              autoFlushRequested &&
              (result.status === 'drained' || autoFlushTargetRevision !== undefined);
            autoFlushRequested = false;
            autoFlushTargetRevision = undefined;
            if (shouldRetry) queueAutoFlush();
          },
          () => {
            if (autoFlushWaitingFor !== observedFlush) return;
            autoFlushWaitingFor = undefined;
            autoFlushRequested = false;
            autoFlushTargetRevision = undefined;
          },
        );
        return;
      }
      autoFlushRequested = false;
      autoFlushTargetRevision = undefined;
      if (pendingOps.length === 0) return;
      void controller.flush();
    });
  };

  const runFlush = async (): Promise<OutboundSyncFlushResult> => {
    if (closed) return { status: 'closed' };

    flushing = true;
    emitStatus();
    try {
      while (!closed) {
        if (pendingOps.length === 0) return { status: 'drained' };

        if (options.isOnline) {
          try {
            if (!options.isOnline()) {
              return { status: 'deferred', reason: 'offline', pendingOps: pendingOps.length };
            }
          } catch (error) {
            reportError(error);
            if (closed) return { status: 'closed' };
            return { status: 'failed', error, pendingOps: pendingOps.length };
          }
        }

        const registration = targetRegistration;
        if (!registration) {
          return { status: 'deferred', reason: 'no-target', pendingOps: pendingOps.length };
        }
        if (autoFlushTargetRevision === registration.revision) {
          autoFlushTargetRevision = undefined;
        }

        const ops = takePendingOps();
        const pushController = new AbortController();
        activePushController = pushController;
        try {
          await pushWithTimeout(
            (signal) =>
              registration.target(ops, {
                ...options.pushOptions,
                signal,
              }),
            pushController,
            options.pushTimeoutMs,
            options.pushOptions?.signal,
          );
        } catch (error) {
          if (closed) return { status: 'closed' };

          if (targetRegistration !== registration || targetRevision !== registration.revision) {
            restorePendingOps(ops);
            emitStatus();
            continue;
          }

          restorePendingOps(ops);
          reportError(error);
          if (closed) return { status: 'closed' };
          if (targetRegistration !== registration || targetRevision !== registration.revision) {
            emitStatus();
            continue;
          }
          emitStatus();
          return { status: 'failed', error, pendingOps: pendingOps.length };
        } finally {
          if (activePushController === pushController) activePushController = undefined;
        }

        if (closed) return { status: 'closed' };
        if (targetRegistration !== registration || targetRevision !== registration.revision) {
          // Delivery may have completed on the old target, but a replacement cannot assume that.
          // Replaying is safe because TreeCRDT operations are idempotent by operation id.
          restorePendingOps(ops);
        }
        emitStatus();
      }

      return { status: 'closed' };
    } finally {
      flushing = false;
      emitStatus();
    }
  };

  const flush = (): Promise<OutboundSyncFlushResult> => {
    if (closed) return Promise.resolve({ status: 'closed' });
    if (activeFlush) return activeFlush;
    autoFlushWaitingFor = undefined;
    autoFlushRequested = false;
    autoFlushTargetRevision = undefined;

    const barrier = Promise.resolve().then(runFlush);
    activeFlush = barrier;
    const clear = () => {
      if (activeFlush === barrier) activeFlush = undefined;
    };
    void barrier.then(clear, clear);
    return barrier;
  };

  const close = (): Promise<void> => {
    if (closeBarrier) return closeBarrier;

    closed = true;
    targetRegistration = undefined;
    targetRevision += 1;
    autoFlushWaitingFor = undefined;
    autoFlushRequested = false;
    autoFlushTargetRevision = undefined;
    pendingOps.splice(0, pendingOps.length);
    pendingOpKeys.clear();

    const flushToAwait = activeFlush;
    let resolveClose!: () => void;
    let rejectClose!: (error: unknown) => void;
    closeBarrier = new Promise<void>((resolve, reject) => {
      resolveClose = resolve;
      rejectClose = reject;
    });
    activePushController?.abort(new Error('outbound sync closed'));
    emitStatus();

    void (async () => {
      try {
        if (flushToAwait) await flushToAwait;
        pendingOps.splice(0, pendingOps.length);
        pendingOpKeys.clear();
        resolveClose();
      } catch (error) {
        rejectClose(error);
      }
    })();
    return closeBarrier;
  };

  const controller: OutboundSync<Op> = {
    get status() {
      return statusSnapshot();
    },
    setTarget: (target) => {
      if (closed) return () => {};

      const registration = { target, revision: ++targetRevision };
      targetRegistration = registration;
      activePushController?.abort(new Error('outbound target replaced'));
      emitStatus();
      if (pendingOps.length > 0) queueAutoFlush(registration.revision);

      let released = false;
      return () => {
        if (released) return;
        released = true;
        if (targetRegistration !== registration) return;
        targetRegistration = undefined;
        targetRevision += 1;
        if (autoFlushTargetRevision === registration.revision) {
          autoFlushTargetRevision = undefined;
        }
        activePushController?.abort(new Error('outbound target removed'));
        emitStatus();
      };
    },
    queueOps: (ops) => {
      if (closed || ops.length === 0) return;
      addPendingOps(ops);
      if (closed) return;
      emitStatus();
      if (closed) return;
      if (pendingOps.length > 0) queueAutoFlush();
      try {
        void Promise.resolve(options.notifyLocalUpdate?.(ops)).catch(() => {});
      } catch {
        // Notification is best-effort; exact outbound upload remains queued below.
      }
    },
    flush,
    close,
  };

  emitStatus();
  return controller;
}

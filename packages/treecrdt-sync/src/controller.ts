import type { Operation } from '@treecrdt/interface';
import { bytesToHex } from '@treecrdt/interface/ids';
import type {
  OutboundSync,
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

async function pushWithTimeout(
  run: (signal: AbortSignal) => Promise<void>,
  controller: AbortController,
  ms: number | undefined,
  parentSignal?: AbortSignal,
): Promise<void> {
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
    // Never race this promise: cancellation may not interrupt an already-started storage write.
    await run(controller.signal);
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

/** Queue committed operations for one replaceable destination, preserving caller boundaries. */
export function createOutboundSync<Op = Operation>(
  options: OutboundSyncOptions<Op> = {},
): OutboundSync<Op> {
  type Entry = { op: Op; key: string | undefined };
  type Batch = { sequence: number; entries: Entry[] };
  type Waiter = { through: number; resolve: () => void; reject: (error: unknown) => void };
  const batches: Batch[] = [];
  const outstandingKeys = new Set<string>();
  let waiters: Waiter[] = [];
  let accepted = 0;
  let acknowledged = 0;
  let targetRegistration: { target: OutboundSyncPushTarget<Op> } | undefined;
  let activePushController: AbortController | undefined;
  let activeWorker: Promise<void> | undefined;
  let closeBarrier: Promise<void> | undefined;
  let requested = false;
  let flushing = false;
  let closed = false;
  const closedError = new Error('outbound sync closed');

  const statusSnapshot = (): OutboundSyncStatus => ({
    hasTarget: targetRegistration !== undefined,
    pendingOps: batches.reduce((count, batch) => count + batch.entries.length, 0),
    flushing,
    closed,
  });
  const emitStatus = () => {
    try {
      options.onStatus?.(statusSnapshot());
    } catch {
      /* Observers cannot interrupt bookkeeping. */
    }
  };
  const reportError = (error: unknown) => {
    try {
      options.onError?.(error);
    } catch {
      /* Observers cannot interrupt bookkeeping. */
    }
  };
  const rejectWaiters = (error: unknown) => {
    const rejected = waiters;
    waiters = [];
    for (const waiter of rejected) waiter.reject(error);
  };
  const waitForPrefix = (): Promise<void> => {
    if (closed) return Promise.reject(closedError);
    if (acknowledged === accepted) return Promise.resolve();
    return new Promise((resolve, reject) => waiters.push({ through: accepted, resolve, reject }));
  };
  const fail = (error: unknown, report = true) => {
    requested = false;
    rejectWaiters(error);
    if (report) reportError(error);
    emitStatus();
  };
  const acknowledge = (batch: Batch) => {
    batches.shift();
    for (const { key } of batch.entries) if (key !== undefined) outstandingKeys.delete(key);
    acknowledged = batch.sequence;
    const completed = waiters.filter((waiter) => waiter.through <= acknowledged);
    waiters = waiters.filter((waiter) => waiter.through > acknowledged);
    for (const waiter of completed) waiter.resolve();
    emitStatus();
  };

  const run = async () => {
    if (closed) return;
    flushing = true;
    emitStatus();
    while (!closed && batches.length > 0) {
      requested = false;
      const batch = batches[0]!;
      // Duplicate-only batches still wait behind the outstanding original, but need no new send.
      if (batch.entries.length === 0) {
        acknowledge(batch);
        await Promise.resolve();
        continue;
      }
      try {
        if (options.isOnline && !options.isOnline()) {
          if (closed) return;
          fail(new Error('outbound sync offline'), false);
          if (requested) continue;
          return;
        }
      } catch (error) {
        if (closed) return;
        fail(error);
        if (requested) continue;
        return;
      }
      if (closed) return;
      const registration = targetRegistration;
      if (!registration) {
        fail(new Error('outbound sync has no target'), false);
        if (requested) continue;
        return;
      }
      const pushController = new AbortController();
      activePushController = pushController;
      let failed = false;
      const failAttempt = (error: unknown) => {
        if (failed || closed || targetRegistration !== registration) return;
        failed = true;
        fail(error);
      };
      const onAbort = () => failAttempt(abortReason(pushController.signal));
      pushController.signal.addEventListener('abort', onAbort, { once: true });
      try {
        await pushWithTimeout(
          (signal) =>
            registration.target(
              batch.entries.map(({ op }) => op),
              {
                ...options.pushOptions,
                signal,
              },
            ),
          pushController,
          options.pushTimeoutMs,
          options.pushOptions?.signal,
        );
      } catch (error) {
        failAttempt(error);
      } finally {
        pushController.signal.removeEventListener('abort', onAbort);
        if (activePushController === pushController) activePushController = undefined;
      }
      if (closed) return;
      // Old-target completion cannot acknowledge delivery to its replacement. Replay is idempotent.
      if (targetRegistration !== registration) continue;
      if (failed) {
        if (requested) continue;
        return;
      }
      acknowledge(batch);
      // Let acknowledged callers close or replace the target before starting a later command.
      await Promise.resolve();
    }
  };

  const requestWork = () => {
    if (closed || batches.length === 0) return;
    requested = true;
    if (activeWorker) return;
    const worker = Promise.resolve().then(run);
    activeWorker = worker;
    void worker.then(() => {
      activeWorker = undefined;
      flushing = false;
      emitStatus();
      if (requested) requestWork();
    });
  };

  const close = (): Promise<void> => {
    if (closeBarrier) return closeBarrier;
    closed = true;
    targetRegistration = undefined;
    requested = false;
    batches.length = 0;
    outstandingKeys.clear();
    rejectWaiters(closedError);
    // Publish the shared barrier before abort listeners or status observers can reenter close.
    closeBarrier = activeWorker ? activeWorker.then(() => {}) : Promise.resolve();
    activePushController?.abort(closedError);
    emitStatus();
    return closeBarrier;
  };

  const controller: OutboundSync<Op> = {
    get status() {
      return statusSnapshot();
    },
    setTarget: (target) => {
      if (closed) return () => {};
      const registration = { target };
      targetRegistration = registration;
      activePushController?.abort(new Error('outbound target replaced'));
      emitStatus();
      requestWork();
      return () => {
        if (targetRegistration !== registration) return;
        targetRegistration = undefined;
        requested = false;
        rejectWaiters(new Error('outbound sync has no target'));
        activePushController?.abort(new Error('outbound target removed'));
        emitStatus();
      };
    },
    push: (ops) => {
      if (closed) return Promise.reject(closedError);
      let candidates: Entry[];
      try {
        candidates = ops.map((op) => ({ op, key: options.opKey?.(op) ?? defaultOpKey(op) }));
      } catch (error) {
        return Promise.reject(error);
      }
      if (closed) return Promise.reject(closedError);
      const entries = candidates.filter(({ key }) => {
        if (key === undefined) return true;
        if (outstandingKeys.has(key)) return false;
        outstandingKeys.add(key);
        return true;
      });
      batches.push({ sequence: ++accepted, entries });
      const completion = waitForPrefix();
      emitStatus();
      requestWork();
      if (!closed && ops.length > 0) {
        try {
          void Promise.resolve(options.notifyLocalUpdate?.(ops)).catch(() => {});
        } catch {
          // Notification is best-effort; exact outbound application remains queued.
        }
      }
      return completion;
    },
    flush: () => {
      const completion = waitForPrefix();
      requestWork();
      return completion;
    },
    whenIdle: async () => {
      while (activeWorker) await activeWorker;
    },
    close,
  };
  emitStatus();
  return controller;
}

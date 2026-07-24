import { bytesToHex } from '@treecrdt/interface/ids';
import type {
  Filter,
  SyncMessage,
  SyncOnceOptions,
  SyncPeer,
  SyncSubscribeOptions,
  SyncSubscription,
} from '@treecrdt/sync-protocol';
import type { DuplexTransport } from '@treecrdt/sync-protocol/transport';

export type InboundSyncErrorPhase = 'sync' | 'ready' | 'live';

export type InboundSyncErrorContext<Op = unknown> = {
  peerId: string;
  filter: Filter;
  error: unknown;
  phase: InboundSyncErrorPhase;
  localPeer: SyncPeer<Op>;
};

export type InboundSyncStatus = {
  peerCount: number;
  subscriptionFilterCount: number;
  activeSubscriptionCount: number;
  busy: boolean;
};

export type InboundSyncOnceOptions = {
  peerIds?: readonly string[];
  syncTimeoutMs?: number | ((peerId: string, filter: Filter) => number | undefined);
};

export type InboundSyncTargetFailure = {
  peerId: string;
  filter: Filter;
  error: unknown;
};

/** Raised when one or more requested peer/filter reconciliations fail. */
export class InboundSyncAggregateError extends AggregateError {
  readonly failures: readonly InboundSyncTargetFailure[];

  constructor(failures: readonly InboundSyncTargetFailure[]) {
    super(
      failures.map(({ error }) => error),
      `${failures.length} inbound sync target${failures.length === 1 ? '' : 's'} failed`,
    );
    this.name = 'InboundSyncAggregateError';
    this.failures = failures;
  }
}

export type InboundSyncOptions<Op = unknown> = {
  localPeer: SyncPeer<Op>;
  syncOptions?: (peerId: string, filter: Filter) => SyncOnceOptions | undefined;
  syncTimeoutMs?: number | ((peerId: string, filter: Filter) => number | undefined);
  subscribeOptions?: (peerId: string, filter: Filter) => SyncSubscribeOptions | undefined;
  onError?: (ctx: InboundSyncErrorContext<Op>) => void;
  onStatus?: (status: InboundSyncStatus) => void;
};

export type InboundSync<Op = unknown> = {
  readonly status: InboundSyncStatus;
  readonly peerCount: number;
  /**
   * Registers a transport that is already attached to `localPeer`.
   *
   * The controller neither attaches nor detaches transports. The returned idempotent cleanup only
   * releases this exact registration, so stale connection teardown cannot remove its replacement.
   * Repeated registrations of the same peer and transport remain until their last cleanup runs.
   */
  addAttachedPeer: (peerId: string, transport: DuplexTransport<SyncMessage<Op>>) => () => void;
  /** Explicitly removes whichever registration currently owns `peerId`. */
  removePeer: (peerId: string) => void;
  clearPeers: () => void;
  /**
   * Runs bidirectional reconciliation, despite this controller's inbound-oriented name.
   *
   * Concurrent calls run independently. Each resolves only if every requested peer/filter target
   * succeeds.
   */
  syncOnce: (filters: Filter | readonly Filter[], opts?: InboundSyncOnceOptions) => Promise<void>;
  subscribe: (filters: Filter | readonly Filter[]) => void;
  /**
   * Permanently closes the controller, aborting and draining all work it owns.
   *
   * Repeated calls return the same promise.
   */
  close: () => Promise<void>;
};

function filterKey(filter: Filter): string {
  return 'all' in filter ? 'all' : `children:${bytesToHex(filter.children.parent)}`;
}

function normalizeFilters(filters: Filter | readonly Filter[]): readonly Filter[] {
  return Array.isArray(filters) ? filters : [filters as Filter];
}

function uniqueFilters(filters: Filter | readonly Filter[]): readonly Filter[] {
  const unique = new Map<string, Filter>();
  for (const filter of normalizeFilters(filters)) {
    const key = filterKey(filter);
    if (!unique.has(key)) unique.set(key, filter);
  }
  return Array.from(unique.values());
}

function timeoutMsFor(
  peerId: string,
  filter: Filter,
  local: InboundSyncOnceOptions['syncTimeoutMs'],
  fallback: InboundSyncOptions['syncTimeoutMs'],
) {
  const option = local ?? fallback;
  return typeof option === 'function' ? option(peerId, filter) : option;
}

function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new Error('sync aborted');
}

async function syncTarget<Op>(
  localPeer: SyncPeer<Op>,
  transport: DuplexTransport<SyncMessage<Op>>,
  filter: Filter,
  syncOptions: SyncOnceOptions | undefined,
  ms: number | undefined,
  timeoutMessage: string,
  controller: AbortController,
): Promise<void> {
  if (ms !== undefined && (!Number.isFinite(ms) || ms <= 0)) {
    throw new Error(`invalid syncTimeoutMs: ${ms}`);
  }

  const sourceSignal = syncOptions?.signal;
  const relayAbort = () => {
    if (sourceSignal) controller.abort(abortReason(sourceSignal));
  };
  if (sourceSignal?.aborted) relayAbort();
  else sourceSignal?.addEventListener('abort', relayAbort, { once: true });

  const timer =
    ms === undefined
      ? undefined
      : setTimeout(() => controller.abort(new Error(timeoutMessage)), ms);

  try {
    if (controller.signal.aborted) throw abortReason(controller.signal);
    await localPeer.syncOnce(transport, filter, {
      ...syncOptions,
      signal: controller.signal,
    });
    if (controller.signal.aborted) throw abortReason(controller.signal);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    sourceSignal?.removeEventListener('abort', relayAbort);
  }
}

export function createInboundSync<Op = unknown>(options: InboundSyncOptions<Op>): InboundSync<Op> {
  type PeerRegistration = {
    peerId: string;
    transport: DuplexTransport<SyncMessage<Op>>;
    leaseCount: number;
  };
  type ActiveReconciliation = {
    registration: PeerRegistration;
    controller: AbortController;
  };

  const peers = new Map<string, PeerRegistration>();
  const subscriptionFilters = new Map<string, SubscriptionFilter>();
  const activeReconciliations = new Set<ActiveReconciliation>();
  const activeSyncRuns = new Set<Promise<void>>();
  const subscriptionWork = new Set<Promise<void>>();
  let busyCount = 0;
  let closed = false;
  let closePromise: Promise<void> | undefined;
  let statusMutationDepth = 0;
  let statusPending = false;
  let subscriptionRevision = 0;

  const closedError = () => new Error('Inbound sync is closed.');

  const assertOpen = () => {
    if (closed) throw closedError();
  };

  const activeSubscriptionCount = () => {
    let count = 0;
    for (const subscription of subscriptionFilters.values()) count += subscription.activeCount;
    return count;
  };

  const statusSnapshot = (): InboundSyncStatus => ({
    peerCount: peers.size,
    subscriptionFilterCount: subscriptionFilters.size,
    activeSubscriptionCount: activeSubscriptionCount(),
    busy: busyCount > 0,
  });

  const dispatchStatus = () => {
    try {
      options.onStatus?.(statusSnapshot());
    } catch {
      // Observer failures must not corrupt controller lifecycle state.
    }
  };

  const emitStatus = () => {
    if (statusMutationDepth > 0) {
      statusPending = true;
      return;
    }
    dispatchStatus();
  };

  const mutateStatus = <T>(action: () => T): T => {
    statusMutationDepth += 1;
    try {
      return action();
    } finally {
      statusMutationDepth -= 1;
      if (statusMutationDepth === 0 && statusPending) {
        statusPending = false;
        dispatchStatus();
      }
    }
  };

  const reportError = (ctx: InboundSyncErrorContext<Op>) => {
    try {
      options.onError?.(ctx);
    } catch {
      // Error observers are notifications; the original sync result remains authoritative.
    }
  };

  const beginWork = (notify = true) => {
    busyCount += 1;
    if (notify) emitStatus();
  };

  const endWork = (notify = true) => {
    busyCount = Math.max(0, busyCount - 1);
    if (notify && !closed) emitStatus();
  };

  type SubscriptionState = {
    registration: PeerRegistration;
    subscription?: SyncSubscription;
    ready: boolean;
    starting: boolean;
    stopped: boolean;
    finished: boolean;
    drain: Promise<void>;
    resolveDrain: () => void;
  };

  class SubscriptionFilter {
    private readonly subscriptions = new Map<string, SubscriptionState>();
    private isClosed = false;

    constructor(readonly filter: Filter) {}

    get activeCount() {
      let count = 0;
      for (const state of this.subscriptions.values()) {
        if (state.subscription && !state.stopped) count += 1;
      }
      return count;
    }

    startAll() {
      if (this.isClosed || closed) return;
      for (const registration of peers.values()) this.startPeer(registration);
    }

    stopAll(): readonly Promise<void>[] {
      return Array.from(this.subscriptions.keys(), (peerId) => this.stopPeer(peerId));
    }

    close(): readonly Promise<void>[] {
      if (this.isClosed) return [];
      this.isClosed = true;
      return this.stopAll();
    }

    addPeer(registration: PeerRegistration) {
      if (this.isClosed || closed) return;
      this.startPeer(registration);
    }

    removePeer(peerId: string, registration?: PeerRegistration): Promise<void> {
      if (registration && this.subscriptions.get(peerId)?.registration !== registration) {
        return Promise.resolve();
      }
      return this.stopPeer(peerId);
    }

    private startPeer(registration: PeerRegistration) {
      const { peerId, transport } = registration;
      if (this.isClosed || closed || peers.get(peerId) !== registration) return;
      const existing = this.subscriptions.get(peerId);
      if (existing?.registration === registration) return;
      if (existing) this.stopPeer(peerId);
      if (this.isClosed || closed || peers.get(peerId) !== registration) return;

      // Publish the pending drain before observer, option, or SyncPeer callbacks can reentrantly
      // close the controller. If subscribe returns after such a close, its handle is stopped below
      // and this same drain waits for both ready and done.
      let resolveDrain!: () => void;
      const drain = new Promise<void>((resolve) => {
        resolveDrain = resolve;
      });
      const state: SubscriptionState = {
        registration,
        ready: false,
        starting: true,
        stopped: false,
        finished: false,
        drain,
        resolveDrain,
      };
      this.subscriptions.set(peerId, state);
      subscriptionWork.add(drain);
      void drain.then(() => subscriptionWork.delete(drain));

      const isCurrent = () =>
        !state.stopped &&
        !this.isClosed &&
        !closed &&
        peers.get(peerId) === registration &&
        this.subscriptions.get(peerId) === state;

      beginWork(false);

      let sub: SyncSubscription;
      try {
        if (!isCurrent()) {
          state.starting = false;
          endWork(false);
          this.finishState(state);
          return;
        }
        const subscribeOptions = options.subscribeOptions?.(peerId, this.filter);
        if (!isCurrent()) {
          state.starting = false;
          endWork(false);
          this.finishState(state);
          return;
        }
        sub = options.localPeer.subscribe(transport, this.filter, {
          immediate: true,
          intervalMs: 0,
          ...subscribeOptions,
        });
      } catch (error) {
        state.starting = false;
        if (this.subscriptions.get(peerId) === state) this.subscriptions.delete(peerId);
        endWork(false);
        if (!closed && !state.stopped && peers.get(peerId) === registration) {
          reportError({
            localPeer: options.localPeer,
            peerId,
            filter: this.filter,
            error,
            phase: 'ready',
          });
        }
        this.finishState(state);
        return;
      }

      state.starting = false;
      state.subscription = sub;

      const readyWork = sub.ready
        .then(
          () => {
            if (isCurrent()) state.ready = true;
          },
          (error) => {
            if (!isCurrent()) return;
            this.stopPeer(peerId);
            reportError({
              localPeer: options.localPeer,
              peerId,
              filter: this.filter,
              error,
              phase: 'ready',
            });
          },
        )
        .finally(() => endWork());

      const doneWork = sub.done.then(
        () => {
          if (!isCurrent()) return;
          this.stopPeer(peerId);
          emitStatus();
        },
        (error) => {
          if (!state.ready || !isCurrent()) return;
          this.stopPeer(peerId);
          reportError({
            localPeer: options.localPeer,
            peerId,
            filter: this.filter,
            error,
            phase: 'live',
          });
          emitStatus();
        },
      );

      void Promise.all([readyWork, doneWork]).then(() => this.finishState(state));
      if (!isCurrent()) this.stopState(state);
    }

    private stopPeer(peerId: string): Promise<void> {
      const state = this.subscriptions.get(peerId);
      if (!state) return Promise.resolve();
      this.subscriptions.delete(peerId);
      this.stopState(state);
      return state.drain;
    }

    private stopState(state: SubscriptionState) {
      state.stopped = true;
      if (!state.subscription) {
        if (!state.starting) this.finishState(state);
        return;
      }
      try {
        state.subscription.stop();
      } catch {
        // The subscription promises remain the lifecycle authority.
      }
    }

    private finishState(state: SubscriptionState) {
      if (state.finished) return;
      state.finished = true;
      state.resolveDrain();
    }
  }

  const abortReconciliations = (registration: PeerRegistration, reason: Error) => {
    for (const active of activeReconciliations) {
      if (active.registration === registration) active.controller.abort(reason);
    }
  };

  const retireRegistration = (registration: PeerRegistration, reason: Error) => {
    abortReconciliations(registration, reason);
    for (const subscription of subscriptionFilters.values()) {
      subscription.removePeer(registration.peerId, registration);
    }
  };

  const removeCurrentRegistration = (
    registration: PeerRegistration,
    reason: Error,
    force = false,
  ) => {
    if (peers.get(registration.peerId) !== registration) return;
    if (force) registration.leaseCount = 0;
    peers.delete(registration.peerId);
    retireRegistration(registration, reason);
    if (!closed) emitStatus();
  };

  const leaseRegistration = (registration: PeerRegistration) => {
    registration.leaseCount += 1;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      mutateStatus(() => {
        registration.leaseCount = Math.max(0, registration.leaseCount - 1);
        if (registration.leaseCount > 0) return;
        removeCurrentRegistration(
          registration,
          new Error(`Inbound sync peer registration was removed: ${registration.peerId}`),
        );
      });
    };
  };

  const runSyncOnce = async (
    filters: Filter | readonly Filter[],
    opts: InboundSyncOnceOptions = {},
  ) => {
    assertOpen();
    const filterList = uniqueFilters(filters);
    if (filterList.length === 0) return;
    const targetIds = opts.peerIds ? Array.from(new Set(opts.peerIds)) : Array.from(peers.keys());
    const targets = targetIds.map((peerId) => [peerId, peers.get(peerId)] as const);
    if (targets.length === 0) throw new Error('No peers available for scoped sync.');

    const failures: InboundSyncTargetFailure[] = [];
    beginWork();
    try {
      for (const filter of filterList) {
        for (const [peerId, registration] of targets) {
          let active: ActiveReconciliation | undefined;
          try {
            if (!registration) throw new Error(`Unknown inbound sync peer: ${peerId}`);
            if (peers.get(peerId) !== registration) {
              throw new Error(`Inbound sync peer was removed or replaced: ${peerId}`);
            }
            active = {
              registration,
              controller: new AbortController(),
            };
            activeReconciliations.add(active);
            await syncTarget(
              options.localPeer,
              registration.transport,
              filter,
              options.syncOptions?.(peerId, filter),
              timeoutMsFor(peerId, filter, opts.syncTimeoutMs, options.syncTimeoutMs),
              `sync with ${peerId.slice(0, 8)} timed out`,
              active.controller,
            );
          } catch (error) {
            failures.push({ peerId, filter, error });
            if (!closed && (!registration || peers.get(peerId) === registration)) {
              reportError({
                localPeer: options.localPeer,
                peerId,
                filter,
                error,
                phase: 'sync',
              });
            }
          } finally {
            if (active) activeReconciliations.delete(active);
          }
        }
      }
    } finally {
      endWork();
    }

    if (failures.length > 0) throw new InboundSyncAggregateError(failures);
  };

  const syncOnce = (
    filters: Filter | readonly Filter[],
    opts: InboundSyncOnceOptions = {},
  ): Promise<void> => {
    if (closed) return Promise.reject(closedError());
    // Defer execution so the run is lifecycle-registered before beginWork, option callbacks, or
    // SyncPeer.syncOnce can reentrantly close the controller.
    const run = Promise.resolve().then(() => runSyncOnce(filters, opts));
    activeSyncRuns.add(run);
    void run.then(
      () => activeSyncRuns.delete(run),
      () => activeSyncRuns.delete(run),
    );
    return run;
  };

  const close = (): Promise<void> => {
    if (closePromise) return closePromise;
    let resolveClose!: () => void;
    closePromise = new Promise<void>((resolve) => {
      resolveClose = resolve;
    });
    closed = true;
    const closeReason = closedError();
    for (const active of activeReconciliations) active.controller.abort(closeReason);
    for (const subscription of subscriptionFilters.values()) subscription.close();
    subscriptionFilters.clear();
    for (const registration of peers.values()) registration.leaseCount = 0;
    peers.clear();

    const work = [...activeSyncRuns, ...subscriptionWork];
    void Promise.allSettled(work).then(() => {
      emitStatus();
      resolveClose();
    });
    return closePromise;
  };

  const inbound: InboundSync<Op> = {
    get status() {
      return statusSnapshot();
    },
    get peerCount() {
      return peers.size;
    },
    addAttachedPeer: (peerId, transport) =>
      mutateStatus(() => {
        assertOpen();
        const previous = peers.get(peerId);
        if (previous?.transport === transport) {
          return leaseRegistration(previous);
        }

        const registration = { peerId, transport, leaseCount: 0 };
        const unregister = leaseRegistration(registration);
        peers.set(peerId, registration);
        if (previous) {
          previous.leaseCount = 0;
          retireRegistration(
            previous,
            new Error(`Inbound sync peer transport was replaced: ${peerId}`),
          );
        }
        for (const subscription of Array.from(subscriptionFilters.values())) {
          if (closed || peers.get(peerId) !== registration) break;
          subscription.addPeer(registration);
        }
        if (!closed) emitStatus();
        return unregister;
      }),
    removePeer: (peerId) =>
      mutateStatus(() => {
        assertOpen();
        const registration = peers.get(peerId);
        if (registration) {
          removeCurrentRegistration(
            registration,
            new Error(`Inbound sync peer was removed: ${peerId}`),
            true,
          );
        }
      }),
    clearPeers: () =>
      mutateStatus(() => {
        assertOpen();
        const registrations = Array.from(peers.values());
        for (const registration of registrations) registration.leaseCount = 0;
        peers.clear();
        for (const registration of registrations) {
          retireRegistration(
            registration,
            new Error(`Inbound sync peer was cleared: ${registration.peerId}`),
          );
        }
        if (!closed) emitStatus();
      }),
    syncOnce,
    subscribe: (filters) =>
      mutateStatus(() => {
        assertOpen();
        const revision = ++subscriptionRevision;
        const next = new Map<string, Filter>();
        for (const filter of normalizeFilters(filters)) {
          const key = filterKey(filter);
          if (!next.has(key)) next.set(key, filter);
        }

        for (const [key, subscription] of Array.from(subscriptionFilters.entries())) {
          if (closed || subscriptionRevision !== revision) return;
          if (next.has(key)) continue;
          subscriptionFilters.delete(key);
          subscription.close();
          if (closed || subscriptionRevision !== revision) return;
        }

        for (const [key, filter] of next) {
          if (closed || subscriptionRevision !== revision) return;
          let subscription = subscriptionFilters.get(key);
          if (!subscription) {
            subscription = new SubscriptionFilter(filter);
            subscriptionFilters.set(key, subscription);
          }
          subscription.startAll();
          if (closed || subscriptionRevision !== revision) return;
        }
        emitStatus();
      }),
    close,
  };

  emitStatus();
  return inbound;
}

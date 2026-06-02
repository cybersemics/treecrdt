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
  liveScopeCount: number;
  livePeerCount: number;
  busy: boolean;
};

export type InboundSyncOnceOptions = {
  peerIds?: readonly string[];
  syncTimeoutMs?: number | ((peerId: string, filter: Filter) => number | undefined);
};

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
  addPeer: (peerId: string, transport: DuplexTransport<SyncMessage<Op>>) => void;
  removePeer: (peerId: string) => void;
  clearPeers: () => void;
  syncOnce: (filters: Filter | readonly Filter[], opts?: InboundSyncOnceOptions) => Promise<void>;
  setLiveScopes: (filters: readonly Filter[]) => void;
  close: () => void;
};

function filterKey(filter: Filter): string {
  return 'all' in filter ? 'all' : `children:${bytesToHex(filter.children.parent)}`;
}

function normalizeFilters(filters: Filter | readonly Filter[]): readonly Filter[] {
  return Array.isArray(filters) ? filters : [filters as Filter];
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

function withTimeout<T>(promise: Promise<T>, ms: number | undefined, message: string): Promise<T> {
  if (ms === undefined) return promise;
  if (!Number.isFinite(ms) || ms <= 0) {
    return Promise.reject(new Error(`invalid syncTimeoutMs: ${ms}`));
  }
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

export function createInboundSync<Op = unknown>(options: InboundSyncOptions<Op>): InboundSync<Op> {
  const peers = new Map<string, DuplexTransport<SyncMessage<Op>>>();
  const liveScopes = new Map<string, LiveScope>();
  let busyCount = 0;
  let closed = false;

  const livePeerCount = () => {
    let count = 0;
    for (const scope of liveScopes.values()) count += scope.livePeerCount;
    return count;
  };

  const statusSnapshot = (): InboundSyncStatus => ({
    peerCount: peers.size,
    liveScopeCount: liveScopes.size,
    livePeerCount: livePeerCount(),
    busy: busyCount > 0,
  });

  const emitStatus = () => {
    options.onStatus?.(statusSnapshot());
  };

  const beginWork = () => {
    busyCount += 1;
    emitStatus();
  };

  const endWork = () => {
    busyCount = Math.max(0, busyCount - 1);
    emitStatus();
  };

  const selectedPeers = (peerIds?: readonly string[]) => {
    if (!peerIds) return Array.from(peers.entries());
    const seen = new Set<string>();
    return peerIds.flatMap((peerId) => {
      if (seen.has(peerId)) return [];
      seen.add(peerId);
      const transport = peers.get(peerId);
      return transport ? ([[peerId, transport]] as const) : [];
    });
  };

  class LiveScope {
    private readonly subscriptions = new Map<string, SyncSubscription>();
    private readonly starting = new Set<string>();
    private isClosed = false;

    constructor(readonly filter: Filter) {}

    get livePeerCount() {
      return this.subscriptions.size;
    }

    startAll() {
      if (this.isClosed || closed) return;
      for (const [peerId, transport] of peers) this.startPeer(peerId, transport);
    }

    stopAll() {
      for (const peerId of Array.from(this.subscriptions.keys())) this.stopPeer(peerId);
      this.starting.clear();
      emitStatus();
    }

    close() {
      if (this.isClosed) return;
      this.isClosed = true;
      this.stopAll();
    }

    addPeer(peerId: string, transport: DuplexTransport<SyncMessage<Op>>) {
      if (this.isClosed || closed) return;
      this.startPeer(peerId, transport);
    }

    removePeer(peerId: string) {
      this.stopPeer(peerId);
      emitStatus();
    }

    private startPeer(peerId: string, transport: DuplexTransport<SyncMessage<Op>>) {
      if (this.subscriptions.has(peerId) || this.starting.has(peerId)) return;
      this.starting.add(peerId);
      beginWork();

      let ready = false;
      const sub = options.localPeer.subscribe(transport, this.filter, {
        immediate: true,
        intervalMs: 0,
        ...options.subscribeOptions?.(peerId, this.filter),
      });
      this.subscriptions.set(peerId, sub);
      emitStatus();

      const isCurrent = () => !this.isClosed && !closed && this.subscriptions.get(peerId) === sub;

      void sub.done.catch((error) => {
        if (!ready || !isCurrent()) return;
        this.stopPeer(peerId);
        options.onError?.({
          localPeer: options.localPeer,
          peerId,
          filter: this.filter,
          error,
          phase: 'live',
        });
        emitStatus();
      });

      void (async () => {
        try {
          await sub.ready;
          if (isCurrent()) ready = true;
        } catch (error) {
          if (isCurrent()) {
            this.stopPeer(peerId);
            options.onError?.({
              localPeer: options.localPeer,
              peerId,
              filter: this.filter,
              error,
              phase: 'ready',
            });
          }
        } finally {
          this.starting.delete(peerId);
          endWork();
        }
      })();
    }

    private stopPeer(peerId: string) {
      this.starting.delete(peerId);
      const sub = this.subscriptions.get(peerId);
      if (!sub) return;
      try {
        sub.stop();
      } finally {
        this.subscriptions.delete(peerId);
      }
    }
  }

  const syncOnce = async (
    filters: Filter | readonly Filter[],
    opts: InboundSyncOnceOptions = {},
  ) => {
    if (closed) return;
    const filterList = normalizeFilters(filters);
    if (filterList.length === 0) return;
    const targets = selectedPeers(opts.peerIds);
    if (targets.length === 0) throw new Error('No peers available for scoped sync.');

    let successes = 0;
    let lastError: unknown = null;
    beginWork();
    try {
      for (const filter of filterList) {
        for (const [peerId, transport] of targets) {
          try {
            await withTimeout(
              options.localPeer.syncOnce(transport, filter, options.syncOptions?.(peerId, filter)),
              timeoutMsFor(peerId, filter, opts.syncTimeoutMs, options.syncTimeoutMs),
              `sync with ${peerId.slice(0, 8)} timed out`,
            );
            successes += 1;
          } catch (error) {
            lastError = error;
            options.onError?.({
              localPeer: options.localPeer,
              peerId,
              filter,
              error,
              phase: 'sync',
            });
          }
        }
      }
    } finally {
      endWork();
    }

    if (successes === 0) {
      if (lastError) throw lastError;
      throw new Error('No peers responded to scoped sync.');
    }
  };

  const inbound: InboundSync<Op> = {
    get status() {
      return statusSnapshot();
    },
    get peerCount() {
      return peers.size;
    },
    addPeer: (peerId, transport) => {
      if (closed) return;
      const previous = peers.get(peerId);
      peers.set(peerId, transport);
      if (previous && previous !== transport) {
        for (const scope of liveScopes.values()) scope.removePeer(peerId);
      }
      for (const scope of liveScopes.values()) scope.addPeer(peerId, transport);
      emitStatus();
    },
    removePeer: (peerId) => {
      peers.delete(peerId);
      for (const scope of liveScopes.values()) scope.removePeer(peerId);
      emitStatus();
    },
    clearPeers: () => {
      peers.clear();
      for (const scope of liveScopes.values()) scope.stopAll();
      emitStatus();
    },
    syncOnce,
    setLiveScopes: (filters) => {
      if (closed) return;
      const next = new Map<string, Filter>();
      for (const filter of filters) {
        const key = filterKey(filter);
        if (!next.has(key)) next.set(key, filter);
      }

      for (const [key, scope] of Array.from(liveScopes.entries())) {
        if (next.has(key)) continue;
        scope.close();
        liveScopes.delete(key);
      }

      for (const [key, filter] of next) {
        if (liveScopes.has(key)) continue;
        const scope = new LiveScope(filter);
        liveScopes.set(key, scope);
        scope.startAll();
      }
      emitStatus();
    },
    close: () => {
      if (closed) return;
      closed = true;
      for (const scope of Array.from(liveScopes.values())) scope.close();
      liveScopes.clear();
      peers.clear();
      emitStatus();
    },
  };

  emitStatus();
  return inbound;
}

import type {
  Filter,
  SyncMessage,
  SyncOnceOptions,
  SyncPeer,
  SyncSubscribeOptions,
  SyncSubscription,
} from '@treecrdt/sync-protocol';
import type { DuplexTransport } from '@treecrdt/sync-protocol/transport';

export type ScopeControllerErrorPhase = 'sync' | 'ready' | 'live';

export type ScopeControllerErrorContext<Op = unknown> = {
  peerId: string;
  filter: Filter;
  error: unknown;
  phase: ScopeControllerErrorPhase;
  peer: SyncPeer<Op>;
};

export type ScopeControllerOptions<Op = unknown> = {
  peer: SyncPeer<Op>;
  shouldSyncPeer?: (peerId: string) => boolean;
  syncOptions?: (peerId: string, filter: Filter) => SyncOnceOptions | undefined;
  subscribeOptions?: (peerId: string, filter: Filter) => SyncSubscribeOptions | undefined;
  onWorkStart?: () => void;
  onWorkEnd?: () => void;
  onError?: (ctx: ScopeControllerErrorContext<Op>) => void;
};

export type SyncScope = {
  readonly filter: Filter;
  readonly live: boolean;
  readonly livePeerCount: number;
  syncOnce: () => Promise<void>;
  startLive: () => void;
  stopLive: () => void;
  close: () => void;
};

export type ScopeController<Op = unknown> = {
  readonly peerCount: number;
  setPeer: (peerId: string, transport: DuplexTransport<SyncMessage<Op>>) => void;
  deletePeer: (peerId: string) => void;
  clearPeers: () => void;
  scope: (filter: Filter) => SyncScope;
  close: () => void;
};

export function createScopeController<Op = unknown>(
  options: ScopeControllerOptions<Op>,
): ScopeController<Op> {
  const peers = new Map<string, DuplexTransport<SyncMessage<Op>>>();
  const scopes = new Set<SyncScopeHandle>();
  let closed = false;

  const shouldSyncPeer = (peerId: string) => options.shouldSyncPeer?.(peerId) ?? true;
  const selectedPeers = () =>
    Array.from(peers.entries()).filter(([peerId]) => shouldSyncPeer(peerId));

  class SyncScopeHandle implements SyncScope {
    private readonly subscriptions = new Map<string, SyncSubscription>();
    private readonly starting = new Set<string>();
    private isLive = false;
    private isClosed = false;

    constructor(readonly filter: Filter) {}

    get live() {
      return this.isLive;
    }

    get livePeerCount() {
      return this.subscriptions.size;
    }

    async syncOnce() {
      if (this.isClosed || closed) return;
      const targets = selectedPeers();
      if (targets.length === 0) throw new Error('No peers available for scoped sync.');

      let successes = 0;
      let lastError: unknown = null;
      options.onWorkStart?.();
      try {
        for (const [peerId, transport] of targets) {
          try {
            await options.peer.syncOnce(
              transport,
              this.filter,
              options.syncOptions?.(peerId, this.filter),
            );
            successes += 1;
          } catch (error) {
            lastError = error;
            options.onError?.({
              peer: options.peer,
              peerId,
              filter: this.filter,
              error,
              phase: 'sync',
            });
          }
        }
      } finally {
        options.onWorkEnd?.();
      }

      if (successes === 0) {
        if (lastError) throw lastError;
        throw new Error('No peers responded to scoped sync.');
      }
    }

    startLive() {
      if (this.isClosed || closed) return;
      if (this.isLive) return;
      this.isLive = true;
      for (const [peerId, transport] of selectedPeers()) this.startPeer(peerId, transport);
    }

    stopLive() {
      this.isLive = false;
      for (const peerId of Array.from(this.subscriptions.keys())) this.stopPeer(peerId);
      this.starting.clear();
    }

    close() {
      if (this.isClosed) return;
      this.isClosed = true;
      this.stopLive();
      scopes.delete(this);
    }

    setPeer(peerId: string, transport: DuplexTransport<SyncMessage<Op>>) {
      if (!this.isLive || this.isClosed || closed) return;
      this.startPeer(peerId, transport);
    }

    deletePeer(peerId: string) {
      this.stopPeer(peerId);
    }

    clearPeers() {
      for (const peerId of Array.from(this.subscriptions.keys())) this.stopPeer(peerId);
      this.starting.clear();
    }

    private startPeer(peerId: string, transport: DuplexTransport<SyncMessage<Op>>) {
      if (!shouldSyncPeer(peerId)) return;
      if (this.subscriptions.has(peerId) || this.starting.has(peerId)) return;
      this.starting.add(peerId);
      options.onWorkStart?.();

      let ready = false;
      const sub = options.peer.subscribe(transport, this.filter, {
        immediate: true,
        intervalMs: 0,
        ...options.subscribeOptions?.(peerId, this.filter),
      });
      this.subscriptions.set(peerId, sub);

      void sub.done.catch((error) => {
        if (!ready || this.isClosed || closed) return;
        this.stopPeer(peerId);
        options.onError?.({
          peer: options.peer,
          peerId,
          filter: this.filter,
          error,
          phase: 'live',
        });
      });

      void (async () => {
        try {
          await sub.ready;
          ready = true;
        } catch (error) {
          this.stopPeer(peerId);
          options.onError?.({
            peer: options.peer,
            peerId,
            filter: this.filter,
            error,
            phase: 'ready',
          });
        } finally {
          this.starting.delete(peerId);
          options.onWorkEnd?.();
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

  const controller: ScopeController<Op> = {
    get peerCount() {
      return peers.size;
    },
    setPeer: (peerId, transport) => {
      if (closed) return;
      const previous = peers.get(peerId);
      peers.set(peerId, transport);
      if (previous && previous !== transport) {
        for (const scope of scopes) scope.deletePeer(peerId);
      }
      for (const scope of scopes) scope.setPeer(peerId, transport);
    },
    deletePeer: (peerId) => {
      peers.delete(peerId);
      for (const scope of scopes) scope.deletePeer(peerId);
    },
    clearPeers: () => {
      peers.clear();
      for (const scope of scopes) scope.clearPeers();
    },
    scope: (filter) => {
      if (closed) throw new Error('ScopeController: closed');
      const scope = new SyncScopeHandle(filter);
      scopes.add(scope);
      return scope;
    },
    close: () => {
      if (closed) return;
      closed = true;
      for (const scope of Array.from(scopes)) scope.close();
      peers.clear();
    },
  };

  return controller;
}

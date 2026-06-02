import type { Operation } from '@treecrdt/interface';
import { bytesToHex } from '@treecrdt/interface/ids';
import type { SyncMessage } from '@treecrdt/sync-protocol';
import type { DuplexTransport } from '@treecrdt/sync-protocol/transport';
import type { OutboundSync, OutboundSyncOptions, OutboundSyncStatus } from './types.js';

function outboundSyncStatusSnapshot<Op>(
  peers: ReadonlyMap<string, DuplexTransport<SyncMessage<Op>>>,
  pendingOps: readonly Op[],
  running: boolean,
  scheduled: boolean,
): OutboundSyncStatus {
  return {
    peerCount: peers.size,
    pendingOps: pendingOps.length,
    running,
    scheduled,
  };
}

function withTimeout<T>(promise: Promise<T>, ms: number | undefined, message: string): Promise<T> {
  if (ms === undefined) return promise;
  if (!Number.isFinite(ms) || ms <= 0) {
    return Promise.reject(new Error(`invalid pushTimeoutMs: ${ms}`));
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

function defaultOpKey(op: unknown): string | undefined {
  const id = (op as { meta?: { id?: { replica?: unknown; counter?: unknown } } })?.meta?.id;
  if (!(id?.replica instanceof Uint8Array)) return undefined;
  if (typeof id.counter !== 'number') return undefined;
  return `${bytesToHex(id.replica)}:${id.counter}`;
}

/**
 * Queue exact committed local writes for a single {@link SyncPeer} that is attached to one or more
 * outbound transports.
 *
 * `queueOps` also notifies the low-level peer about the local update, so apps can report a local
 * write once instead of separately waking live subscriptions and queueing remote upload.
 *
 * Apps that also use local-tab mesh transports should attach those transports directly to the
 * low-level peer, but only register outbound upload targets with this controller.
 */
export function createOutboundSync<Op = Operation>(
  options: OutboundSyncOptions<Op>,
): OutboundSync<Op> {
  const peers = new Map<string, DuplexTransport<SyncMessage<Op>>>();
  const pendingOps: Op[] = [];
  const pendingOpKeys = new Set<string>();
  let running = false;
  let scheduled = false;
  let closed = false;

  const emitStatus = () => {
    options.onStatus?.(outboundSyncStatusSnapshot(peers, pendingOps, running, scheduled));
  };

  const addPendingOps = (ops: readonly Op[]) => {
    for (const op of ops) {
      const key = options.opKey?.(op) ?? defaultOpKey(op);
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

  const pushTimeoutMs = (peerId: string) =>
    typeof options.pushTimeoutMs === 'function'
      ? options.pushTimeoutMs(peerId)
      : options.pushTimeoutMs;

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
    if (!scheduled && pendingOps.length > 0) scheduled = true;
    if (!scheduled) {
      emitStatus();
      return;
    }

    running = true;
    emitStatus();
    try {
      while (scheduled && !closed) {
        scheduled = false;
        if (options.isOnline && !options.isOnline()) {
          emitStatus();
          return;
        }

        const targets = Array.from(peers.entries());
        if (targets.length === 0) {
          emitStatus();
          return;
        }

        const ops = takePendingOps();
        if (ops.length === 0) {
          emitStatus();
          continue;
        }

        let failed = false;
        for (const [peerId, transport] of targets) {
          try {
            await withTimeout(
              options.localPeer.pushOps(transport, ops, options.pushOptions?.(peerId)),
              pushTimeoutMs(peerId),
              `outbound push with ${peerId.slice(0, 8)} timed out`,
            );
          } catch (error) {
            failed = true;
            options.onError?.({ peerId, error });
          }
        }

        if (failed) {
          restorePendingOps(ops);
          emitStatus();
          return;
        }

        emitStatus();
      }
    } finally {
      running = false;
      emitStatus();
    }
  };

  const controller: OutboundSync<Op> = {
    get status() {
      return outboundSyncStatusSnapshot(peers, pendingOps, running, scheduled);
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
      if (pendingOps.length > 0) scheduleFlush();
    },
    removePeer: (peerId) => {
      peers.delete(peerId);
      emitStatus();
    },
    clearPeers: () => {
      peers.clear();
      emitStatus();
    },
    queueOps: (ops) => {
      if (closed || ops.length === 0) return;
      void options.localPeer.notifyLocalUpdate(ops);
      addPendingOps(ops);
      if (pendingOps.length > 0) scheduleFlush();
    },
    flush,
    close: () => {
      closed = true;
      scheduled = false;
      pendingOps.splice(0, pendingOps.length);
      pendingOpKeys.clear();
      peers.clear();
      emitStatus();
    },
  };

  emitStatus();
  return controller;
}

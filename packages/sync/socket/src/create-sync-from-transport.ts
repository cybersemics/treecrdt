import type { Operation } from '@treecrdt/interface';
import type { TreecrdtEngineOps } from '@treecrdt/interface/engine';
import { createTreecrdtSyncBackendFromClient } from '@treecrdt/sync-sqlite/backend';
import {
  SyncPeer,
  deriveOpRefV0,
  type Filter,
  type SyncBackend,
  type SyncMessage,
  type SyncOnceOptions,
  type SyncPeerOptions,
  type SyncSubscribeOptions,
} from '@treecrdt/sync';
import { DEFAULT_LIVE_SUBSCRIBE, DEFAULT_SYNC_ONCE } from './constants.js';
import type { CreateTreecrdtWebSocketSyncFromTransportOptions, TreecrdtWebSocketSync, TreecrdtWebSocketSyncClient } from './types.js';
import type { DuplexTransport } from '@treecrdt/sync/transport';

const defaultAutoNotify = true;

function mergeSyncOnceOptions(opts: SyncOnceOptions = {}): SyncOnceOptions {
  return {
    ...opts,
    maxCodewords: opts.maxCodewords ?? DEFAULT_SYNC_ONCE.maxCodewords,
    maxOpsPerBatch: opts.maxOpsPerBatch ?? DEFAULT_SYNC_ONCE.maxOpsPerBatch,
    codewordsPerMessage: opts.codewordsPerMessage ?? DEFAULT_SYNC_ONCE.codewordsPerMessage,
  };
}

function mergeLiveSubscribeOptions(opts: SyncSubscribeOptions = {}): SyncSubscribeOptions {
  return {
    ...opts,
    maxCodewords: opts.maxCodewords ?? DEFAULT_LIVE_SUBSCRIBE.maxCodewords,
    maxOpsPerBatch: opts.maxOpsPerBatch ?? DEFAULT_LIVE_SUBSCRIBE.maxOpsPerBatch,
    codewordsPerMessage: opts.codewordsPerMessage ?? DEFAULT_LIVE_SUBSCRIBE.codewordsPerMessage,
    intervalMs: opts.intervalMs ?? DEFAULT_LIVE_SUBSCRIBE.intervalMs,
    immediate: opts.immediate ?? DEFAULT_LIVE_SUBSCRIBE.immediate,
  };
}

/**
 * Build a high-level sync handle for an existing duplex transport (used by WebSocket in production;
 * in-memory transport in tests). Attaches a {@link SyncPeer} to the transport; `onCloseTransport`
 * runs after the peer is detached (e.g. close WebSocket).
 */
export function createTreecrdtWebSocketSyncFromTransport(
  client: TreecrdtWebSocketSyncClient,
  transport: DuplexTransport<SyncMessage<Operation>>,
  onCloseTransport: (() => void) | undefined,
  options: CreateTreecrdtWebSocketSyncFromTransportOptions = {},
): TreecrdtWebSocketSync {
  const {
    enablePendingSidecar = false,
    auth,
    syncPeerOptions: extraPeerOptions,
    autoNotifyLocalOnWrite = defaultAutoNotify,
  } = options;

  const getMaxLamport = () => client.meta.headLamport().then((n) => BigInt(n));

  const remoteApplyDepth = { value: 0 };
  const isLive = { value: false };
  let liveSubscription: { stop: () => void } | null = null;
  let peer!: SyncPeer<Operation>;
  let closed = false;

  const tryNotifyLocal = (ops: readonly Operation[]) => {
    if (!isLive.value || !autoNotifyLocalOnWrite || ops.length === 0) return;
    if (remoteApplyDepth.value > 0) return;
    void peer.notifyLocalUpdate([...ops]);
  };

  const savedOps = client.ops;
  const patchedOps: TreecrdtEngineOps = {
    ...savedOps,
    append: async (op, writeOpts) => {
      if (remoteApplyDepth.value > 0) {
        return savedOps.append(op, writeOpts);
      }
      await savedOps.append(op, writeOpts);
      tryNotifyLocal([op]);
    },
    appendMany: async (ops, writeOpts) => {
      if (remoteApplyDepth.value > 0) {
        return savedOps.appendMany(ops, writeOpts);
      }
      await savedOps.appendMany(ops, writeOpts);
      tryNotifyLocal(ops);
    },
  };

  client.ops = patchedOps;

  const baseBackend = createTreecrdtSyncBackendFromClient(client, client.docId, {
    maxLamport: getMaxLamport,
    enablePendingSidecar,
  });

  const backend: SyncBackend<Operation> = {
    ...baseBackend,
    applyOps: async (ops) => {
      if (ops.length === 0) return;
      remoteApplyDepth.value += 1;
      try {
        await baseBackend.applyOps(ops);
      } finally {
        remoteApplyDepth.value -= 1;
      }
    },
  };

  const syncPeerBase: SyncPeerOptions<Operation> = {
    maxCodewords: 2_000_000,
    maxOpsPerBatch: 20_000,
    deriveOpRef: (op, ctx) =>
      deriveOpRefV0(ctx.docId, { replica: op.meta.id.replica, counter: op.meta.id.counter }),
    ...extraPeerOptions,
    ...(auth ? { auth } : {}),
  };

  peer = new SyncPeer(backend, syncPeerBase);
  const unlisten = peer.attach(transport);

  const ensureOpen = () => {
    if (closed) throw new Error('TreecrdtWebSocketSync: connection is closed');
  };

  const onChange: TreecrdtWebSocketSync['onChange'] = (listener) => client.onMaterialized(listener);

  const result: TreecrdtWebSocketSync = {
    onChange,
    syncOnce: async (filter: Filter = { all: {} }, opts: SyncOnceOptions = {}) => {
      ensureOpen();
      await peer.syncOnce(transport, filter, mergeSyncOnceOptions(opts));
    },
    startLive: async (subscribeOpts: SyncSubscribeOptions = {}) => {
      ensureOpen();
      if (liveSubscription) return;
      const merged = mergeLiveSubscribeOptions(subscribeOpts);
      const sub = peer.subscribe(transport, { all: {} }, merged);
      liveSubscription = sub;
      void sub.done.catch((err) => {
        isLive.value = false;
        if (liveSubscription === sub) liveSubscription = null;
        console.error('TreecrdtWebSocketSync: live subscription failed', err);
      });
      try {
        await sub.ready;
      } catch (err) {
        isLive.value = false;
        if (liveSubscription === sub) liveSubscription = null;
        throw err;
      }
      isLive.value = true;
    },
    stopLive: () => {
      if (liveSubscription) {
        try {
          liveSubscription.stop();
        } catch {
          // ignore
        }
        liveSubscription = null;
      }
      isLive.value = false;
    },
    notifyLocalUpdate: (ops) => peer.notifyLocalUpdate(ops),
    close: async () => {
      if (closed) return;
      closed = true;
      result.stopLive();
      client.ops = savedOps;
      try {
        unlisten();
      } catch {
        // ignore
      }
      try {
        onCloseTransport?.();
      } catch {
        // ignore
      }
    },
  };

  return result;
}

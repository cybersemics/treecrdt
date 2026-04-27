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
} from '@treecrdt/sync-protocol';
import { DEFAULT_LIVE_SUBSCRIBE, DEFAULT_SYNC_ONCE } from './constants.js';
import type {
  CreateTreecrdtWebSocketSyncFromTransportOptions,
  TreecrdtWebSocketSync,
  TreecrdtWebSocketSyncClient,
} from './types.js';
import type { DuplexTransport } from '@treecrdt/sync-protocol/transport';

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
    onLiveError,
  } = options;

  const reportLiveError =
    onLiveError ??
    ((err: unknown) => {
      console.error('TreecrdtWebSocketSync: live subscription failed', err);
    });

  const getMaxLamport = () => client.meta.headLamport().then((n) => BigInt(n));

  const ingestDepth = { value: 0 };
  const liveOn = { value: false };
  let liveSub: { stop: () => void } | null = null;
  let peer!: SyncPeer<Operation>;
  let closed = false;

  const afterLocalWrite = (ops: readonly Operation[]) => {
    if (!liveOn.value || !autoNotifyLocalOnWrite || ops.length === 0) return;
    if (ingestDepth.value > 0) return;
    void peer.notifyLocalUpdate([...ops]);
  };

  const baseOps = client.ops;
  const wrappedOps: TreecrdtEngineOps = {
    ...baseOps,
    append: async (op, writeOpts) => {
      if (ingestDepth.value > 0) {
        return baseOps.append(op, writeOpts);
      }
      await baseOps.append(op, writeOpts);
      afterLocalWrite([op]);
    },
    appendMany: async (ops, writeOpts) => {
      if (ingestDepth.value > 0) {
        return baseOps.appendMany(ops, writeOpts);
      }
      await baseOps.appendMany(ops, writeOpts);
      afterLocalWrite(ops);
    },
  };

  client.ops = wrappedOps;

  const clientBackend = createTreecrdtSyncBackendFromClient(client, client.docId, {
    maxLamport: getMaxLamport,
    enablePendingSidecar,
  });

  const backend: SyncBackend<Operation> = {
    ...clientBackend,
    applyOps: async (ops) => {
      if (ops.length === 0) return;
      ingestDepth.value += 1;
      try {
        await clientBackend.applyOps(ops);
      } finally {
        ingestDepth.value -= 1;
      }
    },
  };

  const peerOpts: SyncPeerOptions<Operation> = {
    maxCodewords: 2_000_000,
    maxOpsPerBatch: 20_000,
    deriveOpRef: (op, ctx) =>
      deriveOpRefV0(ctx.docId, { replica: op.meta.id.replica, counter: op.meta.id.counter }),
    ...extraPeerOptions,
    ...(auth ? { auth } : {}),
  };

  peer = new SyncPeer(backend, peerOpts);
  const detach = peer.attach(transport);

  const assertOpen = () => {
    if (closed) throw new Error('TreecrdtWebSocketSync: connection is closed');
  };

  const onChange: TreecrdtWebSocketSync['onChange'] = (listener) => client.onMaterialized(listener);

  const handle: TreecrdtWebSocketSync = {
    onChange,
    syncOnce: async (filter: Filter = { all: {} }, opts: SyncOnceOptions = {}) => {
      assertOpen();
      await peer.syncOnce(transport, filter, mergeSyncOnceOptions(opts));
    },
    startLive: async (subscribeOpts: SyncSubscribeOptions = {}) => {
      assertOpen();
      if (liveSub) return;
      const subOpts = mergeLiveSubscribeOptions(subscribeOpts);
      const sub = peer.subscribe(transport, { all: {} }, subOpts);
      liveSub = sub;
      void sub.done.catch((err) => {
        liveOn.value = false;
        if (liveSub === sub) liveSub = null;
        reportLiveError(err);
      });
      try {
        await sub.ready;
      } catch (err) {
        liveOn.value = false;
        if (liveSub === sub) liveSub = null;
        throw err;
      }
      liveOn.value = true;
    },
    stopLive: () => {
      if (liveSub) {
        try {
          liveSub.stop();
        } catch {
          // ignore
        }
        liveSub = null;
      }
      liveOn.value = false;
    },
    notifyLocalUpdate: (ops) => peer.notifyLocalUpdate(ops),
    close: async () => {
      if (closed) return;
      closed = true;
      handle.stopLive();
      client.ops = baseOps;
      try {
        detach();
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

  return handle;
}

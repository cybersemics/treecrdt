import type { Operation } from '@treecrdt/interface';
import { createTreecrdtSyncBackendFromClient } from '@treecrdt/sync-sqlite/backend';
import {
  SyncPeer,
  deriveOpRefV0,
  type Filter,
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
  const { enablePendingSidecar = false, auth, syncPeerOptions: extraPeerOptions, onLiveError } =
    options;

  const reportLiveError =
    onLiveError ??
    ((err: unknown) => {
      console.error('TreecrdtWebSocketSync: live subscription failed', err);
    });

  const getMaxLamport = () => client.meta.headLamport().then((n) => BigInt(n));

  let liveSub: { stop: () => void } | null = null;
  let peer!: SyncPeer<Operation>;
  let closed = false;

  const backend = createTreecrdtSyncBackendFromClient(client, client.docId, {
    maxLamport: getMaxLamport,
    enablePendingSidecar,
  });

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
        if (liveSub === sub) liveSub = null;
        reportLiveError(err);
      });
      try {
        await sub.ready;
      } catch (err) {
        if (liveSub === sub) liveSub = null;
        throw err;
      }
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
    },
    pushLocalOps: (ops) => peer.notifyLocalUpdate(ops),
    close: async () => {
      if (closed) return;
      closed = true;
      handle.stopLive();
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

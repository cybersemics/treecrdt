import type { SyncPeerOptions } from "./sync.js";
import { SyncPeer } from "./sync.js";
import type { DuplexTransport, WireCodec } from "./transport.js";
import { createInMemoryDuplex, wrapDuplexTransportWithCodec } from "./transport.js";
import type { Filter, OpRef, SyncBackend, SyncMessage } from "./types.js";

export type FlushableSyncBackend<Op> = SyncBackend<Op> & { flush: () => Promise<void> };

export function makeQueuedSyncBackend<Op>(opts: {
  docId: string;
  initialMaxLamport: number;
  maxLamportFromOps: (ops: Op[]) => number;
  listOpRefs: (filter: Filter) => Promise<OpRef[]>;
  getOpsByOpRefs: (opRefs: OpRef[]) => Promise<Op[]>;
  applyOps: (ops: Op[]) => Promise<void>;
}): FlushableSyncBackend<Op> {
  let maxLamportValue = opts.initialMaxLamport;
  let lastApply = Promise.resolve();

  return {
    docId: opts.docId,
    maxLamport: async () => BigInt(maxLamportValue),
    listOpRefs: opts.listOpRefs,
    getOpsByOpRefs: opts.getOpsByOpRefs,
    applyOps: async (ops: Op[]) => {
      if (ops.length === 0) return;
      const nextMax = opts.maxLamportFromOps(ops);
      if (nextMax > maxLamportValue) maxLamportValue = nextMax;
      lastApply = lastApply.then(() => opts.applyOps(ops));
      await lastApply;
    },
    flush: async () => lastApply,
  };
}

export type InMemoryConnectedPeers<Op> = {
  peerA: SyncPeer<Op>;
  peerB: SyncPeer<Op>;
  transportA: DuplexTransport<SyncMessage<Op>>;
  transportB: DuplexTransport<SyncMessage<Op>>;
  detach: () => void;
};

export function createInMemoryConnectedPeers<Op>(opts: {
  backendA: SyncBackend<Op>;
  backendB: SyncBackend<Op>;
  codec: WireCodec<SyncMessage<Op>, Uint8Array>;
  peerOptions?: SyncPeerOptions<Op>;
  peerAOptions?: SyncPeerOptions<Op>;
  peerBOptions?: SyncPeerOptions<Op>;
}): InMemoryConnectedPeers<Op> {
  const [wireA, wireB] = createInMemoryDuplex<Uint8Array>();
  const transportA = wrapDuplexTransportWithCodec(wireA, opts.codec);
  const transportB = wrapDuplexTransportWithCodec(wireB, opts.codec);

  const peerAOptions = opts.peerAOptions ?? opts.peerOptions;
  const peerBOptions = opts.peerBOptions ?? opts.peerOptions;
  const peerA = new SyncPeer(opts.backendA, peerAOptions);
  const peerB = new SyncPeer(opts.backendB, peerBOptions);

  const detachA = peerA.attach(transportA);
  const detachB = peerB.attach(transportB);

  return {
    peerA,
    peerB,
    transportA,
    transportB,
    detach: () => {
      detachA();
      detachB();
    },
  };
}

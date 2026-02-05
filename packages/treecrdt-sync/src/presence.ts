import type { BroadcastChannelLike, DuplexTransport, WireCodec } from "./transport.js";
import { createBroadcastDuplex } from "./transport.js";

export type BroadcastPresenceMessageV1 = {
  t: "presence";
  peer_id: string;
  ts: number;
};

export type BroadcastPresenceAckMessageV1 = {
  t: "presence_ack";
  peer_id: string;
  to_peer_id: string;
  ts: number;
};

export type BroadcastPresencePeer = {
  id: string;
  lastSeen: number;
  ready: boolean;
};

type Connection<M> = {
  transport: DuplexTransport<M> & { close?: () => void };
  detach: () => void;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object";
}

function safePeerId(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const clean = value.trim();
  if (!clean) return null;
  return clean;
}

export function createBroadcastPresenceMesh<M>(opts: {
  channel: BroadcastChannelLike;
  selfId: string;
  codec: WireCodec<M, Uint8Array>;
  isOnline?: () => boolean;
  nowMs?: () => number;
  presenceIntervalMs?: number;
  pruneIntervalMs?: number;
  peerTimeoutMs?: number;
  createChannel?: (name: string) => BroadcastChannelLike;
  debug?: boolean;
  log?: (line: string) => void;
  onBroadcastMessage?: (msg: unknown) => void;
  onPeersChanged?: (peers: BroadcastPresencePeer[]) => void;
  onPeerReady?: (peerId: string) => void;
  onPeerTransport?: (peerId: string, transport: DuplexTransport<M>) => (() => void) | void;
  onPeerDisconnected?: (peerId: string) => void;
}): {
  getPeers: () => BroadcastPresencePeer[];
  isPeerReady: (peerId: string) => boolean;
  postBroadcastMessage: (msg: unknown) => boolean;
  disconnectPeer: (peerId: string) => void;
  stop: () => void;
} {
  const nowMs = opts.nowMs ?? (() => Date.now());
  const presenceIntervalMs = opts.presenceIntervalMs ?? 1_000;
  const pruneIntervalMs = opts.pruneIntervalMs ?? 2_000;
  const peerTimeoutMs = opts.peerTimeoutMs ?? 5_000;

  const channel = opts.channel;
  const selfId = opts.selfId;

  const connections = new Map<string, Connection<M>>();
  const lastSeen = new Map<string, number>();
  const peerReady = new Set<string>();
  const peerAckSent = new Set<string>();

  const updatePeers = () => {
    const peers = Array.from(lastSeen.entries())
      .map(([id, ts]) => ({ id, lastSeen: ts, ready: peerReady.has(id) }))
      .sort((a, b) => (a.id === b.id ? 0 : a.id < b.id ? -1 : 1));
    opts.onPeersChanged?.(peers);
  };

  const isOnline = () => (opts.isOnline ? opts.isOnline() : true);

  const sendPresenceAck = (toPeerId: string) => {
    const msg = {
      t: "presence_ack",
      peer_id: selfId,
      to_peer_id: toPeerId,
      ts: nowMs(),
    } as const satisfies BroadcastPresenceAckMessageV1;
    channel.postMessage(msg);
  };

  const ensureAckSent = (peerId: string) => {
    if (!peerId || peerId === selfId) return;
    if (peerAckSent.has(peerId)) return;
    peerAckSent.add(peerId);
    sendPresenceAck(peerId);
  };

  const ensureConnection = (peerId: string) => {
    if (!peerId || peerId === selfId) return;
    if (connections.has(peerId)) return;

    const rawTransport = createBroadcastDuplex<M>(channel, selfId, peerId, opts.codec, {
      debug: opts.debug,
      log: opts.log,
      createChannel: opts.createChannel,
    });

    const transport: DuplexTransport<M> & { close?: () => void } = {
      ...rawTransport,
      async send(msg) {
        if (!isOnline()) return;
        return rawTransport.send(msg);
      },
      onMessage(handler) {
        return rawTransport.onMessage((msg) => {
          if (!isOnline()) return;
          lastSeen.set(peerId, nowMs());
          return handler(msg);
        });
      },
    };

    const detach = opts.onPeerTransport?.(peerId, transport) ?? (() => {});
    connections.set(peerId, { transport, detach: typeof detach === "function" ? detach : () => {} });
  };

  const disconnectPeer = (peerId: string) => {
    const conn = connections.get(peerId);
    if (conn) {
      try {
        conn.detach();
      } finally {
        conn.transport.close?.();
        connections.delete(peerId);
      }
    }
    lastSeen.delete(peerId);
    peerReady.delete(peerId);
    peerAckSent.delete(peerId);
    opts.onPeerDisconnected?.(peerId);
    updatePeers();
  };

  const onBroadcast = (ev: { data: unknown }) => {
    const data = ev.data;

    if (!isRecord(data)) {
      opts.onBroadcastMessage?.(data);
      return;
    }

    const t = data.t;
    if (t === "presence") {
      const peerId = safePeerId(data.peer_id);
      const ts = typeof data.ts === "number" ? data.ts : null;
      if (!peerId || peerId === selfId || ts === null) return;

      lastSeen.set(peerId, ts);
      ensureConnection(peerId);
      ensureAckSent(peerId);
      updatePeers();
      return;
    }

    if (t === "presence_ack") {
      const peerId = safePeerId(data.peer_id);
      const toPeerId = safePeerId(data.to_peer_id);
      const ts = typeof data.ts === "number" ? data.ts : null;
      if (!peerId || peerId === selfId || !toPeerId || ts === null) return;
      if (toPeerId !== selfId) return;

      lastSeen.set(peerId, ts);
      ensureConnection(peerId);

      const wasReady = peerReady.has(peerId);
      peerReady.add(peerId);
      if (!wasReady) opts.onPeerReady?.(peerId);

      ensureAckSent(peerId);
      updatePeers();
      return;
    }

    opts.onBroadcastMessage?.(data);
  };

  channel.addEventListener("message", onBroadcast);

  const sendPresence = () => {
    if (!isOnline()) return;
    const msg: BroadcastPresenceMessageV1 = { t: "presence", peer_id: selfId, ts: nowMs() };
    channel.postMessage(msg);
  };

  sendPresence();
  const presenceTimer = setInterval(sendPresence, presenceIntervalMs);

  const pruneTimer = setInterval(() => {
    const now = nowMs();
    let changed = false;
    for (const [id, ts] of lastSeen) {
      if (now - ts <= peerTimeoutMs) continue;
      lastSeen.delete(id);
      peerReady.delete(id);
      peerAckSent.delete(id);
      const conn = connections.get(id);
      if (conn) {
        try {
          conn.detach();
        } finally {
          conn.transport.close?.();
          connections.delete(id);
        }
      }
      opts.onPeerDisconnected?.(id);
      changed = true;
    }
    if (changed) updatePeers();
  }, pruneIntervalMs);

  return {
    getPeers: () =>
      Array.from(lastSeen.entries())
        .map(([id, ts]) => ({ id, lastSeen: ts, ready: peerReady.has(id) }))
        .sort((a, b) => (a.id === b.id ? 0 : a.id < b.id ? -1 : 1)),
    isPeerReady: (peerId: string) => peerReady.has(peerId),
    postBroadcastMessage: (msg: unknown) => {
      if (!isOnline()) return false;
      channel.postMessage(msg);
      return true;
    },
    disconnectPeer,
    stop: () => {
      clearInterval(presenceTimer);
      clearInterval(pruneTimer);
      channel.removeEventListener("message", onBroadcast);

      for (const [peerId, conn] of connections) {
        try {
          conn.detach();
        } finally {
          conn.transport.close?.();
        }
        connections.delete(peerId);
      }
      lastSeen.clear();
      peerReady.clear();
      peerAckSent.clear();
      updatePeers();
    },
  };
}

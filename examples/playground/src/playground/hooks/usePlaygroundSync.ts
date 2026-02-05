import { useEffect, useRef, useState } from "react";
import type { Operation } from "@treecrdt/interface";
import { bytesToHex } from "@treecrdt/interface/ids";
import {
  base64urlDecode,
  createTreecrdtCoseCwtAuth,
  createTreecrdtIdentityChainCapabilityV1,
  createTreecrdtSqliteSubtreeScopeEvaluator,
  type TreecrdtIdentityChainV1,
} from "@treecrdt/auth";
import {
  SyncPeer,
  createTreecrdtSyncSqliteOpAuthStore,
  type Filter,
  type SyncSubscription,
} from "@treecrdt/sync";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync/protobuf";
import type { DuplexTransport } from "@treecrdt/sync/transport";
import type { TreecrdtClient } from "@treecrdt/wa-sqlite/client";

import {
  createBroadcastDuplex,
  createPlaygroundBackend,
  hexToBytes16,
  type AuthGrantMessageV1,
  type PresenceAckMessage,
  type PresenceMessage,
} from "../../sync-v0";
import {
  PLAYGROUND_PEER_TIMEOUT_MS,
  PLAYGROUND_SYNC_MAX_CODEWORDS,
  PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
  ROOT_ID,
} from "../constants";
import type { PeerInfo, TreeState } from "../types";
import type { StoredAuthMaterial } from "../../auth";

function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (err) => {
        clearTimeout(timer);
        reject(err);
      }
    );
  });
}

export type PlaygroundSyncApi = {
  peers: PeerInfo[];
  syncBusy: boolean;
  syncError: string | null;
  setSyncError: React.Dispatch<React.SetStateAction<string | null>>;
  liveChildrenParents: Set<string>;
  setLiveChildrenParents: React.Dispatch<React.SetStateAction<Set<string>>>;
  liveAllEnabled: boolean;
  setLiveAllEnabled: React.Dispatch<React.SetStateAction<boolean>>;
  toggleLiveChildren: (parentId: string) => void;
  notifyLocalUpdate: () => void;
  handleSync: (filter: Filter) => Promise<void>;
  handleScopedSync: () => Promise<void>;
  postBroadcastMessage: (msg: PresenceMessage | PresenceAckMessage | AuthGrantMessageV1) => boolean;
};

export type UsePlaygroundSyncOptions = {
  client: TreecrdtClient | null;
  status: "booting" | "ready" | "error";
  docId: string;
  selfPeerId: string | null;
  online: boolean;
  getMaxLamport: () => bigint;
  authEnabled: boolean;
  authMaterial: StoredAuthMaterial;
  authError: string | null;
  joinMode: boolean;
  authCanSyncAll: boolean;
  viewRootId: string;
  treeStateRef: React.MutableRefObject<TreeState>;
  refreshMeta: () => Promise<void>;
  refreshParents: (parentIds: string[]) => Promise<void>;
  refreshNodeCount: () => Promise<void>;
  getLocalIdentityChain: () => Promise<TreecrdtIdentityChainV1 | null>;
  onPeerIdentityChain: (chain: {
    identityPublicKey: Uint8Array;
    devicePublicKey: Uint8Array;
    replicaPublicKey: Uint8Array;
  }) => void;
  onAuthGrantMessage?: (grant: AuthGrantMessageV1) => void;
  onRemoteOpsApplied: (ops: Operation[]) => Promise<void> | void;
};

export function usePlaygroundSync(opts: UsePlaygroundSyncOptions): PlaygroundSyncApi {
  const {
    client,
    status,
    docId,
    selfPeerId,
    online,
    getMaxLamport,
    authEnabled,
    authMaterial,
    authError,
    joinMode,
    authCanSyncAll,
    viewRootId,
    treeStateRef,
    refreshMeta,
    refreshParents,
    refreshNodeCount,
    getLocalIdentityChain,
    onPeerIdentityChain,
    onAuthGrantMessage,
    onRemoteOpsApplied,
  } = opts;

  const [syncBusy, setSyncBusy] = useState(false);
  const [syncError, setSyncError] = useState<string | null>(null);
  const [peers, setPeers] = useState<PeerInfo[]>([]);
  const [liveChildrenParents, setLiveChildrenParents] = useState<Set<string>>(() => new Set());
  const [liveAllEnabled, setLiveAllEnabled] = useState(false);

  const onlineRef = useRef(true);
  useEffect(() => {
    onlineRef.current = online;
  }, [online]);

  const broadcastChannelRef = useRef<BroadcastChannel | null>(null);

  const syncPeerRef = useRef<SyncPeer<Operation> | null>(null);
  const syncConnRef = useRef<Map<string, { transport: DuplexTransport<any>; detach: () => void }>>(new Map());
  const liveChildrenParentsRef = useRef<Set<string>>(new Set());
  const liveChildSubsRef = useRef<Map<string, Map<string, SyncSubscription>>>(new Map());
  const liveAllEnabledRef = useRef(false);
  const liveAllSubsRef = useRef<Map<string, SyncSubscription>>(new Map());
  const liveAllStartingRef = useRef<Set<string>>(new Set());
  const liveChildrenStartingRef = useRef<Set<string>>(new Set());
  const peerReadyRef = useRef<Set<string>>(new Set());
  const peerAckSentRef = useRef<Set<string>>(new Set());

  const stopLiveAllForPeer = (peerId: string) => {
    const existing = liveAllSubsRef.current.get(peerId);
    if (!existing) return;
    existing.stop();
    liveAllSubsRef.current.delete(peerId);
  };

  const stopAllLiveAll = () => {
    for (const sub of liveAllSubsRef.current.values()) sub.stop();
    liveAllSubsRef.current.clear();
  };

  const startLiveAll = (peerId: string) => {
    const conn = syncConnRef.current.get(peerId);
    const peer = syncPeerRef.current;
    if (!conn || !peer) return;

    if (liveAllSubsRef.current.has(peerId)) return;
    if (liveAllStartingRef.current.has(peerId)) return;
    liveAllStartingRef.current.add(peerId);

    void (async () => {
      try {
        await peer.syncOnce(
          conn.transport,
          { all: {} },
          {
            maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
            maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
            codewordsPerMessage: 1024,
          }
        );
      } catch (err) {
        console.error("Live sync(all) initial catch-up failed", err);
        setSyncError(err instanceof Error ? err.message : String(err));
        return;
      }

      const sub = peer.subscribe(
        conn.transport,
        { all: {} },
        {
          immediate: false,
          intervalMs: 0,
          maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
          maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
          codewordsPerMessage: 1024,
        }
      );
      liveAllSubsRef.current.set(peerId, sub);
      void sub.done.catch((err) => {
        console.error("Live sync(all) failed", err);
        stopLiveAllForPeer(peerId);
        setSyncError(err instanceof Error ? err.message : String(err));
      });
    })().finally(() => {
      liveAllStartingRef.current.delete(peerId);
    });
  };

  const stopLiveChildrenForPeer = (peerId: string) => {
    const byParent = liveChildSubsRef.current.get(peerId);
    if (!byParent) return;
    for (const sub of byParent.values()) sub.stop();
    liveChildSubsRef.current.delete(peerId);
  };

  const stopLiveChildren = (peerId: string, parentId: string) => {
    const byParent = liveChildSubsRef.current.get(peerId);
    if (!byParent) return;
    const sub = byParent.get(parentId);
    if (!sub) return;
    sub.stop();
    byParent.delete(parentId);
    if (byParent.size === 0) liveChildSubsRef.current.delete(peerId);
  };

  const stopAllLiveChildren = () => {
    for (const peerId of Array.from(liveChildSubsRef.current.keys())) stopLiveChildrenForPeer(peerId);
  };

  const startLiveChildren = (peerId: string, parentId: string) => {
    const conn = syncConnRef.current.get(peerId);
    const peer = syncPeerRef.current;
    if (!conn || !peer) return;

    const existing = liveChildSubsRef.current.get(peerId);
    if (existing?.has(parentId)) return;
    const startKey = `${peerId}\u0000${parentId}`;
    if (liveChildrenStartingRef.current.has(startKey)) return;
    liveChildrenStartingRef.current.add(startKey);

    const byParent = existing ?? new Map<string, SyncSubscription>();
    void (async () => {
      try {
        await peer.syncOnce(
          conn.transport,
          { children: { parent: hexToBytes16(parentId) } },
          {
            maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
            maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
            codewordsPerMessage: 1024,
          }
        );
      } catch (err) {
        console.error("Live sync(children) initial catch-up failed", err);
        setSyncError(err instanceof Error ? err.message : String(err));
        return;
      }

      const sub = peer.subscribe(
        conn.transport,
        { children: { parent: hexToBytes16(parentId) } },
        {
          immediate: false,
          intervalMs: 0,
          maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
          maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
          codewordsPerMessage: 1024,
        }
      );
      byParent.set(parentId, sub);
      liveChildSubsRef.current.set(peerId, byParent);

      void sub.done.catch((err) => {
        console.error("Live sync failed", err);
        stopLiveChildren(peerId, parentId);
        setSyncError(err instanceof Error ? err.message : String(err));
      });
    })().finally(() => {
      liveChildrenStartingRef.current.delete(startKey);
    });
  };

  const toggleLiveChildren = (parentId: string) => {
    setLiveChildrenParents((prev) => {
      const next = new Set(prev);
      if (next.has(parentId)) next.delete(parentId);
      else next.add(parentId);
      return next;
    });
  };

  const notifyLocalUpdate = () => {
    void syncPeerRef.current?.notifyLocalUpdate();
  };

  const dropPeerConnection = (peerId: string) => {
    const connections = syncConnRef.current;
    const conn = connections.get(peerId);
    if (!conn) return;
    try {
      conn.detach();
    } catch {
      // ignore
    }
    try {
      (conn.transport as any).close?.();
    } catch {
      // ignore
    }
    connections.delete(peerId);
    peerReadyRef.current.delete(peerId);
    peerAckSentRef.current.delete(peerId);
    stopLiveAllForPeer(peerId);
    stopLiveChildrenForPeer(peerId);
    setPeers((prev) => prev.filter((p) => p.id !== peerId));
  };

  const handleSync = async (filter: Filter) => {
    if (!onlineRef.current) {
      setSyncError("Offline: toggle Online to sync.");
      return;
    }
    const peer = syncPeerRef.current;
    if (!peer) {
      setSyncError("Sync peer is not ready yet.");
      return;
    }
    const connections = syncConnRef.current;
    if (connections.size === 0) {
      setSyncError("No peers discovered yet.");
      return;
    }

    setSyncBusy(true);
    setSyncError(null);
    try {
      const now = Date.now();
      const recentPeerIds = peers
        .filter((p) => now - p.lastSeen < 5_000)
        .sort((a, b) => b.lastSeen - a.lastSeen)
        .map((p) => p.id);
      const targets = recentPeerIds.length > 0 ? recentPeerIds : Array.from(connections.keys());
      const perPeerTimeoutMs = targets.length > 1 ? 8_000 : 15_000;

      let successes = 0;
      let lastErr: unknown = null;
      for (const peerId of targets) {
        const conn = connections.get(peerId);
        if (!conn) continue;
        try {
          await withTimeout(
            peer.syncOnce(conn.transport, filter, {
              maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
              maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
              codewordsPerMessage: 2048,
            }),
            perPeerTimeoutMs,
            `sync with ${peerId.slice(0, 8)}… timed out`
          );
          successes += 1;
        } catch (err) {
          lastErr = err;
          console.error("Sync failed for peer", peerId, err);
          dropPeerConnection(peerId);
        }
      }
      if (successes === 0) {
        if (lastErr) throw lastErr;
        throw new Error("No peers responded to sync.");
      }
      await refreshMeta();
      await refreshParents(Object.keys(treeStateRef.current.childrenByParent));
      await refreshNodeCount();
    } catch (err) {
      console.error("Sync failed", err);
      setSyncError(err instanceof Error ? err.message : String(err));
    } finally {
      setSyncBusy(false);
    }
  };

  const handleScopedSync = async () => {
    const parents = new Set(Object.keys(treeStateRef.current.childrenByParent));
    parents.add(viewRootId);
    if (viewRootId !== ROOT_ID) parents.delete(ROOT_ID);
    const parentIds = Array.from(parents).filter((id) => /^[0-9a-f]{32}$/i.test(id));
    parentIds.sort();

    if (!onlineRef.current) {
      setSyncError("Offline: toggle Online to sync.");
      return;
    }
    const peer = syncPeerRef.current;
    if (!peer) {
      setSyncError("Sync peer is not ready yet.");
      return;
    }
    const connections = syncConnRef.current;
    if (connections.size === 0) {
      setSyncError("No peers discovered yet.");
      return;
    }

    setSyncBusy(true);
    setSyncError(null);
    try {
      const now = Date.now();
      const recentPeerIds = peers
        .filter((p) => now - p.lastSeen < 5_000)
        .sort((a, b) => b.lastSeen - a.lastSeen)
        .map((p) => p.id);
      const targets = recentPeerIds.length > 0 ? recentPeerIds : Array.from(connections.keys());
      const perPeerTimeoutMs = targets.length > 1 ? 8_000 : 15_000;

      let successes = 0;
      let lastErr: unknown = null;
      for (const peerId of targets) {
        const conn = connections.get(peerId);
        if (!conn) continue;
        try {
          for (const parentId of parentIds) {
            await withTimeout(
              peer.syncOnce(
                conn.transport,
                { children: { parent: hexToBytes16(parentId) } },
                {
                  maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
                  maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
                  codewordsPerMessage: 2048,
                }
              ),
              perPeerTimeoutMs,
              `sync(children ${parentId.slice(0, 8)}…) with ${peerId.slice(0, 8)}… timed out`
            );
          }
          successes += 1;
        } catch (err) {
          lastErr = err;
          console.error("Scoped sync failed for peer", peerId, err);
          dropPeerConnection(peerId);
        }
      }
      if (successes === 0) {
        if (lastErr) throw lastErr;
        throw new Error("No peers responded to sync.");
      }
      await refreshMeta();
      await refreshParents(Object.keys(treeStateRef.current.childrenByParent));
      await refreshNodeCount();
    } catch (err) {
      console.error("Scoped sync failed", err);
      setSyncError(err instanceof Error ? err.message : String(err));
    } finally {
      setSyncBusy(false);
    }
  };

  const postBroadcastMessage = (msg: PresenceMessage | PresenceAckMessage | AuthGrantMessageV1) => {
    const channel = broadcastChannelRef.current;
    if (!channel) return false;
    channel.postMessage(msg);
    return true;
  };

  useEffect(() => {
    liveChildrenParentsRef.current = liveChildrenParents;

    const connections = syncConnRef.current;
    for (const peerId of connections.keys()) {
      for (const parentId of liveChildrenParents) startLiveChildren(peerId, parentId);
    }

    for (const peerId of Array.from(liveChildSubsRef.current.keys())) {
      if (!connections.has(peerId)) {
        stopLiveChildrenForPeer(peerId);
        continue;
      }
      const byParent = liveChildSubsRef.current.get(peerId);
      if (!byParent) continue;
      for (const parentId of Array.from(byParent.keys())) {
        if (!liveChildrenParents.has(parentId)) stopLiveChildren(peerId, parentId);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveChildrenParents]);

  useEffect(() => {
    liveAllEnabledRef.current = liveAllEnabled;
    const connections = syncConnRef.current;
    if (liveAllEnabled) {
      for (const peerId of connections.keys()) startLiveAll(peerId);
    } else {
      stopAllLiveAll();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveAllEnabled]);

  useEffect(() => {
    if (!authCanSyncAll && liveAllEnabled) setLiveAllEnabled(false);
  }, [authCanSyncAll, liveAllEnabled]);

  useEffect(() => {
    if (!client || status !== "ready") return;
    if (!docId) return;
    if (typeof BroadcastChannel === "undefined") {
      setSyncError("BroadcastChannel is not available in this environment.");
      return;
    }

    const peerAuthConfig =
      authEnabled &&
      authMaterial.issuerPkB64 &&
      authMaterial.localSkB64 &&
      authMaterial.localPkB64 &&
      authMaterial.localTokensB64.length > 0
        ? {
            issuerPk: base64urlDecode(authMaterial.issuerPkB64),
            localSk: base64urlDecode(authMaterial.localSkB64),
            localPk: base64urlDecode(authMaterial.localPkB64),
            localTokens: authMaterial.localTokensB64.map((t) => base64urlDecode(t)),
            opAuthStore: createTreecrdtSyncSqliteOpAuthStore({ runner: client.runner, docId }),
            scopeEvaluator: createTreecrdtSqliteSubtreeScopeEvaluator(client.runner),
            getLocalIdentityChain,
            onPeerIdentityChain,
          }
        : null;

    if (!authEnabled) {
      // If auth is off, clear any auth-gating error strings so the UI doesn't keep telling users to import invites.
      setSyncError((prev) =>
        prev && (prev.startsWith("Auth enabled:") || prev.startsWith("Initializing local peer key")) ? null : prev
      );
    }

    if (authEnabled && !peerAuthConfig) {
      const waitingForInvite = joinMode && authMaterial.localTokensB64.length === 0;
      setSyncError(waitingForInvite ? null : authError ?? "Auth enabled: initializing keys/tokens...");
      return;
    }

    if (!selfPeerId) {
      setSyncError("Initializing local peer key...");
      return;
    }

    setSyncError((prev) =>
      prev && (prev.includes("initializing keys/tokens") || prev.startsWith("Initializing local peer key")) ? null : prev
    );

    const debugSync = typeof window !== "undefined" && new URLSearchParams(window.location.search).has("debugSync");

    const channel = new BroadcastChannel(`treecrdt-sync-v0:${docId}`);
    broadcastChannelRef.current = channel;
    const baseBackend = createPlaygroundBackend(client, docId, { enablePendingSidecar: authEnabled });
    const backend = {
      ...baseBackend,
      maxLamport: async () => {
        return getMaxLamport();
      },
      listOpRefs: async (filter: Filter) => {
        const refs = await baseBackend.listOpRefs(filter);
        if (debugSync) {
          const name = "all" in filter ? "all" : `children(${bytesToHex(filter.children.parent)})`;
          console.debug(`[sync:${selfPeerId}] listOpRefs(${name}) -> ${refs.length}`);
        }
        return refs;
      },
      applyOps: async (ops: Operation[]) => {
        if (debugSync && ops.length > 0) {
          console.debug(`[sync:${selfPeerId}] applyOps(${ops.length})`);
        }
        await baseBackend.applyOps(ops);
        await onRemoteOpsApplied(ops);
      },
    };

    const sharedPeer = new SyncPeer<Operation>(backend, {
      maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
      maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
      ...(peerAuthConfig
        ? {
            auth: (() => {
              const baseAuth = createTreecrdtCoseCwtAuth({
                issuerPublicKeys: [peerAuthConfig.issuerPk],
                localPrivateKey: peerAuthConfig.localSk,
                localPublicKey: peerAuthConfig.localPk,
                localCapabilityTokens: peerAuthConfig.localTokens,
                requireProofRef: true,
                opAuthStore: peerAuthConfig.opAuthStore,
                scopeEvaluator: peerAuthConfig.scopeEvaluator,
                onPeerIdentityChain: peerAuthConfig.onPeerIdentityChain,
              });

              const withIdentity: typeof baseAuth = {
                ...baseAuth,
                helloCapabilities: async (ctx) => {
                  const caps = (await baseAuth.helloCapabilities?.(ctx)) ?? [];
                  try {
                    const chain = await peerAuthConfig.getLocalIdentityChain();
                    if (chain) caps.push(createTreecrdtIdentityChainCapabilityV1(chain));
                  } catch {
                    // Best-effort; identity chains are optional.
                  }
                  return caps;
                },
                onHello: async (hello, ctx) => {
                  const ackCaps = (await baseAuth.onHello?.(hello, ctx)) ?? [];
                  try {
                    const chain = await peerAuthConfig.getLocalIdentityChain();
                    if (chain) ackCaps.push(createTreecrdtIdentityChainCapabilityV1(chain));
                  } catch {
                    // Best-effort; identity chains are optional.
                  }
                  return ackCaps;
                },
              };

              return withIdentity;
            })(),
          }
        : {}),
    });
    syncPeerRef.current = sharedPeer;

    const connections = new Map<string, { transport: DuplexTransport<any>; detach: () => void }>();
    const lastSeen = new Map<string, number>();
    syncConnRef.current = connections;
    peerReadyRef.current.clear();
    peerAckSentRef.current.clear();

    const updatePeers = () => {
      setPeers(
        Array.from(lastSeen.entries())
          .map(([id, ts]) => ({ id, lastSeen: ts }))
          .sort((a, b) => (a.id === b.id ? 0 : a.id < b.id ? -1 : 1))
      );
    };

    const maybeStartLiveForPeer = (peerId: string) => {
      if (!peerReadyRef.current.has(peerId)) return;
      if (liveAllEnabledRef.current) startLiveAll(peerId);
      for (const parentId of liveChildrenParentsRef.current) startLiveChildren(peerId, parentId);
    };

    const sendPresenceAck = (toPeerId: string) => {
      const msg = {
        t: "presence_ack",
        peer_id: selfPeerId,
        to_peer_id: toPeerId,
        ts: Date.now(),
      } as const satisfies PresenceAckMessage;
      channel.postMessage(msg);
    };

    const ensureAckSent = (peerId: string) => {
      if (!peerId || peerId === selfPeerId) return;
      if (peerAckSentRef.current.has(peerId)) return;
      peerAckSentRef.current.add(peerId);
      sendPresenceAck(peerId);
    };

    const ensureConnection = (peerId: string) => {
      if (!peerId || peerId === selfPeerId) return;
      if (connections.has(peerId)) return;

      const rawTransport = createBroadcastDuplex<Operation>(channel, selfPeerId, peerId, treecrdtSyncV0ProtobufCodec);
      const transport: DuplexTransport<any> = {
        ...rawTransport,
        async send(msg) {
          if (!onlineRef.current) return;
          return rawTransport.send(msg);
        },
        onMessage(handler) {
          return rawTransport.onMessage((msg) => {
            if (!onlineRef.current) return;
            lastSeen.set(peerId, Date.now());
            return handler(msg);
          });
        },
      };
      const detach = sharedPeer.attach(transport);
      connections.set(peerId, { transport, detach });

      maybeStartLiveForPeer(peerId);
    };

    const onBroadcast = (ev: MessageEvent<any>) => {
      const data = ev.data as unknown;
      if (!data || typeof data !== "object") return;
      const msg = data as Partial<PresenceMessage | PresenceAckMessage | AuthGrantMessageV1>;

      if (msg.t === "auth_grant_v1") {
        const grant = msg as Partial<AuthGrantMessageV1>;
        if (typeof grant.doc_id !== "string") return;
        if (grant.doc_id !== docId) return;
        if (typeof grant.to_replica_pk_hex !== "string") return;
        if (typeof grant.issuer_pk_b64 !== "string") return;
        if (typeof grant.token_b64 !== "string") return;

        const localReplicaHex = selfPeerId;
        if (!localReplicaHex) return;
        if (grant.to_replica_pk_hex.toLowerCase() !== localReplicaHex.toLowerCase()) return;

        if (onAuthGrantMessage) onAuthGrantMessage(grant as AuthGrantMessageV1);
        return;
      }

      if (msg.t === "presence") {
        if (typeof msg.peer_id !== "string" || typeof msg.ts !== "number") return;
        if (msg.peer_id === selfPeerId) return;
        lastSeen.set(msg.peer_id, msg.ts);
        ensureConnection(msg.peer_id);
        ensureAckSent(msg.peer_id);
        maybeStartLiveForPeer(msg.peer_id);
        updatePeers();
        return;
      }

      if (msg.t === "presence_ack") {
        const toPeerId = (msg as Partial<PresenceAckMessage>).to_peer_id;
        if (typeof msg.peer_id !== "string" || typeof toPeerId !== "string" || typeof msg.ts !== "number") return;
        if (msg.peer_id === selfPeerId) return;
        if (toPeerId !== selfPeerId) return;
        lastSeen.set(msg.peer_id, msg.ts);
        ensureConnection(msg.peer_id);
        peerReadyRef.current.add(msg.peer_id);
        ensureAckSent(msg.peer_id);
        maybeStartLiveForPeer(msg.peer_id);
        updatePeers();
      }
    };

    channel.addEventListener("message", onBroadcast);

    const sendPresence = () => {
      if (!onlineRef.current) return;
      const msg: PresenceMessage = { t: "presence", peer_id: selfPeerId, ts: Date.now() };
      channel.postMessage(msg);
    };

    sendPresence();
    const interval = window.setInterval(sendPresence, 1000);
    const pruneInterval = window.setInterval(() => {
      const now = Date.now();
      for (const [id, ts] of lastSeen) {
        if (now - ts > PLAYGROUND_PEER_TIMEOUT_MS) {
          lastSeen.delete(id);
          peerReadyRef.current.delete(id);
          peerAckSentRef.current.delete(id);
          const conn = connections.get(id);
          if (conn) {
            conn.detach();
            connections.delete(id);
          }
          stopLiveAllForPeer(id);
          stopLiveChildrenForPeer(id);
        }
      }
      updatePeers();
    }, 2000);

    return () => {
      window.clearInterval(interval);
      window.clearInterval(pruneInterval);
      channel.removeEventListener("message", onBroadcast);
      stopAllLiveAll();
      stopAllLiveChildren();
      if (broadcastChannelRef.current === channel) broadcastChannelRef.current = null;
      if (syncPeerRef.current === sharedPeer) syncPeerRef.current = null;
      channel.close();
      liveAllStartingRef.current.clear();
      liveChildrenStartingRef.current.clear();
      peerReadyRef.current.clear();
      peerAckSentRef.current.clear();

      for (const conn of connections.values()) {
        conn.detach();
        (conn.transport as any).close?.();
      }
      connections.clear();
      setPeers([]);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    authEnabled,
    authError,
    authMaterial,
    client,
    docId,
    getLocalIdentityChain,
    getMaxLamport,
    joinMode,
    onAuthGrantMessage,
    onPeerIdentityChain,
    onRemoteOpsApplied,
    selfPeerId,
    status,
  ]);

  return {
    peers,
    syncBusy,
    syncError,
    setSyncError,
    liveChildrenParents,
    setLiveChildrenParents,
    liveAllEnabled,
    setLiveAllEnabled,
    toggleLiveChildren,
    notifyLocalUpdate,
    handleSync,
    handleScopedSync,
    postBroadcastMessage,
  };
}

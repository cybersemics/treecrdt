import { useEffect, useRef, useState } from "react";
import type { Operation } from "@treecrdt/interface";
import { bytesToHex } from "@treecrdt/interface/ids";
import {
  SyncPeer,
  type Filter,
  type SyncAuth,
  type SyncSubscription,
} from "@treecrdt/sync";
import { createTreecrdtSyncBackendFromClient } from "@treecrdt/sync-sqlite";
import type { BroadcastPresenceAckMessageV1, BroadcastPresenceMessageV1 } from "@treecrdt/sync/browser";
import { createBroadcastPresenceMesh, createBrowserWebSocketTransport } from "@treecrdt/sync/browser";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync/protobuf";
import { wrapDuplexTransportWithCodec, type DuplexTransport } from "@treecrdt/sync/transport";
import type { TreecrdtClient } from "@treecrdt/wa-sqlite/client";

import {
  hexToBytes16,
  type AuthGrantMessageV1,
} from "../../sync-v0";
import {
  PLAYGROUND_PEER_TIMEOUT_MS,
  PLAYGROUND_REMOTE_SYNC_TIMEOUT_MS,
  PLAYGROUND_SYNC_MAX_CODEWORDS,
  PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
  ROOT_ID,
} from "../constants";
import type { PeerInfo, RemoteSyncStatus, SyncTransportMode, TreeState } from "../types";

const REMOTE_SYNC_CODEWORDS_PER_MESSAGE = 512;

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

function normalizeSyncServerUrl(raw: string, docId: string): URL {
  let input = raw.trim();
  if (input.length === 0) throw new Error("Sync server URL is empty");
  if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(input)) input = `ws://${input}`;

  const url = new URL(input);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  if (url.protocol !== "ws:" && url.protocol !== "wss:") {
    throw new Error("Sync server URL must use ws://, wss://, http://, or https://");
  }
  if (url.pathname === "/" || url.pathname.length === 0) {
    url.pathname = "/sync";
  }
  url.searchParams.set("docId", docId);
  return url;
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function isCapabilityRevokedError(err: unknown): boolean {
  return /capability token revoked/i.test(errorMessage(err));
}

function formatSyncError(err: unknown): string {
  if (isCapabilityRevokedError(err)) {
    return "Access revoked for this capability. Import/update access, then sync again.";
  }
  if (/unknown author:/i.test(errorMessage(err))) {
    return "This document contains ops from an author whose capability token is not available here yet. Sync from a peer that has the full author history, or try a fresh doc.";
  }
  return errorMessage(err);
}

export type PlaygroundSyncApi = {
  peers: PeerInfo[];
  remoteSyncStatus: RemoteSyncStatus;
  syncBusy: boolean;
  liveBusy: boolean;
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
  postBroadcastMessage: (
    msg: BroadcastPresenceMessageV1 | BroadcastPresenceAckMessageV1 | AuthGrantMessageV1
  ) => boolean;
};

export type UsePlaygroundSyncOptions = {
  client: TreecrdtClient | null;
  status: "booting" | "ready" | "error";
  docId: string;
  selfPeerId: string | null;
  autoSyncJoin?: boolean;
  syncServerUrl?: string;
  transportMode?: SyncTransportMode;
  online: boolean;
  getMaxLamport: () => bigint;
  authEnabled: boolean;
  syncAuth: SyncAuth<Operation> | null;
  authError: string | null;
  joinMode: boolean;
  authNeedsInvite: boolean;
  authCanSyncAll: boolean;
  viewRootId: string;
  treeStateRef: React.MutableRefObject<TreeState>;
  refreshMeta: () => Promise<void>;
  refreshParents: (parentIds: string[]) => Promise<void>;
  refreshNodeCount: () => Promise<void>;
  onAuthGrantMessage?: (grant: AuthGrantMessageV1) => void;
  onRemoteOpsApplied: (ops: Operation[]) => Promise<void> | void;
};

export function usePlaygroundSync(opts: UsePlaygroundSyncOptions): PlaygroundSyncApi {
  const {
    client,
    status,
    docId,
    selfPeerId,
    autoSyncJoin = false,
    syncServerUrl = "",
    transportMode = "local",
    online,
    getMaxLamport,
    authEnabled,
    syncAuth,
    authError,
    joinMode,
    authNeedsInvite,
    authCanSyncAll,
    viewRootId,
    treeStateRef,
    refreshMeta,
    refreshParents,
    refreshNodeCount,
    onAuthGrantMessage,
    onRemoteOpsApplied,
  } = opts;

  const [syncBusy, setSyncBusy] = useState(false);
  const [liveBusy, setLiveBusy] = useState(false);
  const [syncError, setSyncError] = useState<string | null>(null);
  const [peers, setPeers] = useState<PeerInfo[]>([]);
  const [remoteSyncStatus, setRemoteSyncStatus] = useState<RemoteSyncStatus>({
    state: "disabled",
    detail: "Remote server transport is disabled in local tabs mode.",
  });
  const [liveChildrenParents, setLiveChildrenParents] = useState<Set<string>>(() => new Set());
  const [liveAllEnabled, setLiveAllEnabled] = useState(false);
  const [autoSyncJoinTick, bumpAutoSyncJoinTick] = useState(0);

  const onlineRef = useRef(true);
  useEffect(() => {
    onlineRef.current = online;
  }, [online]);

  const autoSyncJoinInitial = useRef(autoSyncJoin).current;

  const broadcastChannelRef = useRef<BroadcastChannel | null>(null);
  const presenceMeshRef = useRef<ReturnType<typeof createBroadcastPresenceMesh<any>> | null>(null);

  const syncPeerRef = useRef<SyncPeer<Operation> | null>(null);
  const syncConnRef = useRef<Map<string, { transport: DuplexTransport<any>; detach: () => void }>>(new Map());
  const liveChildrenParentsRef = useRef<Set<string>>(new Set());
  const liveChildSubsRef = useRef<Map<string, Map<string, SyncSubscription>>>(new Map());
  const liveAllEnabledRef = useRef(false);
  const liveAllSubsRef = useRef<Map<string, SyncSubscription>>(new Map());
  const liveAllStartingRef = useRef<Set<string>>(new Set());
  const liveChildrenStartingRef = useRef<Set<string>>(new Set());
  const liveBusyCountRef = useRef(0);
  const remoteLivePushScheduledRef = useRef(false);
  const remoteLivePushRunningRef = useRef(false);
  const autoSyncDoneRef = useRef(false);
  const autoSyncInFlightRef = useRef(false);
  const autoSyncAttemptRef = useRef(0);
  const autoSyncPeerIdRef = useRef<string | null>(null);
  const meshPeersRef = useRef<PeerInfo[]>([]);
  const remotePeerRef = useRef<PeerInfo | null>(null);

  const publishPeers = () => {
    const merged: PeerInfo[] = [...meshPeersRef.current];
    if (remotePeerRef.current) merged.push(remotePeerRef.current);
    merged.sort((a, b) => a.id.localeCompare(b.id));
    setPeers(merged);
  };

  const isRemotePeerId = (peerId: string) => peerId.startsWith("remote:");
  const syncOnceOptionsForPeer = (peerId: string, localCodewordsPerMessage: number) => ({
    maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
    maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
    codewordsPerMessage: isRemotePeerId(peerId) ? REMOTE_SYNC_CODEWORDS_PER_MESSAGE : localCodewordsPerMessage,
  });
  const syncTimeoutMsForPeer = (peerId: string, opts: { autoSync?: boolean; multipleTargets?: boolean } = {}) => {
    if (isRemotePeerId(peerId)) return PLAYGROUND_REMOTE_SYNC_TIMEOUT_MS;
    if (opts.autoSync) return PLAYGROUND_PEER_TIMEOUT_MS;
    return opts.multipleTargets ? 8_000 : 15_000;
  };

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

  const beginLiveWork = () => {
    liveBusyCountRef.current += 1;
    setLiveBusy(true);
  };

  const endLiveWork = () => {
    liveBusyCountRef.current = Math.max(0, liveBusyCountRef.current - 1);
    setLiveBusy(liveBusyCountRef.current > 0);
  };

  const startLiveAll = (peerId: string) => {
    const conn = syncConnRef.current.get(peerId);
    const peer = syncPeerRef.current;
    if (!conn || !peer) return;

    if (liveAllSubsRef.current.has(peerId)) return;
    if (liveAllStartingRef.current.has(peerId)) return;
    liveAllStartingRef.current.add(peerId);
    beginLiveWork();

    void (async () => {
      let started = false;
      const sub = peer.subscribe(
        conn.transport,
        { all: {} },
        {
          immediate: true,
          intervalMs: 0,
          ...syncOnceOptionsForPeer(peerId, 1024),
        }
      );
      liveAllSubsRef.current.set(peerId, sub);
      void sub.done.catch((err) => {
        if (!started) return;
        console.error("Live sync(all) failed", err);
        stopLiveAllForPeer(peerId);
        setSyncError(formatSyncError(err));
      });

      try {
        await sub.ready;
        started = true;
      } catch (err) {
        console.error("Live sync(all) initial catch-up failed", err);
        stopLiveAllForPeer(peerId);
        setSyncError(formatSyncError(err));
        return;
      }
    })().finally(() => {
      liveAllStartingRef.current.delete(peerId);
      endLiveWork();
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
    beginLiveWork();

    const byParent = existing ?? new Map<string, SyncSubscription>();
    void (async () => {
      let started = false;
      const sub = peer.subscribe(
        conn.transport,
        { children: { parent: hexToBytes16(parentId) } },
        {
          immediate: true,
          intervalMs: 0,
          ...syncOnceOptionsForPeer(peerId, 1024),
        }
      );
      byParent.set(parentId, sub);
      liveChildSubsRef.current.set(peerId, byParent);
      void sub.done.catch((err) => {
        if (!started) return;
        console.error("Live sync failed", err);
        stopLiveChildren(peerId, parentId);
        setSyncError(formatSyncError(err));
      });

      try {
        await sub.ready;
        started = true;
      } catch (err) {
        console.error("Live sync(children) initial catch-up failed", err);
        stopLiveChildren(peerId, parentId);
        setSyncError(formatSyncError(err));
        return;
      }
    })().finally(() => {
      liveChildrenStartingRef.current.delete(startKey);
      endLiveWork();
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
    if (remoteLivePushRunningRef.current) {
      remoteLivePushScheduledRef.current = true;
      return;
    }
    remoteLivePushScheduledRef.current = true;
    remoteLivePushRunningRef.current = true;
    beginLiveWork();
    void (async () => {
      try {
        while (remoteLivePushScheduledRef.current) {
          remoteLivePushScheduledRef.current = false;
          if (!onlineRef.current) continue;

          const peer = syncPeerRef.current;
          if (!peer) continue;

          const connections = syncConnRef.current;
          const remotePeerIds = Array.from(connections.keys()).filter(isRemotePeerId);
          if (remotePeerIds.length === 0) continue;

          const liveChildren = Array.from(liveChildrenParentsRef.current).filter((id) => /^[0-9a-f]{32}$/i.test(id));
          if (!liveAllEnabledRef.current && liveChildren.length === 0) continue;

          for (const peerId of remotePeerIds) {
            const conn = connections.get(peerId);
            if (!conn) continue;
            try {
              if (liveAllEnabledRef.current) {
                await withTimeout(
                  peer.syncOnce(conn.transport, { all: {} }, syncOnceOptionsForPeer(peerId, 1024)),
                  syncTimeoutMsForPeer(peerId, { autoSync: true }),
                  `live sync with ${peerId.slice(0, 8)}… timed out`
                );
                continue;
              }

              for (const parentId of liveChildren) {
                await withTimeout(
                  peer.syncOnce(
                    conn.transport,
                    { children: { parent: hexToBytes16(parentId) } },
                    syncOnceOptionsForPeer(peerId, 1024)
                  ),
                  syncTimeoutMsForPeer(peerId, { autoSync: true }),
                  `live sync(children ${parentId.slice(0, 8)}…) with ${peerId.slice(0, 8)}… timed out`
                );
              }
            } catch (err) {
              console.error("Remote live sync push failed", err);
              setSyncError(formatSyncError(err));
              if (!isCapabilityRevokedError(err)) dropPeerConnection(peerId);
            }
          }
        }
      } finally {
        remoteLivePushRunningRef.current = false;
        endLiveWork();
      }
    })();
  };

  const dropPeerConnection = (peerId: string) => {
    const mesh = presenceMeshRef.current;
    if (mesh && !isRemotePeerId(peerId)) {
      mesh.disconnectPeer(peerId);
      return;
    }

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
    stopLiveAllForPeer(peerId);
    stopLiveChildrenForPeer(peerId);

    if (isRemotePeerId(peerId)) {
      remotePeerRef.current = null;
    } else {
      meshPeersRef.current = meshPeersRef.current.filter((p) => p.id !== peerId);
    }
    publishPeers();
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
      let successes = 0;
      let lastErr: unknown = null;
      for (const peerId of targets) {
        const conn = connections.get(peerId);
        if (!conn) continue;
        const perPeerTimeoutMs = syncTimeoutMsForPeer(peerId, { multipleTargets: targets.length > 1 });
        try {
          await withTimeout(
            peer.syncOnce(conn.transport, filter, syncOnceOptionsForPeer(peerId, 2048)),
            perPeerTimeoutMs,
            `sync with ${peerId.slice(0, 8)}… timed out`
          );
          successes += 1;
        } catch (err) {
          lastErr = err;
          console.error("Sync failed for peer", peerId, err);
          if (!isCapabilityRevokedError(err)) dropPeerConnection(peerId);
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
      setSyncError(formatSyncError(err));
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
      let successes = 0;
      let lastErr: unknown = null;
      for (const peerId of targets) {
        const conn = connections.get(peerId);
        if (!conn) continue;
        const perPeerTimeoutMs = syncTimeoutMsForPeer(peerId, { multipleTargets: targets.length > 1 });
        try {
          for (const parentId of parentIds) {
            await withTimeout(
              peer.syncOnce(
                conn.transport,
                { children: { parent: hexToBytes16(parentId) } },
                syncOnceOptionsForPeer(peerId, 2048)
              ),
              perPeerTimeoutMs,
              `sync(children ${parentId.slice(0, 8)}…) with ${peerId.slice(0, 8)}… timed out`
            );
          }
          successes += 1;
        } catch (err) {
          lastErr = err;
          console.error("Scoped sync failed for peer", peerId, err);
          if (!isCapabilityRevokedError(err)) dropPeerConnection(peerId);
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
      setSyncError(formatSyncError(err));
    } finally {
      setSyncBusy(false);
    }
  };

  const postBroadcastMessage = (
    msg: BroadcastPresenceMessageV1 | BroadcastPresenceAckMessageV1 | AuthGrantMessageV1
  ) => {
    const channel = broadcastChannelRef.current;
    if (!channel) return false;
    channel.postMessage(msg);
    return true;
  };

  useEffect(() => {
    if (!autoSyncJoinInitial || !joinMode) return;
    if (autoSyncDoneRef.current) return;
    if (autoSyncInFlightRef.current) return;
    if (!onlineRef.current) return;
    if (syncBusy) return;

    const authReady = !authEnabled || Boolean(syncAuth);
    if (!authReady) return;

    const mesh = presenceMeshRef.current;

    let peerId = autoSyncPeerIdRef.current;
    const connections = syncConnRef.current;
    if (peerId && !connections.has(peerId)) {
      autoSyncPeerIdRef.current = null;
      peerId = null;
    }
    if (!peerId) {
      const candidates = Array.from(connections.keys());
      peerId = candidates.find((id) => !mesh || mesh.isPeerReady(id)) ?? null;
      autoSyncPeerIdRef.current = peerId;
    }
    if (!peerId) return;

    const peer = syncPeerRef.current;
    const conn = connections.get(peerId);
    if (!peer || !conn) return;

    if (!authCanSyncAll) {
      const clean = viewRootId.toLowerCase();
      if (clean === ROOT_ID || !/^[0-9a-f]{32}$/.test(clean)) return;
    }

    if (autoSyncAttemptRef.current >= 3) return;
    autoSyncAttemptRef.current += 1;
    autoSyncInFlightRef.current = true;

    void (async () => {
      setSyncBusy(true);
      setSyncError(null);
      try {
        if (authCanSyncAll) {
          await withTimeout(
            peer.syncOnce(conn.transport, { all: {} }, syncOnceOptionsForPeer(peerId, 2048)),
            syncTimeoutMsForPeer(peerId, { autoSync: true }),
            `auto sync with ${peerId.slice(0, 8)}… timed out`
          );
        } else {
          await withTimeout(
            peer.syncOnce(
              conn.transport,
              { children: { parent: hexToBytes16(viewRootId) } },
              syncOnceOptionsForPeer(peerId, 2048)
            ),
            syncTimeoutMsForPeer(peerId, { autoSync: true }),
            `auto sync(children ${viewRootId.slice(0, 8)}…) with ${peerId.slice(0, 8)}… timed out`
          );
        }

        await refreshMeta();
        const parentIds = new Set(Object.keys(treeStateRef.current.childrenByParent));
        parentIds.add(viewRootId);
        await refreshParents(Array.from(parentIds));
        await refreshNodeCount();

        autoSyncDoneRef.current = true;
        if (typeof window !== "undefined") {
          const url = new URL(window.location.href);
          url.searchParams.delete("autosync");
          window.history.replaceState({}, "", url);
        }
      } catch (err) {
        console.error("Auto sync failed", err);
        setSyncError(formatSyncError(err));
        autoSyncPeerIdRef.current = null;
        if (!isCapabilityRevokedError(err)) dropPeerConnection(peerId);
      } finally {
        autoSyncInFlightRef.current = false;
        setSyncBusy(false);
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    authEnabled,
    authCanSyncAll,
    autoSyncJoinTick,
    joinMode,
    refreshMeta,
    refreshNodeCount,
    refreshParents,
    syncAuth,
    syncBusy,
    viewRootId,
  ]);

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
    const hasBroadcastChannel = typeof BroadcastChannel !== "undefined";
    const wantsLocalMesh = transportMode !== "remote";
    const wantsRemoteSocket = transportMode !== "local";
    const configuredRemoteSyncUrl = syncServerUrl.trim();
    const hasLocalMesh = wantsLocalMesh && hasBroadcastChannel;
    const remoteSyncUrl = wantsRemoteSocket ? configuredRemoteSyncUrl : "";

    if (!wantsRemoteSocket) {
      setRemoteSyncStatus({
        state: "disabled",
        detail: "Remote server transport is disabled in local tabs mode.",
      });
    } else if (configuredRemoteSyncUrl.length === 0) {
      setRemoteSyncStatus({
        state: "missing_url",
        detail: "Enter a websocket URL to use remote transport.",
      });
    } else {
      try {
        const remoteUrl = normalizeSyncServerUrl(configuredRemoteSyncUrl, docId);
        setRemoteSyncStatus({
          state: "connecting",
          detail: `Preparing connection to ${remoteUrl.host}...`,
        });
      } catch (err) {
        setRemoteSyncStatus({
          state: "invalid",
          detail: formatSyncError(err),
        });
      }
    }

    if (!hasLocalMesh && remoteSyncUrl.length === 0) {
      if (wantsRemoteSocket && configuredRemoteSyncUrl.length === 0) {
        setSyncError("Remote transport requires a sync server URL.");
        return;
      }
      if (wantsLocalMesh && !hasBroadcastChannel) {
        setSyncError("BroadcastChannel is not available in this environment.");
        return;
      }
      setSyncError("No sync transport is configured.");
      return;
    }

    if (!authEnabled) {
      // If auth is off, clear any auth-gating error strings so the UI doesn't keep telling users to import invites.
      setSyncError((prev) =>
        prev && (prev.startsWith("Auth enabled:") || prev.startsWith("Initializing local peer key")) ? null : prev
      );
    }

    if (authEnabled && !syncAuth) {
      setSyncError(authNeedsInvite ? null : authError ?? "Auth enabled: initializing keys/tokens...");
      return;
    }

    if (!selfPeerId) {
      setSyncError("Initializing local peer key...");
      return;
    }

    setSyncError((prev) =>
      prev &&
      (
        prev.includes("initializing keys/tokens") ||
        prev.startsWith("Initializing local peer key") ||
        prev === "Remote transport requires a sync server URL." ||
        prev === "BroadcastChannel is not available in this environment." ||
        prev === "No sync transport is configured."
      )
        ? null
        : prev
    );

    const debugSync = typeof window !== "undefined" && new URLSearchParams(window.location.search).has("debugSync");

    const channel = hasLocalMesh ? new BroadcastChannel(`treecrdt-sync-v0:${docId}`) : null;
    broadcastChannelRef.current = channel;
    meshPeersRef.current = [];
    remotePeerRef.current = null;
    publishPeers();

    const baseBackend = createTreecrdtSyncBackendFromClient(client, docId, {
      enablePendingSidecar: authEnabled,
      maxLamport: getMaxLamport,
    });
    const backend = {
      ...baseBackend,
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
      ...(syncAuth
        ? {
            auth: syncAuth,
          }
        : {}),
    });
    syncPeerRef.current = sharedPeer;

    const connections = new Map<string, { transport: DuplexTransport<any>; detach: () => void }>();
    syncConnRef.current = connections;

    const maybeStartLiveForPeer = (peerId: string) => {
      if (!isRemotePeerId(peerId)) {
        const mesh = presenceMeshRef.current;
        if (!mesh || !mesh.isPeerReady(peerId)) return;
      }
      if (liveAllEnabledRef.current) startLiveAll(peerId);
      for (const parentId of liveChildrenParentsRef.current) startLiveChildren(peerId, parentId);
    };

    const mesh = channel
      ? createBroadcastPresenceMesh({
          channel,
          selfId: selfPeerId,
          codec: treecrdtSyncV0ProtobufCodec,
          isOnline: () => onlineRef.current,
          peerTimeoutMs: PLAYGROUND_PEER_TIMEOUT_MS,
          onPeersChanged: (next) => {
            meshPeersRef.current = next.map((p) => ({ id: p.id, lastSeen: p.lastSeen }));
            publishPeers();
          },
          onPeerReady: (peerId) => {
            maybeStartLiveForPeer(peerId);
            if (autoSyncJoinInitial && joinMode && !autoSyncDoneRef.current) {
              autoSyncPeerIdRef.current = peerId;
              // Ensure the auto-sync effect runs even if peer readiness toggles without changing `peers.length`.
              bumpAutoSyncJoinTick((t) => t + 1);
            }
          },
          onPeerTransport: (peerId, transport) => {
            const detach = sharedPeer.attach(transport);
            connections.set(peerId, { transport, detach });
            maybeStartLiveForPeer(peerId);
            if (autoSyncJoinInitial && joinMode && !autoSyncDoneRef.current) {
              autoSyncPeerIdRef.current = peerId;
              bumpAutoSyncJoinTick((t) => t + 1);
            }
            return detach;
          },
          onPeerDisconnected: (peerId) => {
            connections.delete(peerId);
            stopLiveAllForPeer(peerId);
            stopLiveChildrenForPeer(peerId);
            meshPeersRef.current = meshPeersRef.current.filter((p) => p.id !== peerId);
            publishPeers();
          },
          onBroadcastMessage: (data) => {
            if (!data || typeof data !== "object") return;
            const msg = data as Partial<AuthGrantMessageV1>;
            if (msg.t !== "auth_grant_v1") return;

            const grant = msg as Partial<AuthGrantMessageV1>;
            if (typeof grant.doc_id !== "string") return;
            if (grant.doc_id !== docId) return;
            if (typeof grant.to_replica_pk_hex !== "string") return;
            if (typeof grant.issuer_pk_b64 !== "string") return;
            if (typeof grant.token_b64 !== "string") return;

            const localReplicaHex = selfPeerId;
            if (!localReplicaHex) return;
            if (grant.to_replica_pk_hex.toLowerCase() !== localReplicaHex.toLowerCase()) return;

            onAuthGrantMessage?.(grant as AuthGrantMessageV1);
          },
        })
      : null;

    presenceMeshRef.current = mesh;

    let remoteSocket: WebSocket | null = null;
    let remotePeerId: string | null = null;
    let disposed = false;
    let remoteOpened = false;

    if (remoteSyncUrl.length > 0) {
      try {
        const remoteUrl = normalizeSyncServerUrl(remoteSyncUrl, docId);
        setRemoteSyncStatus({
          state: "connecting",
          detail: `Connecting to ${remoteUrl.host}...`,
        });
        remotePeerId = `remote:${remoteUrl.host}`;
        remoteSocket = new WebSocket(remoteUrl.toString());
        remoteSocket.binaryType = "arraybuffer";

        remoteSocket.addEventListener("open", () => {
          if (disposed || syncConnRef.current !== connections) return;
          if (!remoteSocket || remoteSocket.readyState !== WebSocket.OPEN || !remotePeerId) return;
          remoteOpened = true;
          setSyncError((prev) => (prev === `Remote sync socket error (${remoteUrl.host})` ? null : prev));
          setRemoteSyncStatus({
            state: "connected",
            detail: `Connected to ${remoteUrl.host}`,
          });
          const wire = createBrowserWebSocketTransport(remoteSocket);
          const transport = wrapDuplexTransportWithCodec<Uint8Array, any>(
            wire,
            treecrdtSyncV0ProtobufCodec as any
          );
          const detach = sharedPeer.attach(transport);
          syncConnRef.current.set(remotePeerId, { transport, detach });
          remotePeerRef.current = { id: remotePeerId, lastSeen: Date.now() };
          publishPeers();
          maybeStartLiveForPeer(remotePeerId);

          if (autoSyncJoinInitial && joinMode && !autoSyncDoneRef.current) {
            autoSyncPeerIdRef.current = remotePeerId;
            bumpAutoSyncJoinTick((t) => t + 1);
          }
        });

        remoteSocket.addEventListener("message", () => {
          if (disposed || syncConnRef.current !== connections) return;
          if (!remotePeerId) return;
          remotePeerRef.current = { id: remotePeerId, lastSeen: Date.now() };
          publishPeers();
        });

        remoteSocket.addEventListener("close", () => {
          if (syncConnRef.current !== connections) return;
          if (!disposed) {
            setRemoteSyncStatus({
              state: "error",
              detail: remoteOpened
                ? `Disconnected from ${remoteUrl.host}`
                : `Could not connect to ${remoteUrl.host}`,
            });
          }
          if (!remotePeerId) return;
          dropPeerConnection(remotePeerId);
        });

        remoteSocket.addEventListener("error", () => {
          if (syncConnRef.current !== connections) return;
          setRemoteSyncStatus({
            state: "error",
            detail: remoteOpened
              ? `Connection error talking to ${remoteUrl.host}`
              : `Could not reach ${remoteUrl.host}`,
          });
          setSyncError((prev) => prev ?? `Remote sync socket error (${remoteUrl.host})`);
        });
      } catch (err) {
        setRemoteSyncStatus({
          state: "invalid",
          detail: formatSyncError(err),
        });
        setSyncError(formatSyncError(err));
      }
    }

    return () => {
      disposed = true;
      stopAllLiveAll();
      stopAllLiveChildren();
      if (presenceMeshRef.current === mesh) presenceMeshRef.current = null;
      mesh?.stop();
      if (remoteSocket) {
        try {
          remoteSocket.close();
        } catch {
          // ignore
        }
      }
      if (broadcastChannelRef.current === channel) broadcastChannelRef.current = null;
      if (syncPeerRef.current === sharedPeer) syncPeerRef.current = null;
      channel?.close();
      liveAllStartingRef.current.clear();
      liveChildrenStartingRef.current.clear();
      remoteLivePushScheduledRef.current = false;
      remoteLivePushRunningRef.current = false;
      liveBusyCountRef.current = 0;
      setLiveBusy(false);
      connections.clear();
      meshPeersRef.current = [];
      remotePeerRef.current = null;
      publishPeers();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    authEnabled,
    authNeedsInvite,
    client,
    docId,
    getMaxLamport,
    joinMode,
    onAuthGrantMessage,
    onRemoteOpsApplied,
    selfPeerId,
    syncAuth,
    syncServerUrl,
    transportMode,
    status,
  ]);

  return {
    peers,
    remoteSyncStatus,
    syncBusy,
    liveBusy,
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

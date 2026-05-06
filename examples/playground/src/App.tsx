import React, { useEffect, useMemo, useRef, useState } from "react";
import { encodeImageFileContent, encodeTextContent } from "@treecrdt/content";
import { type Operation } from "@treecrdt/interface";
import type { BoundTreecrdtEngineLocal, MaterializationEvent } from "@treecrdt/interface/engine";
import { bytesToHex } from "@treecrdt/interface/ids";
import { createTreecrdtClient, type TreecrdtClient } from "@treecrdt/wa-sqlite/client";
import { detectOpfsSupport } from "@treecrdt/wa-sqlite/opfs";

import { hexToBytes16 } from "./sync-v0";
import { useVirtualizer } from "./virtualizer";

import { MAX_COMPOSER_NODE_COUNT, ROOT_ID } from "./playground/constants";
import { ComposerPanel } from "./playground/components/ComposerPanel";
import { OpsPanel } from "./playground/components/OpsPanel";
import { PlaygroundHeader } from "./playground/components/PlaygroundHeader";
import { ShareSubtreeDialog } from "./playground/components/ShareSubtreeDialog";
import { PlaygroundToast } from "./playground/components/PlaygroundToast";
import { TreePanel } from "./playground/components/TreePanel";
import { usePlaygroundAuth } from "./playground/hooks/usePlaygroundAuth";
import { usePlaygroundOpsLog } from "./playground/hooks/usePlaygroundOpsLog";
import { usePlaygroundPayloads } from "./playground/hooks/usePlaygroundPayloads";
import { usePlaygroundSync } from "./playground/hooks/usePlaygroundSync";
import { materializationRefreshPlan } from "./playground/materializationEvents";
import {
  ensureOpfsKey,
  initialDocId,
  initialStorage,
  makeDefaultDocId,
  makeNodeId,
  makeSessionKey,
  persistDocId,
  persistSyncSettings,
  persistOpfsKey,
  persistStorage,
} from "./playground/persist";
import { getPlaygroundProfileId, prefixPlaygroundStorageKey } from "./playground/storage";
import { applyChildrenLoaded, flattenForSelectState } from "./playground/treeState";
import type {
  BulkAddProgress,
  CollapseState,
  DisplayNode,
  ImagePayloadViewMetric,
  PayloadDisplay,
  Status,
  StorageMode,
  SyncTransportMode,
  TreeState,
} from "./playground/types";

const PLAYGROUND_SYNC_SERVER_URL_KEY = "treecrdt-playground-sync-server-url";
const PLAYGROUND_SYNC_TRANSPORT_MODE_KEY = "treecrdt-playground-sync-transport-mode";

function isSyncTransportMode(value: string | null): value is SyncTransportMode {
  return value === "local" || value === "remote" || value === "hybrid";
}

function initialSyncServerUrl(): string {
  if (typeof window === "undefined") return "";
  const fromQuery = new URLSearchParams(window.location.search).get("sync")?.trim();
  if (fromQuery && fromQuery.length > 0) return fromQuery;
  return window.localStorage.getItem(PLAYGROUND_SYNC_SERVER_URL_KEY) ?? "";
}

function initialSyncTransportMode(): SyncTransportMode {
  if (typeof window === "undefined") return "local";

  const params = new URLSearchParams(window.location.search);
  const fromQuery = params.get("transport")?.trim() ?? null;
  if (isSyncTransportMode(fromQuery)) return fromQuery;

  const fromStorage = window.localStorage.getItem(PLAYGROUND_SYNC_TRANSPORT_MODE_KEY);
  if (isSyncTransportMode(fromStorage)) return fromStorage;

  const fromQuerySync = params.get("sync")?.trim();
  if (fromQuerySync && fromQuerySync.length > 0) return "hybrid";

  const storedSyncUrl = window.localStorage.getItem(PLAYGROUND_SYNC_SERVER_URL_KEY)?.trim();
  if (storedSyncUrl) return "hybrid";

  return "local";
}

export default function App() {
  const [client, setClient] = useState<TreecrdtClient | null>(null);
  const clientRef = useRef<TreecrdtClient | null>(null);
  const [treeState, setTreeState] = useState<TreeState>(() => ({
    index: { [ROOT_ID]: { parentId: null, order: 0, childCount: 0 } },
    childrenByParent: { [ROOT_ID]: [] },
  }));
  const [status, setStatus] = useState<Status>("booting");
  const [error, setError] = useState<string | null>(null);
  const [headLamport, setHeadLamport] = useState(0);
  const [totalNodes, setTotalNodes] = useState<number | null>(null);
  const [docId, setDocId] = useState<string>(() => initialDocId());
  const [storage, setStorage] = useState<StorageMode>(() => initialStorage());
  const [sessionKey, setSessionKey] = useState<string>(() =>
    initialStorage() === "opfs" ? ensureOpfsKey() : makeSessionKey()
  );
  const [parentChoice, setParentChoice] = useState(ROOT_ID);
  const [collapse, setCollapse] = useState<CollapseState>(() => ({
    defaultCollapsed: true,
    overrides: new Set([ROOT_ID]),
  }));
  const [busy, setBusy] = useState(false);
  const [bulkAddProgress, setBulkAddProgress] = useState<BulkAddProgress | null>(null);
  const [nodeCount, setNodeCount] = useState(1);
  const [fanout, setFanout] = useState(10);
  const [newNodeValue, setNewNodeValue] = useState("");
  const [selectedImageFile, setSelectedImageFile] = useState<File | null>(null);
  const [showOpsPanel, setShowOpsPanel] = useState(false);
  const [showPeersPanel, setShowPeersPanel] = useState(false);
  const [syncServerUrl, setSyncServerUrl] = useState<string>(() => initialSyncServerUrl());
  const [syncTransportMode, setSyncTransportMode] = useState<SyncTransportMode>(() => initialSyncTransportMode());
  const [lastImageViewMetric, setLastImageViewMetric] = useState<ImagePayloadViewMetric | null>(null);
  const [composerOpen, setComposerOpen] = useState(() => {
    if (typeof window === "undefined") return true;
    const key = prefixPlaygroundStorageKey("treecrdt-playground-ui-composer-open");
    const stored = window.localStorage.getItem(key);
    if (stored === "0") return false;
    if (stored === "1") return true;
    return false;
  });
  const [online, setOnline] = useState(true);

  const joinMode =
    typeof window !== "undefined" && new URLSearchParams(window.location.search).get("join") === "1";
  const autoSyncJoin =
    typeof window !== "undefined" && new URLSearchParams(window.location.search).get("autosync") === "1";
  const profileId = useMemo(() => getPlaygroundProfileId(), []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const key = prefixPlaygroundStorageKey("treecrdt-playground-ui-composer-open");
    window.localStorage.setItem(key, composerOpen ? "1" : "0");
  }, [composerOpen]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const next = syncServerUrl.trim();
    if (next.length === 0) {
      window.localStorage.removeItem(PLAYGROUND_SYNC_SERVER_URL_KEY);
      return;
    }
    window.localStorage.setItem(PLAYGROUND_SYNC_SERVER_URL_KEY, next);
  }, [syncServerUrl]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(PLAYGROUND_SYNC_TRANSPORT_MODE_KEY, syncTransportMode);
  }, [syncTransportMode]);

  useEffect(() => {
    persistSyncSettings(syncServerUrl, syncTransportMode);
  }, [syncServerUrl, syncTransportMode]);

  const lamportRef = useRef(0);
  const initEpochRef = useRef(0);
  const disposedRef = useRef(false);
  const opfsSupport = useMemo(detectOpfsSupport, []);
  const {
    encryptPayloadBytes,
    payloadDisplayForNode,
    refreshDocPayloadKey,
    refreshPayloadsForNodes,
    resetPayloadCache,
    schedulePayloadEventUpdates,
  } = usePlaygroundPayloads({ docId, setError });
  const identityByReplicaRef = useRef<Map<string, { identityPk: Uint8Array; devicePk: Uint8Array }>>(new Map());
  const [, bumpIdentityVersion] = useState(0);
  const onPeerIdentityChain = React.useCallback(
    (chain: { identityPublicKey: Uint8Array; devicePublicKey: Uint8Array; replicaPublicKey: Uint8Array }) => {
      const replicaHex = bytesToHex(chain.replicaPublicKey);
      const existing = identityByReplicaRef.current.get(replicaHex);
      if (
        existing &&
        bytesToHex(existing.identityPk) === bytesToHex(chain.identityPublicKey) &&
        bytesToHex(existing.devicePk) === bytesToHex(chain.devicePublicKey)
      ) {
        return;
      }
      identityByReplicaRef.current.set(replicaHex, { identityPk: chain.identityPublicKey, devicePk: chain.devicePublicKey });
      bumpIdentityVersion((v) => v + 1);
    },
    []
  );

  const {
    authEnabled,
    setAuthEnabled,
    revealIdentity,
    setRevealIdentity,
    showAuthPanel,
    setShowAuthPanel,
    showShareDialog,
    setShowShareDialog,
    showAuthAdvanced,
    setShowAuthAdvanced,
    authInfo,
    authError,
    setAuthError,
    authBusy,
    toast,
    setToast,
    wrapKeyImportText,
    setWrapKeyImportText,
    issuerKeyBlobImportText,
    setIssuerKeyBlobImportText,
    identityKeyBlobImportText,
    setIdentityKeyBlobImportText,
    deviceSigningKeyBlobImportText,
    setDeviceSigningKeyBlobImportText,
    authMaterial,
    syncAuth,
    refreshAuthMaterial,
    resetLocalIdentityChain,
    replica,
    selfPeerId,
    viewRootId,
    authCanSyncAll,
    canWriteStructure,
    canWritePayload,
    canDelete,
    isScopedAccess,
    authCanIssue,
    authCanDelegate,
    authIssuerPkHex,
    authLocalKeyIdHex,
    authLocalTokenIdHex,
    authTokenCount,
    authTokenScope,
    authTokenActions,
    authNeedsInvite,
    hardRevokedTokenIds,
    toggleHardRevokedTokenId,
    pendingOps,
    refreshPendingOps,
    privateRoots,
    privateRootsCount,
    togglePrivateRoot,
    inviteRoot,
    inviteActions,
    setInviteActions,
    inviteAllowGrant,
    setInviteAllowGrant,
    inviteLink,
    generateInviteLink,
    issuedGrantRecords,
    grantSubtreeToReplicaPubkey: grantSubtreeToReplicaPubkeyRaw,
    resetAuth,
    openMintingPeerTab,
    openNewIsolatedPeerTab,
    openShareForNode,
    getLocalWriteOptions,
    copyToClipboard,
    onAuthGrantMessage,
  } = usePlaygroundAuth({
    docId,
    joinMode,
    client,
    syncServerUrl,
    syncTransportMode,
    onPeerIdentityChain,
    refreshDocPayloadKey,
  });

  const { ops, recordOps, resetOps } = usePlaygroundOpsLog({
    client,
    status,
    showOpsPanel,
    lamportRef,
    setHeadLamport,
    setError,
    refreshPayloadsForNodes,
  });

  const treeStateRef = useRef<TreeState>(treeState);
  useEffect(() => {
    treeStateRef.current = treeState;
  }, [treeState]);

  const payloadWriteQueueRef = useRef<Promise<void>>(Promise.resolve());
  const childrenLoadInFlightRef = useRef<Set<string>>(new Set());
  const imageColdSyncStartRef = useRef<number | null>(null);

  const ensureChildrenLoaded = React.useCallback(
    async (parentId: string, opts: { force?: boolean; nextClient?: TreecrdtClient } = {}) => {
      const active = opts.nextClient ?? clientRef.current ?? client;
      if (!active) return;

      const current = treeStateRef.current.childrenByParent;
      const loaded = Object.prototype.hasOwnProperty.call(current, parentId);
      if (loaded && !opts.force) return;

      if (childrenLoadInFlightRef.current.has(parentId)) return;
      childrenLoadInFlightRef.current.add(parentId);
      try {
        const children = await active.tree.children(parentId);
        setTreeState((prev) => applyChildrenLoaded(prev, parentId, children));
        try {
          const nodeIds = [parentId, ...children].filter((id) => id !== ROOT_ID);
          if (nodeIds.length > 0) {
            await refreshPayloadsForNodes(active, nodeIds);
          }
        } catch (err) {
          console.error("Failed to load child payloads", err);
        }
      } catch (err) {
        console.error("Failed to load children", err);
        setError("Failed to load tree children (see console)");
      } finally {
        childrenLoadInFlightRef.current.delete(parentId);
      }
    },
    [client, refreshPayloadsForNodes]
  );

  const refreshParents = React.useCallback(
    async (parentIds: Iterable<string>, opts: { nextClient?: TreecrdtClient } = {}) => {
      const active = opts.nextClient ?? clientRef.current ?? client;
      if (!active) return;

      const loadedChildren = treeStateRef.current.childrenByParent;
      const index = treeStateRef.current.index;
      const unique = new Set<string>();
      for (const id of parentIds) {
        if (Object.prototype.hasOwnProperty.call(loadedChildren, id)) unique.add(id);
      }
      const ids = Array.from(unique);
      if (ids.length === 0) return;

      try {
        const idsNeedingParent = ids.filter((id) => id !== ROOT_ID && !index[id]?.parentId);
        const [childrenResults, parentResults] = await Promise.all([
          Promise.all(ids.map((id) => active.tree.children(id).then((children) => [id, children] as const))),
          idsNeedingParent.length > 0
            ? Promise.all(
                idsNeedingParent.map((id) => active.tree.parent(id).then((p) => [id, p] as const))
              )
            : Promise.resolve([]),
        ]);
        const parentOverrides = Object.fromEntries(
          parentResults.filter(([, p]) => p !== null) as [string, string][]
        );
        setTreeState((prev) => {
          let next = prev;
          for (const [id, children] of childrenResults) {
            next = applyChildrenLoaded(next, id, children, parentOverrides[id]);
          }
          return next;
        });
      } catch (err) {
        console.error("Failed to refresh tree parents", err);
      }
    },
    [client]
  );

  const refreshNodeCount = React.useCallback(
    async (nextClient?: TreecrdtClient) => {
      const active = nextClient ?? clientRef.current ?? client;
      if (!active) return;
      try {
        const count = await active.tree.nodeCount();
        setTotalNodes(Number.isFinite(count) ? count : null);
      } catch (err) {
        console.error("Failed to refresh node count", err);
      }
    },
    [client]
  );

  const refreshMeta = React.useCallback(
    async (nextClient?: TreecrdtClient) => {
      const active = nextClient ?? clientRef.current ?? client;
      if (!active) return;
      try {
        const lamport = await active.meta.headLamport();
        lamportRef.current = Math.max(lamportRef.current, lamport);
        setHeadLamport(lamportRef.current);
      } catch (err) {
        console.error("Failed to refresh meta", err);
      }
    },
    [client]
  );

  const refreshParentsScheduledRef = useRef(false);
  const refreshParentsQueueRef = useRef<Set<string>>(new Set());
  const scheduleRefreshParents = React.useCallback(
    (parentIds: Iterable<string>) => {
      const loadedChildren = treeStateRef.current.childrenByParent;
      const queue = refreshParentsQueueRef.current;
      for (const id of parentIds) {
        if (Object.prototype.hasOwnProperty.call(loadedChildren, id)) queue.add(id);
      }
      if (queue.size === 0) return;
      if (refreshParentsScheduledRef.current) return;
      refreshParentsScheduledRef.current = true;
      setTimeout(() => {
        refreshParentsScheduledRef.current = false;
        const ids = Array.from(refreshParentsQueueRef.current);
        refreshParentsQueueRef.current.clear();
        void refreshParents(ids);
      }, 0);
    },
    [refreshParents]
  );

  const refreshNodeCountQueuedRef = useRef(false);
  const scheduleRefreshNodeCount = React.useCallback(() => {
    if (refreshNodeCountQueuedRef.current) return;
    refreshNodeCountQueuedRef.current = true;
    setTimeout(() => {
      refreshNodeCountQueuedRef.current = false;
      void refreshNodeCount();
    }, 0);
  }, [refreshNodeCount]);

  const getMaxLamport = React.useCallback(() => BigInt(lamportRef.current), []);
  const getLoadedParentIds = React.useCallback(
    () => Object.keys(treeStateRef.current.childrenByParent),
    []
  );

  const applyMaterializationEvent = React.useCallback(
    (event: MaterializationEvent) => {
      if (event.changes.length === 0) return;
      const { payloadUpdates, parentsToRefresh } = materializationRefreshPlan(event);
      schedulePayloadEventUpdates(payloadUpdates);
      scheduleRefreshParents(parentsToRefresh);
      scheduleRefreshNodeCount();
    },
    [schedulePayloadEventUpdates, scheduleRefreshNodeCount, scheduleRefreshParents]
  );

  useEffect(() => {
    if (!client) return;
    return client.onMaterialized(applyMaterializationEvent);
  }, [client, applyMaterializationEvent]);

  const markImageColdSyncStart = React.useCallback(() => {
    imageColdSyncStartRef.current = typeof performance === "undefined" ? Date.now() : performance.now();
  }, []);

  const handleImagePayloadLoaded = React.useCallback(
    (nodeId: string, payload: Extract<PayloadDisplay, { kind: "image" }>) => {
      const now = typeof performance === "undefined" ? Date.now() : performance.now();
      const start = imageColdSyncStartRef.current;
      setLastImageViewMetric({
        nodeId,
        mime: payload.mime,
        name: payload.name,
        bytes: payload.size,
        coldMs: start === null ? null : Math.max(0, now - start),
        loadedAtMs: Date.now(),
      });
    },
    []
  );

  const openNewPeerTab = () => {
    if (typeof window === "undefined") return;
    const url = new URL(window.location.href);
    url.searchParams.set("doc", docId);
    url.searchParams.set("transport", syncTransportMode);
    const remoteSync = syncServerUrl.trim();
    if (remoteSync.length > 0) {
      url.searchParams.set("sync", remoteSync);
    } else {
      url.searchParams.delete("sync");
    }
    url.searchParams.set("fresh", "1");
    url.searchParams.delete("replica");
    url.searchParams.delete("auth");
    url.hash = "";
    window.open(url.toString(), "_blank", "noopener,noreferrer");
  };

  const { index, childrenByParent } = treeState;
  const getLocalWriter = React.useCallback((): BoundTreecrdtEngineLocal | null => {
    if (!client || !replica) return null;
    return client.local.forReplica(replica, getLocalWriteOptions());
  }, [client, getLocalWriteOptions, replica]);

  const {
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
    queueLocalOpsForSync,
    handleSync,
    handleScopedSync,
    postBroadcastMessage,
  } = usePlaygroundSync({
    client,
    status,
    docId,
    selfPeerId,
    autoSyncJoin,
    syncServerUrl,
    transportMode: syncTransportMode,
    online,
    getMaxLamport,
    authEnabled,
    authMaterial,
    // Pass the prepared auth object through so sync does not rebuild auth state from raw material.
    syncAuth,
    authError,
    joinMode,
    authCanSyncAll,
    viewRootId,
    getLoadedParentIds,
    refreshMeta,
    onAuthGrantMessage,
    onRemoteOpsImported: recordOps,
  });

  const handleCommittedLocalOps = React.useCallback(
    (ops: Operation[]) => {
      // The local write already committed. This only feeds playground sync and debug UI state.
      queueLocalOpsForSync(ops);
      recordOps(ops, { assumeSorted: true });
    },
    [queueLocalOpsForSync, recordOps]
  );

  const grantSubtreeToReplicaPubkey = React.useCallback(
    async (opts?: {
      recipientKey?: string;
      rootNodeId?: string;
      actions?: string[];
      supersedesTokenIds?: string[];
    }) => {
      return await grantSubtreeToReplicaPubkeyRaw(postBroadcastMessage, opts);
    },
    [grantSubtreeToReplicaPubkeyRaw, postBroadcastMessage]
  );

  useEffect(() => {
    if (viewRootId === ROOT_ID) return;
    setCollapse((prev) => {
      if (!prev.defaultCollapsed) return prev;
      if (prev.overrides.has(viewRootId)) return prev;
      const overrides = new Set(prev.overrides);
      overrides.add(viewRootId);
      return { ...prev, overrides };
    });
    setParentChoice((prev) => (prev === ROOT_ID ? viewRootId : prev));
    void ensureChildrenLoaded(viewRootId);
  }, [ensureChildrenLoaded, viewRootId]);

  const nodeLabelForId = React.useCallback(
    (id: string) => payloadDisplayForNode(id).label,
    [payloadDisplayForNode]
  );

  const nodeList = useMemo(
    () => flattenForSelectState(childrenByParent, nodeLabelForId, { rootId: viewRootId }),
    [childrenByParent, nodeLabelForId, viewRootId]
  );

  const expandPathTo = React.useCallback(
    (nodeId: string) => {
      setCollapse((prev) => {
        const overrides = new Set(prev.overrides);
        const setExpanded = (id: string) => {
          if (prev.defaultCollapsed) overrides.add(id);
          else overrides.delete(id);
        };
        setExpanded(nodeId);
        let cur = index[nodeId]?.parentId ?? null;
        while (cur) {
          setExpanded(cur);
          cur = index[cur]?.parentId ?? null;
        }
        return { ...prev, overrides };
      });
    },
    [index]
  );

  const visibleNodes = useMemo(() => {
    const acc: Array<{ node: DisplayNode; depth: number }> = [];
    const isCollapsed = (id: string) => {
      return collapse.defaultCollapsed ? !collapse.overrides.has(id) : collapse.overrides.has(id);
    };
    const stack: Array<{ id: string; depth: number }> = [{ id: viewRootId, depth: 0 }];
    while (stack.length > 0) {
      const entry = stack.pop();
      if (!entry) break;
      const payload = payloadDisplayForNode(entry.id);
      acc.push({ node: { id: entry.id, label: payload.label, value: payload.value, payload }, depth: entry.depth });
      if (isCollapsed(entry.id)) continue;
      const kids = childrenByParent[entry.id] ?? [];
      for (let i = kids.length - 1; i >= 0; i--) {
        stack.push({ id: kids[i]!, depth: entry.depth + 1 });
      }
    }
    return acc;
  }, [childrenByParent, collapse, payloadDisplayForNode]);
  const treeParentRef = useRef<HTMLDivElement | null>(null);
  const opsParentRef = useRef<HTMLDivElement | null>(null);
  const treeEstimateSize = React.useCallback(() => 72, []);
  const opsEstimateSize = React.useCallback(() => 96, []);
  const getTreeScrollElement = React.useCallback(() => treeParentRef.current, []);
  const getOpsScrollElement = React.useCallback(() => opsParentRef.current, []);
  const treeItemKey = React.useCallback(
    (index: number) => visibleNodes[index]?.node.id ?? index,
    [visibleNodes]
  );
  const opsItemKey = React.useCallback(
    (index: number) => {
      const op = ops[index];
      return op ? `${op.meta.id.counter}-${op.meta.lamport}-${index}` : index;
    },
    [ops]
  );
  const treeVirtualizer = useVirtualizer({
    count: visibleNodes.length,
    getScrollElement: getTreeScrollElement,
    estimateSize: treeEstimateSize,
    overscan: 12,
    getItemKey: treeItemKey,
  });
  const opsVirtualizer = useVirtualizer({
    count: ops.length,
    getScrollElement: getOpsScrollElement,
    estimateSize: opsEstimateSize,
    overscan: 12,
    getItemKey: opsItemKey,
  });

  useEffect(() => {
    persistStorage(storage);
    void resetAndInit(storage);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [storage]);

  useEffect(() => {
    persistDocId(docId);
  }, [docId]);

  const closeClientSafely = React.useCallback(async (closingClient: TreecrdtClient | null) => {
    if (!closingClient?.close) return;
    try {
      await closingClient.close();
    } catch {
      // Client teardown is best-effort. HMR/remount/reset can race prior close calls.
    }
  }, []);

  useEffect(() => {
    disposedRef.current = false;
    return () => {
      disposedRef.current = true;
      initEpochRef.current += 1;
      const closingClient = clientRef.current;
      clientRef.current = null;
      void closeClientSafely(closingClient);
    };
  }, [closeClientSafely]);

  const initClient = async (storageMode: StorageMode, keyOverride?: string, docIdOverride?: string) => {
    const initEpoch = ++initEpochRef.current;
    setStatus("booting");
    setError(null);
    try {
      const resolvedBase =
        typeof window !== "undefined"
          ? new URL(import.meta.env.BASE_URL ?? "./", window.location.href).href
          : import.meta.env.BASE_URL ?? "./";
      const baseUrl = resolvedBase.endsWith("/") ? resolvedBase : `${resolvedBase}/`;
      const filename = storageMode === "opfs" ? `/treecrdt-playground-${keyOverride ?? sessionKey}.db` : undefined;
      const c = await createTreecrdtClient({
        storage:
          storageMode === "opfs"
            ? { type: "opfs", filename, fallback: "throw" }
            : { type: "memory" },
        runtime: { type: "auto" },
        assets: { baseUrl },
        docId: docIdOverride ?? docId,
      });
      if (disposedRef.current || initEpoch !== initEpochRef.current) {
        await closeClientSafely(c);
        return;
      }
      clientRef.current = c;
      setClient(c);
      setStorage(c.storage);
      await refreshMeta(c);
      await ensureChildrenLoaded(ROOT_ID, { nextClient: c, force: true });
      await refreshNodeCount(c);
      setStatus("ready");
    } catch (err) {
      console.error("Failed to init wa-sqlite", err);
      setError("Failed to initialize wa-sqlite (see console for details)");
      setStatus("error");
    }
  };

  const resetAndInit = async (target: StorageMode, opts: { resetKey?: boolean; docId?: string } = {}) => {
    setStatus("booting");
    const nextKey =
      target === "opfs"
        ? opts.resetKey
          ? persistOpfsKey(makeSessionKey())
          : ensureOpfsKey()
        : sessionKey;
    setSessionKey(nextKey);
    resetOps();
    setTreeState({
      index: { [ROOT_ID]: { parentId: null, order: 0, childCount: 0 } },
      childrenByParent: { [ROOT_ID]: [] },
    });
    resetPayloadCache();
    imageColdSyncStartRef.current = null;
    setLastImageViewMetric(null);
    setCollapse({ defaultCollapsed: true, overrides: new Set([ROOT_ID]) });
    lamportRef.current = 0;
    setHeadLamport(0);
    setTotalNodes(null);
    setParentChoice(ROOT_ID);
    setNewNodeValue("");
    setSelectedImageFile(null);
    setBulkAddProgress(null);
    setError(null);
    const closingClient = clientRef.current;
    clientRef.current = null;
    setClient(null);
    await closeClientSafely(closingClient);
    await initClient(target, nextKey, opts.docId);
  };

  const appendMoveAfter = async (nodeId: string, newParent: string, after: string | null) => {
    const localWriter = getLocalWriter();
    if (!localWriter) return;
    if (authEnabled && (!canWriteStructure || (isScopedAccess && newParent === ROOT_ID))) return;
    setBusy(true);
    try {
      const placement = after ? { type: "after" as const, after } : { type: "first" as const };
      const op = await localWriter.move(nodeId, newParent, placement);
      handleCommittedLocalOps([op]);
    } catch (err) {
      console.error("Failed to append move op", err);
      setError("Failed to move node (see console)");
    } finally {
      setBusy(false);
    }
  };

  const handleAddNodes = async (
    parentId: string,
    count: number,
    opts: { fanout?: number; imageFile?: File | null } = {}
  ) => {
    const localWriter = getLocalWriter();
    if (!localWriter) return;
    if (authEnabled && !canWriteStructure) return;
    if (opts.imageFile && !canWritePayload) return;
    const normalizedCount = opts.imageFile ? 1 : Math.max(0, Math.min(MAX_COMPOSER_NODE_COUNT, Math.floor(count)));
    if (normalizedCount <= 0) return;
    setBusy(true);
    const startedAtMs = Date.now();
    const progressStep = normalizedCount >= 1_000 ? 50 : normalizedCount >= 200 ? 20 : normalizedCount >= 50 ? 5 : 1;
    setBulkAddProgress({ total: normalizedCount, completed: 0, phase: "creating", startedAtMs });
    const ops: Operation[] = [];
    let opsRecorded = false;
    try {
      const fanoutLimit = opts.imageFile ? 0 : Math.max(0, Math.floor(opts.fanout ?? fanout));
      const imagePayload = opts.imageFile ? await encodeImageFileContent(opts.imageFile) : null;
      const valueBase = canWritePayload && !opts.imageFile ? newNodeValue.trim() : "";
      const shouldSetValue = canWritePayload && valueBase.length > 0;

      if (fanoutLimit <= 0) {
        for (let i = 0; i < normalizedCount; i++) {
          const nodeId = makeNodeId();
          const value = normalizedCount > 1 ? `${valueBase} ${i + 1}` : valueBase;
          const payload = imagePayload ?? (shouldSetValue ? encodeTextContent(value) : null);
          const encryptedPayload = await encryptPayloadBytes(payload);
          ops.push(await localWriter.insert(parentId, nodeId, { type: "last" }, encryptedPayload));
          const completed = i + 1;
          if (completed === normalizedCount || completed % progressStep === 0) {
            setBulkAddProgress((prev) =>
              prev ? { ...prev, completed } : prev
            );
          }
        }
      } else {
        const expanded = new Set<string>();
        const queue: string[] = [parentId];
        const childCountByParent = new Map<string, number>();

        const getChildCount = (id: string) => {
          const existing = childCountByParent.get(id);
          if (typeof existing === "number") return existing;
          return (childrenByParent[id] ?? []).length;
        };

        const setChildCount = (id: string, nextCount: number) => {
          childCountByParent.set(id, nextCount);
        };

        const ensureExpanded = (id: string) => {
          if (expanded.has(id)) return;
          expanded.add(id);
          for (const childId of childrenByParent[id] ?? []) {
            queue.push(childId);
          }
        };

        for (let i = 0; i < normalizedCount; i++) {
          while (queue.length > 0) {
            const candidate = queue[0];
            ensureExpanded(candidate);
            const childCount = getChildCount(candidate);
            if (childCount < fanoutLimit) break;
            queue.shift();
          }

          const targetParent = queue[0] ?? parentId;
          const childCount = getChildCount(targetParent);

          const nodeId = makeNodeId();
          const value = normalizedCount > 1 ? `${valueBase} ${i + 1}` : valueBase;
          const payload = shouldSetValue ? encodeTextContent(value) : null;
          const encryptedPayload = await encryptPayloadBytes(payload);
          ops.push(await localWriter.insert(targetParent, nodeId, { type: "last" }, encryptedPayload));

          setChildCount(targetParent, childCount + 1);
          queue.push(nodeId);
          const completed = i + 1;
          if (completed === normalizedCount || completed % progressStep === 0) {
            setBulkAddProgress((prev) =>
              prev ? { ...prev, completed } : prev
            );
          }
        }
      }

      setBulkAddProgress((prev) =>
        prev ? { ...prev, completed: normalizedCount, phase: "applying" } : prev
      );

      handleCommittedLocalOps(ops);
      opsRecorded = true;
      expandPathTo(parentId);
    } catch (err) {
      if (!opsRecorded && ops.length > 0) handleCommittedLocalOps(ops);
      console.error("Failed to add nodes", err);
      setError(err instanceof Error ? err.message : "Failed to add nodes (see console)");
    } finally {
      setBulkAddProgress(null);
      setBusy(false);
    }
  };

  const handleInsert = async (parentId: string) => {
    const localWriter = getLocalWriter();
    if (!localWriter) return;
    if (authEnabled && !canWriteStructure) return;
    setBusy(true);
    try {
      const valueBase = canWritePayload ? newNodeValue.trim() : "";
      const payload = valueBase.length > 0 ? encodeTextContent(valueBase) : null;
      const encryptedPayload = await encryptPayloadBytes(payload);
      const nodeId = makeNodeId();
      const op = await localWriter.insert(parentId, nodeId, { type: "last" }, encryptedPayload);
      handleCommittedLocalOps([op]);
      if (!Object.prototype.hasOwnProperty.call(treeStateRef.current.childrenByParent, parentId)) {
        await ensureChildrenLoaded(parentId, { force: true });
      }
      expandPathTo(parentId);
    } catch (err) {
      console.error("Failed to insert node", err);
      setError("Failed to insert node (see console)");
    } finally {
      setBusy(false);
    }
  };

  const handleSetValue = (nodeId: string, value: string): Promise<void> => {
    const run = payloadWriteQueueRef.current
      .catch(() => undefined)
      .then(async () => {
        if (nodeId === ROOT_ID) return;
        const localWriter = getLocalWriter();
        if (!localWriter) return;
        try {
          const payload = value.trim().length === 0 ? null : encodeTextContent(value);
          const encryptedPayload = await encryptPayloadBytes(payload);
          const op = await localWriter.payload(nodeId, encryptedPayload);
          handleCommittedLocalOps([op]);
        } catch (err) {
          console.error("Failed to write payload", err);
          setError("Failed to write payload (see console)");
        }
      });
    payloadWriteQueueRef.current = run.catch(() => undefined);
    return run;
  };

  const handleSetImagePayload = (nodeId: string, file: File): Promise<void> => {
    const run = payloadWriteQueueRef.current
      .catch(() => undefined)
      .then(async () => {
        if (nodeId === ROOT_ID || !canWritePayload) return;
        const localWriter = getLocalWriter();
        if (!localWriter) return;
        try {
          const payload = await encodeImageFileContent(file);
          const encryptedPayload = await encryptPayloadBytes(payload);
          const op = await localWriter.payload(nodeId, encryptedPayload);
          handleCommittedLocalOps([op]);
        } catch (err) {
          console.error("Failed to write image payload", err);
          setError(err instanceof Error ? err.message : "Failed to write image payload (see console)");
        }
      });
    payloadWriteQueueRef.current = run.catch(() => undefined);
    return run;
  };

  const handleClearPayload = (nodeId: string): Promise<void> => {
    const run = payloadWriteQueueRef.current
      .catch(() => undefined)
      .then(async () => {
        if (nodeId === ROOT_ID || !canWritePayload) return;
        const localWriter = getLocalWriter();
        if (!localWriter) return;
        try {
          const op = await localWriter.payload(nodeId, null);
          handleCommittedLocalOps([op]);
        } catch (err) {
          console.error("Failed to clear payload", err);
          setError("Failed to clear payload (see console)");
        }
      });
    payloadWriteQueueRef.current = run.catch(() => undefined);
    return run;
  };

  const handleDelete = async (nodeId: string) => {
    const localWriter = getLocalWriter();
    if (nodeId === ROOT_ID || !localWriter) return;
    setBusy(true);
    try {
      const op = await localWriter.delete(nodeId);
      handleCommittedLocalOps([op]);
    } catch (err) {
      console.error("Failed to delete node", err);
      setError("Failed to delete node (see console)");
    } finally {
      setBusy(false);
    }
  };

  const handleMove = async (nodeId: string, direction: "up" | "down") => {
    const meta = index[nodeId];
    if (!meta || meta.parentId === null) return;
    const siblings = childrenByParent[meta.parentId] ?? [];
    const currentIdx = siblings.indexOf(nodeId);
    if (currentIdx === -1) return;
    const targetIdx = direction === "up" ? currentIdx - 1 : currentIdx + 1;
    if (targetIdx < 0 || targetIdx >= siblings.length) return;
    const without = siblings.filter((id) => id !== nodeId);
    const after = targetIdx <= 0 ? null : without[targetIdx - 1] ?? null;
    await appendMoveAfter(nodeId, meta.parentId, after);
  };

  const handleMoveToRoot = async (nodeId: string) => {
    if (nodeId === ROOT_ID) return;
    if (authEnabled && (!canWriteStructure || isScopedAccess)) return;
    const siblings = childrenByParent[ROOT_ID] ?? [];
    const without = siblings.filter((id) => id !== nodeId);
    const after = without.length === 0 ? null : without[without.length - 1]!;
    await appendMoveAfter(nodeId, ROOT_ID, after);
  };

  const handleReset = async () => {
    setLiveChildrenParents(new Set());
    setLiveAllEnabled(false);
    await resetAndInit(storage, { resetKey: true });
  };

  const handleNewDoc = async () => {
    if (typeof window !== "undefined") {
      const url = new URL(window.location.href);
      url.searchParams.set("doc", makeDefaultDocId());
      url.searchParams.delete("join");
      url.searchParams.delete("autosync");
      url.hash = "";
      window.history.replaceState({}, "", url);
      const nextDocId = url.searchParams.get("doc")!;
      setDocId(nextDocId);
      setLiveChildrenParents(new Set());
      setLiveAllEnabled(false);
      await resetAndInit(storage, { resetKey: true, docId: nextDocId });
      return;
    }

    const nextDocId = makeDefaultDocId();
    setDocId(nextDocId);
    setLiveChildrenParents(new Set());
    setLiveAllEnabled(false);
    await resetAndInit(storage, { resetKey: true, docId: nextDocId });
  };

  const handleStorageToggle = (next: StorageMode) => {
    if (next === storage) {
      void handleReset();
      return;
    }
    setStorage(next);
  };

  const toggleCollapse = (id: string) => {
    const currentlyCollapsed = collapse.defaultCollapsed ? !collapse.overrides.has(id) : collapse.overrides.has(id);
    if (currentlyCollapsed) {
      void ensureChildrenLoaded(id);
      // For scoped tokens, expanding a node should opportunistically sync its children.
      if (!authCanSyncAll) void handleSync({ children: { parent: hexToBytes16(id) } });
    }
    setCollapse((prev) => {
      const overrides = new Set(prev.overrides);
      const currentlyCollapsed = prev.defaultCollapsed ? !overrides.has(id) : overrides.has(id);
      const nextCollapsed = !currentlyCollapsed;
      const differsFromDefault = nextCollapsed !== prev.defaultCollapsed;
      if (differsFromDefault) overrides.add(id);
      else overrides.delete(id);
      return { ...prev, overrides };
    });
  };

  const expandAll = () => setCollapse({ defaultCollapsed: false, overrides: new Set() });
  const collapseAll = () => setCollapse({ defaultCollapsed: true, overrides: new Set([viewRootId]) });

  const selfPeerIdShort = selfPeerId
    ? selfPeerId.length > 20
      ? `${selfPeerId.slice(0, 8)}…${selfPeerId.slice(-6)}`
      : selfPeerId
    : null;
  const canManageCapabilities = authEnabled && (authCanIssue || authCanDelegate);
  const localReplicaHex = selfPeerId;

  return (
    <div className="mx-auto max-w-6xl px-4 pb-12 pt-8 space-y-6">
      <ShareSubtreeDialog
        open={showShareDialog}
        onClose={() => setShowShareDialog(false)}
        busy={busy}
        inviteRoot={inviteRoot}
        nodeLabelForId={nodeLabelForId}
        authEnabled={authEnabled}
        authBusy={authBusy}
        authCanIssue={authCanIssue}
        authCanDelegate={authCanDelegate}
        onEnableAuth={() => setAuthEnabled(true)}
        openMintingPeerTab={openMintingPeerTab}
        authInfo={authInfo}
        authError={authError}
        inviteActions={inviteActions}
        setInviteActions={setInviteActions}
        inviteAllowGrant={inviteAllowGrant}
        setInviteAllowGrant={setInviteAllowGrant}
        openNewIsolatedPeerTab={openNewIsolatedPeerTab}
        generateInviteLink={generateInviteLink}
        inviteLink={inviteLink}
      />

      <PlaygroundHeader
        status={status}
        storage={storage}
        opfsAvailable={opfsSupport.available}
        joinMode={joinMode}
        profileId={profileId}
        selfPeerId={selfPeerId}
        selfPeerIdShort={selfPeerIdShort}
        onCopyPubkey={() =>
          void (selfPeerId ? copyToClipboard(selfPeerId) : Promise.resolve()).catch((err) =>
            setSyncError(err instanceof Error ? err.message : String(err))
          )
        }
        onSelectStorage={handleStorageToggle}
        onNewDoc={handleNewDoc}
        onReset={handleReset}
        onExpandAll={expandAll}
        onCollapseAll={collapseAll}
        error={error}
      />

      <div className="grid min-w-0 gap-6 md:grid-cols-3">
        <section className={`${showOpsPanel ? "md:col-span-2" : "md:col-span-3"} min-w-0 space-y-4`}>
          <ComposerPanel
            composerOpen={composerOpen}
            setComposerOpen={setComposerOpen}
            nodeList={nodeList}
            parentChoice={parentChoice}
            setParentChoice={setParentChoice}
            newNodeValue={newNodeValue}
            setNewNodeValue={setNewNodeValue}
            selectedImageFile={selectedImageFile}
            setSelectedImageFile={setSelectedImageFile}
            nodeCount={nodeCount}
            setNodeCount={setNodeCount}
            maxNodeCount={MAX_COMPOSER_NODE_COUNT}
            fanout={fanout}
            setFanout={setFanout}
            onAddNodes={handleAddNodes}
            ready={status === "ready"}
            busy={busy}
            bulkAddProgress={bulkAddProgress}
            canWritePayload={canWritePayload}
            canWriteStructure={canWriteStructure}
          />

          <TreePanel
            totalNodes={totalNodes}
            loadedNodes={Math.max(0, nodeList.length - 1)}
            privateRootsCount={privateRootsCount}
            online={online}
            setOnline={setOnline}
            ready={status === "ready"}
            busy={busy}
            syncBusy={syncBusy}
            liveBusy={liveBusy}
            peerCount={peers.length}
            authCanSyncAll={authCanSyncAll}
            onSync={() => {
              markImageColdSyncStart();
              void (authCanSyncAll ? handleSync({ all: {} }) : handleScopedSync());
            }}
            liveAllEnabled={liveAllEnabled}
            setLiveAllEnabled={setLiveAllEnabled}
            showPeersPanel={showPeersPanel}
            setShowPeersPanel={setShowPeersPanel}
            showAuthPanel={showAuthPanel}
            setShowAuthPanel={setShowAuthPanel}
            authEnabled={authEnabled}
            openNewPeerTab={openNewPeerTab}
            openNewIsolatedPeerTab={openNewIsolatedPeerTab}
            authCanIssue={authCanIssue}
            authCanDelegate={authCanDelegate}
            showOpsPanel={showOpsPanel}
            setShowOpsPanel={setShowOpsPanel}
            syncError={syncError}
            peersPanelProps={{
              online,
              setOnline,
              syncTransportMode,
              setSyncTransportMode,
              syncServerUrl,
              setSyncServerUrl,
              remoteSyncStatus,
              peers,
            }}
            sharingAuthPanelProps={{
              docId,
              authEnabled,
              setAuthEnabled,
              authBusy,
              resetAuth,
              authNeedsInvite,
              authError,
              authInfo,
              setAuthError,
              authCanIssue,
              authCanDelegate,
              authIssuerPkHex,
              authLocalKeyIdHex,
              authLocalTokenIdHex,
              authTokenCount,
              authTokenScope,
              authTokenActions,
              nodeLabelForId,
              selfPeerId,
              revealIdentity,
              setRevealIdentity,
              openMintingPeerTab,
              showAuthAdvanced,
              setShowAuthAdvanced,
              copyToClipboard,
              refreshAuthMaterial,
              refreshPendingOps,
              client,
              pendingOps,
              wrapKeyImportText,
              setWrapKeyImportText,
              issuerKeyBlobImportText,
              setIssuerKeyBlobImportText,
              identityKeyBlobImportText,
              setIdentityKeyBlobImportText,
              deviceSigningKeyBlobImportText,
              setDeviceSigningKeyBlobImportText,
              resetLocalIdentityChain,
            }}
            treeParentRef={treeParentRef}
            treeVirtualizer={treeVirtualizer}
            visibleNodes={visibleNodes}
            collapse={collapse}
            toggleCollapse={toggleCollapse}
            openShareForNode={openShareForNode}
            grantSubtreeToReplicaPubkey={grantSubtreeToReplicaPubkey}
            onSetValue={handleSetValue}
            onSetImagePayload={handleSetImagePayload}
            onClearPayload={handleClearPayload}
            onImagePayloadLoaded={handleImagePayloadLoaded}
            onAddChild={(id) => {
              setParentChoice(id);
              void handleInsert(id);
            }}
            onDelete={handleDelete}
            onMove={handleMove}
            onMoveToRoot={handleMoveToRoot}
            onToggleLiveChildren={toggleLiveChildren}
            privateRoots={privateRoots}
            togglePrivateRoot={togglePrivateRoot}
            peers={peers}
            selfPeerId={selfPeerId}
            canManageCapabilities={canManageCapabilities}
            authBusy={authBusy}
            issuedGrantRecords={issuedGrantRecords}
            hardRevokedTokenIds={hardRevokedTokenIds}
            toggleHardRevokedTokenId={toggleHardRevokedTokenId}
            scopeRootId={viewRootId}
            canWritePayload={canWritePayload}
            canWriteStructure={canWriteStructure}
            canDelete={canDelete}
            liveChildrenParents={liveChildrenParents}
            meta={index}
            childrenByParent={childrenByParent}
            imagePayloadMetric={lastImageViewMetric}
          />
        </section>

        {showOpsPanel && (
          <OpsPanel
            ops={ops}
            headLamport={headLamport}
            authEnabled={authEnabled}
            localReplicaHex={localReplicaHex}
            getIdentityByReplicaHex={(replicaHex) => identityByReplicaRef.current.get(replicaHex)}
            opsParentRef={opsParentRef}
            opsVirtualizer={opsVirtualizer}
          />
        )}
      </div>

      <PlaygroundToast
        toast={toast}
        setToast={setToast}
        onSync={() => {
          markImageColdSyncStart();
          void (authCanSyncAll ? handleSync({ all: {} }) : handleScopedSync());
        }}
        canSync={status === "ready" && !busy && !syncBusy && peers.length > 0 && online}
        onDetails={() => setShowAuthPanel(true)}
      />
    </div>
  );
}

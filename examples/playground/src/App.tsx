import React, { useEffect, useMemo, useRef, useState } from "react";
import { type Operation, type OperationKind } from "@treecrdt/interface";
import { bytesToHex } from "@treecrdt/interface/ids";
import { createTreecrdtClient, type TreecrdtClient } from "@treecrdt/wa-sqlite/client";
import { detectOpfsSupport } from "@treecrdt/wa-sqlite/opfs";
import { base64urlDecode } from "@treecrdt/auth";
import { encryptTreecrdtPayloadV1, maybeDecryptTreecrdtPayloadV1 } from "@treecrdt/crypto";

import { loadOrCreateDocPayloadKeyB64 } from "./auth";
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
import { usePlaygroundSync } from "./playground/hooks/usePlaygroundSync";
import { compareOps, mergeSortedOps, opKey } from "./playground/ops";
import { compareOpMeta } from "./playground/payload";
import {
  ensureOpfsKey,
  initialDocId,
  initialStorage,
  makeNodeId,
  makeSessionKey,
  persistDocId,
  persistOpfsKey,
  persistStorage,
} from "./playground/persist";
import { getPlaygroundProfileId, prefixPlaygroundStorageKey } from "./playground/storage";
import { applyChildrenLoaded, flattenForSelectState, parentsAffectedByOps } from "./playground/treeState";
import type {
  CollapseState,
  DisplayNode,
  PayloadRecord,
  Status,
  StorageMode,
  TreeState,
} from "./playground/types";

export default function App() {
  const [client, setClient] = useState<TreecrdtClient | null>(null);
  const clientRef = useRef<TreecrdtClient | null>(null);
  const [ops, setOps] = useState<Operation[]>([]);
  const [treeState, setTreeState] = useState<TreeState>(() => ({
    index: { [ROOT_ID]: { parentId: null, order: 0, childCount: 0, deleted: false } },
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
  const [nodeCount, setNodeCount] = useState(1);
  const [fanout, setFanout] = useState(10);
  const [newNodeValue, setNewNodeValue] = useState("");
  const [showOpsPanel, setShowOpsPanel] = useState(false);
  const [showPeersPanel, setShowPeersPanel] = useState(false);
  const [composerOpen, setComposerOpen] = useState(() => {
    if (typeof window === "undefined") return true;
    const key = prefixPlaygroundStorageKey("treecrdt-playground-ui-composer-open");
    const stored = window.localStorage.getItem(key);
    if (stored === "0") return false;
    if (stored === "1") return true;
    return false;
  });
  const [online, setOnline] = useState(true);
  const [payloadVersion, setPayloadVersion] = useState(0);

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

  const counterRef = useRef(0);
  const lamportRef = useRef(0);
  const opfsSupport = useMemo(detectOpfsSupport, []);
  const docPayloadKeyRef = useRef<Uint8Array | null>(null);
  const refreshDocPayloadKey = React.useCallback(async () => {
    const keyB64 = await loadOrCreateDocPayloadKeyB64(docId);
    docPayloadKeyRef.current = base64urlDecode(keyB64);
    return docPayloadKeyRef.current;
  }, [docId]);

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
    refreshAuthMaterial,
    localIdentityChainPromiseRef,
    getLocalIdentityChain,
    authToken,
    replica,
    selfPeerId,
    authActionSet,
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
    pendingOps,
    refreshPendingOps,
    privateRoots,
    privateRootsCount,
    showPrivateRootsPanel,
    setShowPrivateRootsPanel,
    togglePrivateRoot,
    clearPrivateRoots,
    inviteRoot,
    setInviteRoot,
    inviteMaxDepth,
    setInviteMaxDepth,
    inviteActions,
    setInviteActions,
    inviteAllowGrant,
    setInviteAllowGrant,
    invitePreset,
    setInvitePreset,
    showInviteOptions,
    setShowInviteOptions,
    inviteExcludeNodeIds,
    inviteLink,
    generateInviteLink,
    applyInvitePreset,
    invitePanelRef,
    inviteImportText,
    setInviteImportText,
    importInviteLink,
    grantRecipientKey,
    setGrantRecipientKey,
    grantSubtreeToReplicaPubkey: grantSubtreeToReplicaPubkeyRaw,
    resetAuth,
    openMintingPeerTab,
    openNewIsolatedPeerTab,
    openShareForNode,
    verifyLocalOps,
    copyToClipboard,
    onAuthGrantMessage,
  } = usePlaygroundAuth({ docId, joinMode, client, refreshDocPayloadKey });

  const showOpsPanelRef = useRef(false);
  const textEncoder = useMemo(() => new TextEncoder(), []);
  const textDecoder = useMemo(() => new TextDecoder(), []);

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

  useEffect(() => {
    docPayloadKeyRef.current = null;
    let cancelled = false;
    void (async () => {
      try {
        const keyB64 = await loadOrCreateDocPayloadKeyB64(docId);
        if (cancelled) return;
        docPayloadKeyRef.current = base64urlDecode(keyB64);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : String(err));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [docId]);

  const replicaKey = useMemo(
    () => (replica: Operation["meta"]["id"]["replica"]) => bytesToHex(replica),
    []
  );

  const payloadByNodeRef = useRef<Map<string, PayloadRecord>>(new Map());

  const requireDocPayloadKey = React.useCallback(async (): Promise<Uint8Array> => {
    if (docPayloadKeyRef.current) return docPayloadKeyRef.current;
    const next = await refreshDocPayloadKey();
    if (!next) throw new Error("doc payload key is missing");
    return next;
  }, [refreshDocPayloadKey]);

  const ingestPayloadOps = React.useCallback(
    async (incoming: Operation[]) => {
      if (incoming.length === 0) return;
      const payloads = payloadByNodeRef.current;
      let changed = false;

      for (const op of incoming) {
        const kind = op.kind;
        const node = kind.type === "payload" ? kind.node : kind.type === "insert" ? kind.node : null;
        const payload =
          kind.type === "payload" ? kind.payload : kind.type === "insert" ? kind.payload : undefined;
        if (!node || payload === undefined) continue;

        const meta = {
          lamport: op.meta.lamport,
          replica: replicaKey(op.meta.id.replica),
          counter: op.meta.id.counter,
        };

        const existing = payloads.get(node);
        if (existing && compareOpMeta(meta, existing) <= 0) continue;

        if (payload === null) {
          payloads.set(node, { ...meta, payload: null, encrypted: false });
          changed = true;
          continue;
        }

        try {
          const key = await requireDocPayloadKey();
          const res = await maybeDecryptTreecrdtPayloadV1({ docId, payloadKey: key, bytes: payload });
          payloads.set(node, { ...meta, payload: res.plaintext, encrypted: res.encrypted });
          changed = true;
        } catch {
          payloads.set(node, { ...meta, payload: null, encrypted: true });
          changed = true;
        }
      }

      if (changed) setPayloadVersion((v) => v + 1);
    },
    [docId, replicaKey, requireDocPayloadKey]
  );

  const encryptPayloadBytes = React.useCallback(
    async (payload: Uint8Array | null): Promise<Uint8Array | null> => {
      if (payload === null) return null;
      const key = await requireDocPayloadKey();
      return await encryptTreecrdtPayloadV1({ docId, payloadKey: key, plaintext: payload });
    },
    [docId, requireDocPayloadKey]
  );
  const knownOpsRef = useRef<Set<string>>(new Set());

  const treeStateRef = useRef<TreeState>(treeState);
  useEffect(() => {
    treeStateRef.current = treeState;
  }, [treeState]);

  useEffect(() => {
    showOpsPanelRef.current = showOpsPanel;
    if (!showOpsPanel) {
      setOps([]);
      knownOpsRef.current = new Set();
    }
  }, [showOpsPanel]);

  const ingestOps = React.useCallback(
    (incoming: Operation[], opts: { assumeSorted?: boolean } = {}) => {
      if (!showOpsPanelRef.current) return;
      if (incoming.length === 0) return;
      const fresh: Operation[] = [];
      const known = knownOpsRef.current;
      for (const op of incoming) {
        const key = opKey(op);
        if (known.has(key)) continue;
        known.add(key);
        fresh.push(op);
      }
      if (fresh.length === 0) return;
      if (!opts.assumeSorted) fresh.sort(compareOps);
      setOps((prev) => mergeSortedOps(prev, fresh));
    },
    []
  );

  const childrenLoadInFlightRef = useRef<Set<string>>(new Set());

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
        if (children.length > 0) {
          try {
            const ops = await active.ops.children(parentId);
            await ingestPayloadOps(ops);
          } catch (err) {
            console.error("Failed to load child payloads", err);
          }
        }
      } catch (err) {
        console.error("Failed to load children", err);
        setError("Failed to load tree children (see console)");
      } finally {
        childrenLoadInFlightRef.current.delete(parentId);
      }
    },
    [client, ingestPayloadOps]
  );

  const refreshParents = React.useCallback(
    async (parentIds: Iterable<string>, opts: { nextClient?: TreecrdtClient } = {}) => {
      const active = opts.nextClient ?? clientRef.current ?? client;
      if (!active) return;

      const loadedChildren = treeStateRef.current.childrenByParent;
      const unique = new Set<string>();
      for (const id of parentIds) {
        if (Object.prototype.hasOwnProperty.call(loadedChildren, id)) unique.add(id);
      }
      const ids = Array.from(unique);
      if (ids.length === 0) return;

      try {
        const results = await Promise.all(ids.map(async (id) => [id, await active.tree.children(id)] as const));
        setTreeState((prev) => {
          let next = prev;
          for (const [id, children] of results) next = applyChildrenLoaded(next, id, children);
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
        const [lamport, counter] = await Promise.all([
          active.meta.headLamport(),
          replica ? active.meta.replicaMaxCounter(replica) : Promise.resolve(0),
        ]);
        lamportRef.current = Math.max(lamportRef.current, lamport);
        setHeadLamport(lamportRef.current);
        counterRef.current = Math.max(counterRef.current, counter);
      } catch (err) {
        console.error("Failed to refresh meta", err);
      }
    },
    [client, replica]
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
      queueMicrotask(() => {
        refreshParentsScheduledRef.current = false;
        const ids = Array.from(refreshParentsQueueRef.current);
        refreshParentsQueueRef.current.clear();
        void refreshParents(ids);
      });
    },
    [refreshParents]
  );

  const refreshNodeCountQueuedRef = useRef(false);
  const scheduleRefreshNodeCount = React.useCallback(() => {
    if (refreshNodeCountQueuedRef.current) return;
    refreshNodeCountQueuedRef.current = true;
    queueMicrotask(() => {
      refreshNodeCountQueuedRef.current = false;
      void refreshNodeCount();
    });
  }, [refreshNodeCount]);

  const getMaxLamport = React.useCallback(() => BigInt(lamportRef.current), []);

  const onRemoteOpsApplied = React.useCallback(
    async (appliedOps: Operation[]) => {
      await ingestPayloadOps(appliedOps);
      ingestOps(appliedOps);
      if (appliedOps.length > 0) {
        let max = 0;
        for (const op of appliedOps) max = Math.max(max, op.meta.lamport);
        lamportRef.current = Math.max(lamportRef.current, max);
        setHeadLamport(lamportRef.current);
      }
      scheduleRefreshParents(Object.keys(treeStateRef.current.childrenByParent));
      scheduleRefreshNodeCount();
    },
    [ingestOps, ingestPayloadOps, scheduleRefreshNodeCount, scheduleRefreshParents]
  );

  const openNewPeerTab = () => {
    if (typeof window === "undefined") return;
    const url = new URL(window.location.href);
    url.searchParams.set("doc", docId);
    url.searchParams.set("fresh", "1");
    url.searchParams.delete("replica");
    url.searchParams.delete("auth");
    url.hash = "";
    window.open(url.toString(), "_blank", "noopener,noreferrer");
  };

  const { index, childrenByParent } = treeState;

  const {
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
  } = usePlaygroundSync({
    client,
    status,
    docId,
    selfPeerId,
    autoSyncJoin,
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
  });

  const grantSubtreeToReplicaPubkey = React.useCallback(async () => {
    await grantSubtreeToReplicaPubkeyRaw(postBroadcastMessage);
  }, [grantSubtreeToReplicaPubkeyRaw, postBroadcastMessage]);

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
    (id: string) => {
      if (id === ROOT_ID) return "Root";
      const record = payloadByNodeRef.current.get(id);
      const payload = record?.payload ?? null;
      if (payload === null) return record?.encrypted ? "(encrypted)" : id;
      const decoded = textDecoder.decode(payload);
      return decoded.length === 0 ? "(empty)" : decoded;
    },
    [payloadVersion, textDecoder]
  );

  const nodeList = useMemo(
    () => flattenForSelectState(childrenByParent, nodeLabelForId, { rootId: viewRootId }),
    [childrenByParent, nodeLabelForId, viewRootId]
  );
  const privateRootEntries = useMemo(() => {
    const roots = Array.from(privateRoots).filter((id) => id !== ROOT_ID);
    roots.sort((a, b) => {
      const la = nodeLabelForId(a);
      const lb = nodeLabelForId(b);
      if (la === lb) return a.localeCompare(b);
      return la.localeCompare(lb);
    });
    return roots.map((id) => ({ id, label: nodeLabelForId(id) }));
  }, [privateRoots, nodeLabelForId]);
  const visibleNodes = useMemo(() => {
    const acc: Array<{ node: DisplayNode; depth: number }> = [];
    const isCollapsed = (id: string) => {
      return collapse.defaultCollapsed ? !collapse.overrides.has(id) : collapse.overrides.has(id);
    };
    const stack: Array<{ id: string; depth: number }> = [{ id: viewRootId, depth: 0 }];
    while (stack.length > 0) {
      const entry = stack.pop();
      if (!entry) break;
      const record = payloadByNodeRef.current.get(entry.id);
      const payload = record?.payload ?? null;
      const value = payload === null ? "" : textDecoder.decode(payload);
      const label =
        entry.id === ROOT_ID
          ? "Root"
          : payload === null
            ? record?.encrypted
              ? "(encrypted)"
              : entry.id
            : value.length === 0
              ? "(empty)"
              : value;
      acc.push({ node: { id: entry.id, label, value, children: [] }, depth: entry.depth });
      if (isCollapsed(entry.id)) continue;
      const kids = childrenByParent[entry.id] ?? [];
      for (let i = kids.length - 1; i >= 0; i--) {
        stack.push({ id: kids[i]!, depth: entry.depth + 1 });
      }
    }
    return acc;
  }, [childrenByParent, collapse, payloadVersion, textDecoder]);
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

  useEffect(() => {
    return () => {
      void clientRef.current?.close();
    };
  }, []);

  const initClient = async (storageMode: StorageMode, keyOverride?: string) => {
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
        storage: storageMode,
        baseUrl,
        preferWorker: storageMode === "opfs",
        filename,
        docId,
      });
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

  const resetAndInit = async (target: StorageMode, opts: { resetKey?: boolean } = {}) => {
    const nextKey =
      target === "opfs"
        ? opts.resetKey
          ? persistOpfsKey(makeSessionKey())
          : ensureOpfsKey()
        : sessionKey;
    setSessionKey(nextKey);
    setOps([]);
    setTreeState({
      index: { [ROOT_ID]: { parentId: null, order: 0, childCount: 0, deleted: false } },
      childrenByParent: { [ROOT_ID]: [] },
    });
    payloadByNodeRef.current = new Map();
    setPayloadVersion((v) => v + 1);
    knownOpsRef.current = new Set();
    setCollapse({ defaultCollapsed: true, overrides: new Set([ROOT_ID]) });
    counterRef.current = 0;
    lamportRef.current = 0;
    setHeadLamport(0);
    setTotalNodes(null);
    if (clientRef.current?.close) {
      await clientRef.current.close();
    }
    clientRef.current = null;
    setClient(null);
    await initClient(target, nextKey);
  };

  const refreshOps = async (nextClient?: TreecrdtClient, opts: { preserveParent?: boolean } = {}) => {
    const active = nextClient ?? client;
    if (!active) return;
    try {
      const fetched = await active.ops.all();
      fetched.sort(compareOps);
      setOps(fetched);
      knownOpsRef.current = new Set(fetched.map(opKey));
      await ingestPayloadOps(fetched);
      setParentChoice((prev) => (opts.preserveParent ? prev : ROOT_ID));
    } catch (err) {
      console.error("Failed to refresh ops", err);
      setError("Failed to refresh operations (see console)");
    }
  };

  useEffect(() => {
    if (!showOpsPanel) return;
    if (!client || status !== "ready") return;
    void refreshOps(undefined, { preserveParent: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [showOpsPanel, client, status]);

  const appendOperation = async (kind: OperationKind) => {
    if (!client || !replica) return;
    setBusy(true);
    try {
      const stateBefore = treeStateRef.current;

      let op: Operation;
      if (kind.type === "payload") {
        const encryptedPayload = await encryptPayloadBytes(kind.payload);
        op = await client.local.payload(replica, kind.node, encryptedPayload);
      } else if (kind.type === "delete") {
        op = await client.local.delete(replica, kind.node);
      } else {
        throw new Error(`unsupported operation kind: ${kind.type}`);
      }
      await verifyLocalOps([op]);

      lamportRef.current = Math.max(lamportRef.current, op.meta.lamport);
      counterRef.current = Math.max(counterRef.current, op.meta.id.counter);
      setHeadLamport(lamportRef.current);

      notifyLocalUpdate();
      await ingestPayloadOps([op]);
      ingestOps([op], { assumeSorted: true });
      scheduleRefreshParents(parentsAffectedByOps(stateBefore, [op]));
      scheduleRefreshNodeCount();
    } catch (err) {
      console.error("Failed to append op", err);
      setError("Failed to append operation (see console)");
    } finally {
      setBusy(false);
    }
  };

  const appendMoveAfter = async (nodeId: string, newParent: string, after: string | null) => {
    if (!client || !replica) return;
    if (authEnabled && (!canWriteStructure || (isScopedAccess && newParent === ROOT_ID))) return;
    setBusy(true);
    try {
      const stateBefore = treeStateRef.current;
      const placement = after ? { type: "after" as const, after } : { type: "first" as const };
      const op = await client.local.move(replica, nodeId, newParent, placement);
      notifyLocalUpdate();
      await ingestPayloadOps([op]);
      ingestOps([op], { assumeSorted: true });
      scheduleRefreshParents(parentsAffectedByOps(stateBefore, [op]));
      scheduleRefreshNodeCount();
      lamportRef.current = Math.max(lamportRef.current, op.meta.lamport);
      counterRef.current = Math.max(counterRef.current, op.meta.id.counter);
      setHeadLamport(lamportRef.current);
    } catch (err) {
      console.error("Failed to append move op", err);
      setError("Failed to move node (see console)");
    } finally {
      setBusy(false);
    }
  };

  const handleAddNodes = async (parentId: string, count: number, opts: { fanout?: number } = {}) => {
    if (!client || !replica) return;
    if (authEnabled && !canWriteStructure) return;
    const normalizedCount = Math.max(0, Math.min(MAX_COMPOSER_NODE_COUNT, Math.floor(count)));
    if (normalizedCount <= 0) return;
    setBusy(true);
    try {
      const stateBefore = treeStateRef.current;
      const ops: Operation[] = [];
      const fanoutLimit = Math.max(0, Math.floor(opts.fanout ?? fanout));
      const valueBase = canWritePayload ? newNodeValue.trim() : "";
      const shouldSetValue = canWritePayload && valueBase.length > 0;

      if (fanoutLimit <= 0) {
        for (let i = 0; i < normalizedCount; i++) {
          const nodeId = makeNodeId();
          const value = normalizedCount > 1 ? `${valueBase} ${i + 1}` : valueBase;
          const payload = shouldSetValue ? textEncoder.encode(value) : null;
          const encryptedPayload = await encryptPayloadBytes(payload);
          ops.push(await client.local.insert(replica, parentId, nodeId, { type: "last" }, encryptedPayload));
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
          const payload = shouldSetValue ? textEncoder.encode(value) : null;
          const encryptedPayload = await encryptPayloadBytes(payload);
          ops.push(await client.local.insert(replica, targetParent, nodeId, { type: "last" }, encryptedPayload));

          setChildCount(targetParent, childCount + 1);
          queue.push(nodeId);
        }
      }

      for (const op of ops) {
        lamportRef.current = Math.max(lamportRef.current, op.meta.lamport);
        counterRef.current = Math.max(counterRef.current, op.meta.id.counter);
      }
      setHeadLamport(lamportRef.current);

      notifyLocalUpdate();
      await ingestPayloadOps(ops);
      ingestOps(ops, { assumeSorted: true });
      scheduleRefreshParents(parentsAffectedByOps(stateBefore, ops));
      scheduleRefreshNodeCount();
      setCollapse((prev) => {
        const overrides = new Set(prev.overrides);
        const setExpanded = (id: string) => {
          if (prev.defaultCollapsed) overrides.add(id);
          else overrides.delete(id);
        };
        // Keep the chosen parent expanded so the user sees immediate children.
        setExpanded(parentId);
        let cur = index[parentId]?.parentId ?? null;
        while (cur) {
          setExpanded(cur);
          cur = index[cur]?.parentId ?? null;
        }
        return { ...prev, overrides };
      });
    } catch (err) {
      console.error("Failed to add nodes", err);
      setError("Failed to add nodes (see console)");
    } finally {
      setBusy(false);
    }
  };

  const handleInsert = async (parentId: string) => {
    if (!client || !replica) return;
    if (authEnabled && !canWriteStructure) return;
    setBusy(true);
    try {
      const stateBefore = treeStateRef.current;
      const valueBase = canWritePayload ? newNodeValue.trim() : "";
      const payload = valueBase.length > 0 ? textEncoder.encode(valueBase) : null;
      const encryptedPayload = await encryptPayloadBytes(payload);
      const nodeId = makeNodeId();
      const op = await client.local.insert(replica, parentId, nodeId, { type: "last" }, encryptedPayload);
      notifyLocalUpdate();
      await ingestPayloadOps([op]);
      ingestOps([op], { assumeSorted: true });
      scheduleRefreshParents(parentsAffectedByOps(stateBefore, [op]));
      scheduleRefreshNodeCount();
      if (!Object.prototype.hasOwnProperty.call(treeStateRef.current.childrenByParent, parentId)) {
        await ensureChildrenLoaded(parentId, { force: true });
      }
      lamportRef.current = Math.max(lamportRef.current, op.meta.lamport);
      counterRef.current = Math.max(counterRef.current, op.meta.id.counter);
      setHeadLamport(lamportRef.current);
      setCollapse((prev) => {
        const overrides = new Set(prev.overrides);
        const setExpanded = (id: string) => {
          if (prev.defaultCollapsed) overrides.add(id);
          else overrides.delete(id);
        };
        setExpanded(parentId);
        let cur = index[parentId]?.parentId ?? null;
        while (cur) {
          setExpanded(cur);
          cur = index[cur]?.parentId ?? null;
        }
        return { ...prev, overrides };
      });
    } catch (err) {
      console.error("Failed to insert node", err);
      setError("Failed to insert node (see console)");
    } finally {
      setBusy(false);
    }
  };

  const handleSetValue = async (nodeId: string, value: string) => {
    if (nodeId === ROOT_ID) return;
    const payload = value.trim().length === 0 ? null : textEncoder.encode(value);
    await appendOperation({ type: "payload", node: nodeId, payload });
  };

  const handleDelete = async (nodeId: string) => {
    if (nodeId === ROOT_ID) return;
    await appendOperation({ type: "delete", node: nodeId });
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
  const peerTotal = peers.length + 1;
  const authScopeSummary = (() => {
    if (!authTokenScope) return "-";
    const rootId = (authTokenScope.rootNodeId ?? ROOT_ID).toLowerCase();
    if (rootId === ROOT_ID) return "doc-wide";
    const label = nodeLabelForId(rootId);
    if (label && label !== rootId) return `subtree ${label}`;
    return `subtree ${rootId.slice(0, 8)}…`;
  })();
  const authScopeTitle = (() => {
    if (!authTokenScope) return "";
    const rootId = (authTokenScope.rootNodeId ?? ROOT_ID).toLowerCase();
    const parts = [`root=${rootId}`];
    if (authTokenScope.maxDepth !== undefined) parts.push(`maxDepth=${authTokenScope.maxDepth}`);
    const excludeCount = authTokenScope.excludeNodeIds?.length ?? 0;
    if (excludeCount > 0) parts.push(`exclude=${excludeCount}`);
    return parts.join(" ");
  })();
  const authSummaryBadges = (() => {
    if (!Array.isArray(authTokenActions)) return [];
    const set = new Set(authTokenActions.map(String));
    const out: string[] = [];
    if (set.has("write_structure") || set.has("write_payload")) out.push("write");
    if (set.has("delete")) out.push("delete");
    if (set.has("tombstone")) out.push("tombstone");
    return out;
  })();
  const localReplicaHex = selfPeerId;

  return (
    <div className="mx-auto max-w-6xl px-4 pb-12 pt-8 space-y-6">
      <ShareSubtreeDialog
        open={showShareDialog}
        onClose={() => setShowShareDialog(false)}
        inviteRoot={inviteRoot}
        nodeLabelForId={nodeLabelForId}
        joinMode={joinMode}
        authEnabled={authEnabled}
        authBusy={authBusy}
        authCanIssue={authCanIssue}
        authCanDelegate={authCanDelegate}
        authScopeTitle={authScopeTitle}
        authScopeSummary={authScopeSummary}
        inviteExcludeNodeIds={inviteExcludeNodeIds}
        onEnableAuth={() => setAuthEnabled(true)}
        openMintingPeerTab={openMintingPeerTab}
        authInfo={authInfo}
        authError={authError}
        setAuthError={setAuthError}
        invitePreset={invitePreset}
        applyInvitePreset={applyInvitePreset}
        inviteActions={inviteActions}
        setInviteActions={setInviteActions}
        inviteAllowGrant={inviteAllowGrant}
        setInviteAllowGrant={setInviteAllowGrant}
        openNewIsolatedPeerTab={openNewIsolatedPeerTab}
        generateInviteLink={generateInviteLink}
        inviteLink={inviteLink}
        copyToClipboard={copyToClipboard}
        grantRecipientKey={grantRecipientKey}
        setGrantRecipientKey={setGrantRecipientKey}
        grantSubtreeToReplicaPubkey={grantSubtreeToReplicaPubkey}
        selfPeerId={selfPeerId}
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
        onReset={handleReset}
        onExpandAll={expandAll}
        onCollapseAll={collapseAll}
        error={error}
      />

      <div className="grid gap-6 md:grid-cols-3">
        <section className={`${showOpsPanel ? "md:col-span-2" : "md:col-span-3"} space-y-4`}>
          <ComposerPanel
            composerOpen={composerOpen}
            setComposerOpen={setComposerOpen}
            nodeList={nodeList}
            parentChoice={parentChoice}
            setParentChoice={setParentChoice}
            newNodeValue={newNodeValue}
            setNewNodeValue={setNewNodeValue}
            nodeCount={nodeCount}
            setNodeCount={setNodeCount}
            maxNodeCount={MAX_COMPOSER_NODE_COUNT}
            fanout={fanout}
            setFanout={setFanout}
            onAddNodes={handleAddNodes}
            ready={status === "ready"}
            busy={busy}
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
            peerCount={peers.length}
            authCanSyncAll={authCanSyncAll}
            onSync={() => void (authCanSyncAll ? handleSync({ all: {} }) : handleScopedSync())}
            liveAllEnabled={liveAllEnabled}
            setLiveAllEnabled={setLiveAllEnabled}
            showPeersPanel={showPeersPanel}
            setShowPeersPanel={setShowPeersPanel}
            peerTotal={peerTotal}
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
              docId,
              selfPeerId,
              joinMode,
              profileId,
              authEnabled,
              authTokenCount,
              authScopeTitle,
              authScopeSummary,
              authSummaryBadges,
              authCanIssue,
              authCanDelegate,
              openNewIsolatedPeerTab,
              openNewPeerTab,
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
              authScopeSummary,
              authScopeTitle,
              authSummaryBadges,
              selfPeerId,
              revealIdentity,
              setRevealIdentity,
              openMintingPeerTab,
              showAuthAdvanced,
              setShowAuthAdvanced,
              showInviteOptions,
              setShowInviteOptions,
              invitePanelRef,
              nodeList,
              inviteRoot,
              setInviteRoot,
              inviteMaxDepth,
              setInviteMaxDepth,
              inviteActions,
              setInviteActions,
              inviteAllowGrant,
              setInviteAllowGrant,
              invitePreset,
              setInvitePreset,
              inviteExcludeNodeIds,
              inviteLink,
              generateInviteLink,
              applyInvitePreset,
              copyToClipboard,
              refreshAuthMaterial,
              refreshPendingOps,
              client,
              pendingOps,
              showPrivateRootsPanel,
              setShowPrivateRootsPanel,
              privateRootsCount,
              privateRootEntries,
              togglePrivateRoot,
              clearPrivateRoots,
              wrapKeyImportText,
              setWrapKeyImportText,
              issuerKeyBlobImportText,
              setIssuerKeyBlobImportText,
              identityKeyBlobImportText,
              setIdentityKeyBlobImportText,
              deviceSigningKeyBlobImportText,
              setDeviceSigningKeyBlobImportText,
              localIdentityChainPromiseRef,
              inviteImportText,
              setInviteImportText,
              importInviteLink,
              grantRecipientKey,
              setGrantRecipientKey,
              grantSubtreeToReplicaPubkey,
              nodeLabelForId,
            }}
            treeParentRef={treeParentRef}
            treeVirtualizer={treeVirtualizer}
            visibleNodes={visibleNodes}
            collapse={collapse}
            toggleCollapse={toggleCollapse}
            openShareForNode={openShareForNode}
            onSetValue={handleSetValue}
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
            scopeRootId={viewRootId}
            canWritePayload={canWritePayload}
            canWriteStructure={canWriteStructure}
            canDelete={canDelete}
            liveChildrenParents={liveChildrenParents}
            meta={index}
            childrenByParent={childrenByParent}
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
          void (authCanSyncAll ? handleSync({ all: {} }) : handleScopedSync());
        }}
        canSync={status === "ready" && !busy && !syncBusy && peers.length > 0 && online}
        onDetails={() => setShowAuthPanel(true)}
      />
    </div>
  );
}

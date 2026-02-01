import React, { useEffect, useMemo, useRef, useState } from "react";
import { type Operation, type OperationKind } from "@treecrdt/interface";
import { bytesToHex } from "@treecrdt/interface/ids";
import { createTreecrdtClient, type TreecrdtClient } from "@treecrdt/wa-sqlite/client";
import { detectOpfsSupport } from "@treecrdt/wa-sqlite/opfs";
import {
  SyncPeer,
  base64urlDecode,
  base64urlEncode,
  createTreecrdtCoseCwtAuth,
  createTreecrdtIdentityChainCapabilityV1,
  createTreecrdtSqliteSubtreeScopeEvaluator,
  createTreecrdtSyncSqlitePendingOpsStore,
  describeTreecrdtCapabilityTokenV1,
  deriveKeyIdV1,
  deriveTokenIdV1,
  encryptTreecrdtPayloadV1,
  maybeDecryptTreecrdtPayloadV1,
  type Filter,
  type SyncSubscription,
  type TreecrdtCapabilityTokenV1,
} from "@treecrdt/sync";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync/protobuf";
import type { DuplexTransport } from "@treecrdt/sync/transport";
import {
  MdCloudOff,
  MdCloudQueue,
  MdContentCopy,
  MdGroup,
  MdLockOutline,
  MdOpenInNew,
  MdOutlineRssFeed,
  MdSync,
  MdVpnKey,
} from "react-icons/md";
import { IoMdGitBranch } from "react-icons/io";

import {
  clearAuthMaterial,
  createLocalIdentityChainV1,
  createCapabilityTokenV1,
  decodeInvitePayload,
  encodeInvitePayload,
  generateEd25519KeyPair,
  deriveEd25519PublicKey,
  getDeviceWrapKeyB64,
  getSealedDeviceSigningKeyB64,
  getSealedIdentityKeyB64,
  getSealedIssuerKeyB64,
  importDeviceWrapKeyB64,
  initialAuthEnabled,
  initialRevealIdentity,
  loadOrCreateDocPayloadKeyB64,
  loadAuthMaterial,
  persistRevealIdentity,
  persistAuthEnabled,
  saveDocPayloadKeyB64,
  saveIssuerKeys,
  saveLocalKeys,
  saveLocalTokens,
  setSealedDeviceSigningKeyB64,
  setSealedIdentityKeyB64,
  setSealedIssuerKeyB64,
  type StoredAuthMaterial,
} from "./auth";
import { createBroadcastDuplex, createPlaygroundBackend, hexToBytes16, type PresenceAckMessage, type PresenceMessage } from "./sync-v0";
import { useVirtualizer } from "./virtualizer";

import {
  MAX_COMPOSER_NODE_COUNT,
  PLAYGROUND_PEER_TIMEOUT_MS,
  PLAYGROUND_SYNC_MAX_CODEWORDS,
  PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
  ROOT_ID,
} from "./playground/constants";
import { ParentPicker } from "./playground/components/ParentPicker";
import { TreeRow } from "./playground/components/TreeRow";
import { compareOps, mergeSortedOps, opKey, renderKind } from "./playground/ops";
import { compareOpMeta } from "./playground/payload";
import {
  ensureOpfsKey,
  initialDocId,
  initialStorage,
  loadPrivateRoots,
  makeNodeId,
  makeSessionKey,
  persistDocId,
  persistOpfsKey,
  persistPrivateRoots,
  persistStorage,
  pickReplicaLabel,
} from "./playground/persist";
import { prefixPlaygroundStorageKey } from "./playground/storage";
import { applyChildrenLoaded, flattenForSelectState, parentsAffectedByOps } from "./playground/treeState";
import type {
  CollapseState,
  DisplayNode,
  NodeMeta,
  PayloadRecord,
  PeerInfo,
  Status,
  StorageMode,
  TreeState,
} from "./playground/types";

function computeInviteExcludeNodeIds(privateRoots: Set<string>, inviteRoot: string): string[] {
  return Array.from(privateRoots).filter((id) => id !== inviteRoot && id !== ROOT_ID && /^[0-9a-f]{32}$/i.test(id));
}

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
  const [syncBusy, setSyncBusy] = useState(false);
  const [syncError, setSyncError] = useState<string | null>(null);
  const [peers, setPeers] = useState<PeerInfo[]>([]);
  const [nodeCount, setNodeCount] = useState(1);
  const [fanout, setFanout] = useState(10);
  const [newNodeValue, setNewNodeValue] = useState("");
  const [liveChildrenParents, setLiveChildrenParents] = useState<Set<string>>(() => new Set());
  const [liveAllEnabled, setLiveAllEnabled] = useState(false);
  const [showOpsPanel, setShowOpsPanel] = useState(false);
  const [showPeersPanel, setShowPeersPanel] = useState(false);
  const [online, setOnline] = useState(true);
  const [payloadVersion, setPayloadVersion] = useState(0);

  const joinMode =
    typeof window !== "undefined" && new URLSearchParams(window.location.search).get("join") === "1";

  const counterRef = useRef(0);
  const lamportRef = useRef(0);
  const onlineRef = useRef(true);
  const replicaLabel = useMemo(pickReplicaLabel, []);
  const opfsSupport = useMemo(detectOpfsSupport, []);
  const [authEnabled, setAuthEnabled] = useState(() => initialAuthEnabled());
  const [revealIdentity, setRevealIdentity] = useState(() => initialRevealIdentity());
  const [showAuthPanel, setShowAuthPanel] = useState(false);
  const [authError, setAuthError] = useState<string | null>(null);
  const [authBusy, setAuthBusy] = useState(false);
  const [wrapKeyImportText, setWrapKeyImportText] = useState("");
  const [issuerKeyBlobImportText, setIssuerKeyBlobImportText] = useState("");
  const [identityKeyBlobImportText, setIdentityKeyBlobImportText] = useState("");
  const [deviceSigningKeyBlobImportText, setDeviceSigningKeyBlobImportText] = useState("");
  const [authMaterial, setAuthMaterial] = useState<StoredAuthMaterial>(() => ({
    issuerPkB64: null,
    issuerSkB64: null,
    localPkB64: null,
    localSkB64: null,
    localTokensB64: [],
  }));
  const localAuthRef = useRef<ReturnType<typeof createTreecrdtCoseCwtAuth> | null>(null);
  const docPayloadKeyRef = useRef<Uint8Array | null>(null);
  const localIdentityChainPromiseRef = useRef<Promise<Awaited<ReturnType<typeof createLocalIdentityChainV1>> | null> | null>(null);
  const [authToken, setAuthToken] = useState<TreecrdtCapabilityTokenV1 | null>(null);

  const [inviteRoot, setInviteRoot] = useState(ROOT_ID);
  const [inviteMaxDepth, setInviteMaxDepth] = useState<string>("");
  const [inviteActions, setInviteActions] = useState<
    Record<"write_structure" | "write_payload" | "delete" | "tombstone", boolean>
  >({
    write_structure: true,
    write_payload: true,
    delete: false,
    tombstone: false,
  });
  const [inviteLink, setInviteLink] = useState<string>("");
  const [inviteImportText, setInviteImportText] = useState<string>("");
  const [pendingOps, setPendingOps] = useState<Array<{ id: string; kind: string; message?: string }>>([]);
  const showOpsPanelRef = useRef(false);
  const textEncoder = useMemo(() => new TextEncoder(), []);
  const textDecoder = useMemo(() => new TextDecoder(), []);
  const identityByReplicaRef = useRef<Map<string, { identityPk: Uint8Array; devicePk: Uint8Array }>>(new Map());
  const [identityVersion, setIdentityVersion] = useState(0);
  const [privateRoots, setPrivateRoots] = useState<Set<string>>(() => loadPrivateRoots(docId));
  const privateRootsCount = useMemo(
    () => Array.from(privateRoots).filter((id) => id !== ROOT_ID).length,
    [privateRoots]
  );
  const inviteExcludeNodeIds = useMemo(
    () => computeInviteExcludeNodeIds(privateRoots, inviteRoot),
    [privateRoots, inviteRoot]
  );

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
      setIdentityVersion((v) => v + 1);
    },
    []
  );

  useEffect(() => {
    setPrivateRoots(loadPrivateRoots(docId));
  }, [docId]);

  useEffect(() => {
    // Local identity chains are doc-bound (replica cert includes `docId`) and depend on the current replica key.
    localIdentityChainPromiseRef.current = null;
  }, [docId, authMaterial.localPkB64, revealIdentity]);

  const togglePrivateRoot = (id: string) => {
    if (id === ROOT_ID) return;
    setPrivateRoots((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      persistPrivateRoots(docId, next);
      return next;
    });
  };

  const clearPrivateRoots = () => {
    setPrivateRoots(() => {
      const next = new Set<string>();
      persistPrivateRoots(docId, next);
      return next;
    });
  };

  const refreshAuthMaterial = React.useCallback(async () => {
    const next = await loadAuthMaterial(docId, replicaLabel);
    setAuthMaterial(next);
    return next;
  }, [docId, replicaLabel]);

  const refreshDocPayloadKey = React.useCallback(async () => {
    const keyB64 = await loadOrCreateDocPayloadKeyB64(docId);
    docPayloadKeyRef.current = base64urlDecode(keyB64);
    return docPayloadKeyRef.current;
  }, [docId]);

  const getLocalIdentityChain = React.useCallback(async () => {
    if (!revealIdentity) return null;
    const pkB64 = authMaterial.localPkB64;
    if (!pkB64) return null;

    if (!localIdentityChainPromiseRef.current) {
      const replicaPk = base64urlDecode(pkB64);
      localIdentityChainPromiseRef.current = createLocalIdentityChainV1({ docId, replicaPublicKey: replicaPk }).catch((err) => {
        console.error("Failed to create identity chain", err);
        return null;
      });
    }

    return await localIdentityChainPromiseRef.current;
  }, [authMaterial.localPkB64, docId, revealIdentity]);

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

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const next = await loadAuthMaterial(docId, replicaLabel);
        if (cancelled) return;
        setAuthMaterial(next);
      } catch (err) {
        if (cancelled) return;
        setAuthError(err instanceof Error ? err.message : String(err));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [docId, replicaLabel]);

  useEffect(() => {
    if (!authEnabled || !client) {
      localAuthRef.current = null;
      return;
    }

    try {
      if (
        !authMaterial.issuerPkB64 ||
        !authMaterial.localSkB64 ||
        !authMaterial.localPkB64 ||
        authMaterial.localTokensB64.length === 0
      ) {
        localAuthRef.current = null;
        return;
      }

      const issuerPk = base64urlDecode(authMaterial.issuerPkB64);
      const localSk = base64urlDecode(authMaterial.localSkB64);
      const localPk = base64urlDecode(authMaterial.localPkB64);
      const localTokens = authMaterial.localTokensB64.map((t) => base64urlDecode(t));
      const scopeEvaluator = createTreecrdtSqliteSubtreeScopeEvaluator(client.runner);

      localAuthRef.current = createTreecrdtCoseCwtAuth({
        issuerPublicKeys: [issuerPk],
        localPrivateKey: localSk,
        localPublicKey: localPk,
        localCapabilityTokens: localTokens,
        requireProofRef: true,
        scopeEvaluator,
      });
    } catch (err) {
      localAuthRef.current = null;
      setAuthError(err instanceof Error ? err.message : String(err));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    authEnabled,
    client,
    docId,
    authMaterial.issuerPkB64,
    authMaterial.localSkB64,
    authMaterial.localPkB64,
    authMaterial.localTokensB64.join(","),
  ]);

  const replica = useMemo(
    () => (authMaterial.localPkB64 ? base64urlDecode(authMaterial.localPkB64) : null),
    [authMaterial.localPkB64]
  );

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

  useEffect(() => {
    onlineRef.current = online;
  }, [online]);

  const syncConnRef = useRef<
    Map<string, { transport: DuplexTransport<any>; peer: SyncPeer<Operation>; detach: () => void }>
  >(new Map());
  const knownOpsRef = useRef<Set<string>>(new Set());
  const liveChildrenParentsRef = useRef<Set<string>>(new Set());
  const liveChildSubsRef = useRef<Map<string, Map<string, SyncSubscription>>>(new Map());
  const liveAllEnabledRef = useRef(false);
  const liveAllSubsRef = useRef<Map<string, SyncSubscription>>(new Map());
  const liveAllStartingRef = useRef<Set<string>>(new Set());
  const liveChildrenStartingRef = useRef<Set<string>>(new Set());
  const peerReadyRef = useRef<Set<string>>(new Set());
  const peerAckSentRef = useRef<Set<string>>(new Set());

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
    if (!conn) return;

    if (liveAllSubsRef.current.has(peerId)) return;
    if (liveAllStartingRef.current.has(peerId)) return;
    liveAllStartingRef.current.add(peerId);

    void (async () => {
      try {
        await conn.peer.syncOnce(conn.transport, { all: {} }, {
          maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
          maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
          codewordsPerMessage: 1024,
        });
      } catch (err) {
        console.error("Live sync(all) initial catch-up failed", err);
        setSyncError(err instanceof Error ? err.message : String(err));
        return;
      }

      const sub = conn.peer.subscribe(
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
    if (!conn) return;

    const existing = liveChildSubsRef.current.get(peerId);
    if (existing?.has(parentId)) return;
    const startKey = `${peerId}\u0000${parentId}`;
    if (liveChildrenStartingRef.current.has(startKey)) return;
    liveChildrenStartingRef.current.add(startKey);

    const byParent = existing ?? new Map<string, SyncSubscription>();
    void (async () => {
      try {
        await conn.peer.syncOnce(conn.transport, { children: { parent: hexToBytes16(parentId) } }, {
          maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
          maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
          codewordsPerMessage: 1024,
        });
      } catch (err) {
        console.error("Live sync(children) initial catch-up failed", err);
        setSyncError(err instanceof Error ? err.message : String(err));
        return;
      }

      const sub = conn.peer.subscribe(
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

  const makeNewPeerLabel = () => `replica-${crypto.randomUUID().slice(0, 8)}`;
  const makeNewProfileId = () => `profile-${crypto.randomUUID().slice(0, 8)}`;

  const openNewPeerTab = () => {
    if (typeof window === "undefined") return;
    const url = new URL(window.location.href);
    url.searchParams.set("doc", docId);
    url.searchParams.set("replica", makeNewPeerLabel());
    url.searchParams.delete("auth");
    url.hash = "";
    window.open(url.toString(), "_blank", "noopener,noreferrer");
  };

  const openNewIsolatedPeerTab = () => {
    if (typeof window === "undefined") return;
    const url = new URL(window.location.href);
    url.searchParams.set("doc", docId);
    url.searchParams.set("replica", makeNewPeerLabel());
    url.searchParams.set("profile", makeNewProfileId());
    url.searchParams.set("join", "1");
    url.searchParams.delete("auth");
    url.hash = "";
    window.open(url.toString(), "_blank", "noopener,noreferrer");
  };

  const copyToClipboard = async (text: string) => {
    if (typeof navigator === "undefined" || typeof navigator.clipboard?.writeText !== "function") {
      throw new Error("clipboard API is not available");
    }
    await navigator.clipboard.writeText(text);
  };

  const resetAuth = () => {
    clearAuthMaterial(docId, replicaLabel);
    setInviteLink("");
    setInviteImportText("");
    setAuthEnabled(false);
    setAuthError(null);
    void refreshAuthMaterial().catch((err) => setAuthError(err instanceof Error ? err.message : String(err)));
  };

  const generateInviteLink = async () => {
    if (typeof window === "undefined") return;
    setAuthBusy(true);
    setAuthError(null);
    setInviteLink("");
    try {
      const issuerSkB64 = authMaterial.issuerSkB64;
      const issuerPkB64 = authMaterial.issuerPkB64;
      if (!issuerSkB64 || !issuerPkB64) {
        throw new Error("issuer private key is not available in this tab (cannot mint invites)");
      }

      const actions = Object.entries(inviteActions)
        .filter(([, enabled]) => enabled)
        .map(([name]) => name);
      if (actions.length === 0) throw new Error("select at least one action");

      const maxDepthText = inviteMaxDepth.trim();
      let maxDepth: number | undefined;
      if (maxDepthText.length > 0) {
        const parsed = Number(maxDepthText);
        if (!Number.isFinite(parsed) || parsed < 0) throw new Error("max depth must be a non-negative number");
        maxDepth = parsed;
      }

      const issuerSk = base64urlDecode(issuerSkB64);
      const { sk: subjectSk, pk: subjectPk } = await generateEd25519KeyPair();
      const tokenBytes = createCapabilityTokenV1({
        issuerPrivateKey: issuerSk,
        subjectPublicKey: subjectPk,
        docId,
        rootNodeId: inviteRoot,
        actions,
        ...(maxDepth !== undefined ? { maxDepth } : {}),
        ...(inviteExcludeNodeIds.length > 0 ? { excludeNodeIds: inviteExcludeNodeIds } : {}),
      });

      const inviteB64 = encodeInvitePayload({
        v: 1,
        t: "treecrdt.playground.invite",
        docId,
        issuerPkB64,
        subjectSkB64: base64urlEncode(subjectSk),
        tokenB64: base64urlEncode(tokenBytes),
        payloadKeyB64: await loadOrCreateDocPayloadKeyB64(docId),
      });

      const url = new URL(window.location.href);
      url.searchParams.set("doc", docId);
      url.searchParams.set("replica", makeNewPeerLabel());
      url.searchParams.set("auth", "1");
      url.hash = `invite=${inviteB64}`;
      setInviteLink(url.toString());
    } catch (err) {
      setAuthError(err instanceof Error ? err.message : String(err));
    } finally {
      setAuthBusy(false);
    }
  };

  const importInviteLink = async () => {
    if (typeof window === "undefined") return;
    const raw = inviteImportText.trim();
    if (!raw) return;
    setAuthBusy(true);
    setAuthError(null);
    try {
      let inviteB64: string | null = null;
      try {
        const url = new URL(raw, window.location.href);
        inviteB64 = new URLSearchParams(url.hash.slice(1)).get("invite");
      } catch {
        // not a URL; fall back to raw text parsing
      }

      if (!inviteB64) {
        inviteB64 = raw.startsWith("invite=") ? raw.slice("invite=".length) : raw;
      }

      const payload = decodeInvitePayload(inviteB64);
      if (payload.docId !== docId) {
        throw new Error(`invite doc mismatch: got ${payload.docId}, expected ${docId}`);
      }

      if (payload.payloadKeyB64) {
        await saveDocPayloadKeyB64(docId, payload.payloadKeyB64);
        await refreshDocPayloadKey();
      }

      await saveIssuerKeys(docId, payload.issuerPkB64);

      const localSk = base64urlDecode(payload.subjectSkB64);
      const localPk = await deriveEd25519PublicKey(localSk);
      await saveLocalKeys(docId, replicaLabel, base64urlEncode(localPk), payload.subjectSkB64);
      await saveLocalTokens(docId, replicaLabel, [payload.tokenB64]);

      setAuthEnabled(true);
      await refreshAuthMaterial();
      setInviteImportText("");
    } catch (err) {
      setAuthError(err instanceof Error ? err.message : String(err));
    } finally {
      setAuthBusy(false);
    }
  };

  const refreshPendingOps = async () => {
    if (!client) return;
    setAuthBusy(true);
    setAuthError(null);
    try {
      const store = createTreecrdtSyncSqlitePendingOpsStore({ runner: client.runner, docId });
      await store.init();
      const listed = await store.listPendingOps();
      setPendingOps(
        listed.map((p) => ({
          id: `${bytesToHex(p.op.meta.id.replica)}:${p.op.meta.id.counter}`,
          kind: p.op.kind.type,
          ...(p.message ? { message: p.message } : {}),
        }))
      );
    } catch (err) {
      setAuthError(err instanceof Error ? err.message : String(err));
    } finally {
      setAuthBusy(false);
    }
  };

  const { index, childrenByParent } = treeState;

  const viewRootId = useMemo(() => {
    const raw = authEnabled ? authToken?.caps?.[0]?.res.rootNodeId : null;
    if (!raw || typeof raw !== "string") return ROOT_ID;
    const clean = raw.toLowerCase();
    if (!/^[0-9a-f]{32}$/.test(clean)) return ROOT_ID;
    return clean;
  }, [authEnabled, authToken]);

  const authCanSyncAll = useMemo(() => {
    if (!authEnabled) return true;
    if (!authToken) return false;
    if (authToken.caps.length === 0) return true;
    return authToken.caps.some((cap) => {
      const root = cap.res.rootNodeId?.toLowerCase();
      const excludeCount = cap.res.excludeNodeIds?.length ?? 0;
      return root === ROOT_ID && cap.res.maxDepth === undefined && excludeCount === 0;
    });
  }, [authEnabled, authToken]);

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
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [viewRootId]);

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
    persistAuthEnabled(authEnabled);
  }, [authEnabled]);

  useEffect(() => {
    persistRevealIdentity(revealIdentity);
  }, [revealIdentity]);

  useEffect(() => {
    if (!authEnabled) setPendingOps([]);
  }, [authEnabled]);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      if (!authEnabled) {
        setAuthToken(null);
        return;
      }
      if (!authMaterial.issuerPkB64 || authMaterial.localTokensB64.length === 0) {
        setAuthToken(null);
        return;
      }
      try {
        const issuerPk = base64urlDecode(authMaterial.issuerPkB64);
        const tokenBytes = base64urlDecode(authMaterial.localTokensB64[0]!);
        const described = await describeTreecrdtCapabilityTokenV1({
          tokenBytes,
          issuerPublicKeys: [issuerPk],
          docId,
        });
        if (cancelled) return;
        setAuthToken(described);
      } catch {
        if (cancelled) return;
        setAuthToken(null);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [authEnabled, authMaterial.issuerPkB64, authMaterial.localTokensB64.join(","), docId]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const inviteB64 = new URLSearchParams(window.location.hash.slice(1)).get("invite");
    if (!inviteB64) return;

    void (async () => {
      try {
        const payload = decodeInvitePayload(inviteB64);
        if (payload.docId !== docId) {
          throw new Error(`invite doc mismatch: got ${payload.docId}, expected ${docId}`);
        }

        if (payload.payloadKeyB64) {
          await saveDocPayloadKeyB64(docId, payload.payloadKeyB64);
          await refreshDocPayloadKey();
        }

        await saveIssuerKeys(docId, payload.issuerPkB64);

        const localSk = base64urlDecode(payload.subjectSkB64);
        const localPk = await deriveEd25519PublicKey(localSk);
        await saveLocalKeys(docId, replicaLabel, base64urlEncode(localPk), payload.subjectSkB64);
        await saveLocalTokens(docId, replicaLabel, [payload.tokenB64]);

        // Clear hash so we don't re-import on refresh.
        const url = new URL(window.location.href);
        url.hash = "";
        window.history.replaceState({}, "", url);

        setAuthEnabled(true);
        await refreshAuthMaterial();
      } catch (err) {
        console.error("Failed to import invite", err);
        setAuthError(err instanceof Error ? err.message : String(err));
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [docId, replicaLabel]);

  useEffect(() => {
    if (!authEnabled) {
      localAuthRef.current = null;
      setAuthError(null);
      return;
    }

    let cancelled = false;
    void (async () => {
      try {
        const current = await loadAuthMaterial(docId, replicaLabel);
        let { issuerPkB64, issuerSkB64, localPkB64, localSkB64, localTokensB64 } = current;

        const ensureIssuerKeys = async (): Promise<Pick<StoredAuthMaterial, "issuerPkB64" | "issuerSkB64">> => {
            const run = async (): Promise<Pick<StoredAuthMaterial, "issuerPkB64" | "issuerSkB64">> => {
              let { issuerPkB64, issuerSkB64 } = await loadAuthMaterial(docId, replicaLabel);

              if (!issuerPkB64 && !issuerSkB64) {
                if (!joinMode) {
                  const { sk, pk } = await generateEd25519KeyPair();
                  await saveIssuerKeys(docId, base64urlEncode(pk), base64urlEncode(sk));
                }
              }

              // Reload in case another tab raced us.
              ({ issuerPkB64, issuerSkB64 } = await loadAuthMaterial(docId, replicaLabel));

            if (issuerSkB64) {
              // Treat issuer secret key as authoritative and force-sync the public key to match it.
              const issuerSk = base64urlDecode(issuerSkB64);
              const issuerPk = await deriveEd25519PublicKey(issuerSk);
              const issuerPkB64 = base64urlEncode(issuerPk);
              await saveIssuerKeys(docId, issuerPkB64, issuerSkB64, { forcePk: true });
            }

            const final = await loadAuthMaterial(docId, replicaLabel);
            return { issuerPkB64: final.issuerPkB64, issuerSkB64: final.issuerSkB64 };
          };

          const locks = typeof navigator === "undefined" ? null : (navigator as any).locks;
          if (locks?.request) {
            return await locks.request(prefixPlaygroundStorageKey(`treecrdt-playground-issuer:${docId}`), run);
          }

          // Fallback for browsers without Web Locks API.
          if (typeof window === "undefined") return await run();
          const lockKey = prefixPlaygroundStorageKey(`treecrdt-playground-issuer-lock:${docId}`);
          const lockId = typeof crypto !== "undefined" && "randomUUID" in crypto ? crypto.randomUUID() : `${Math.random()}`;
          const now = () => Date.now();
          const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
          const tryParseLock = (raw: string | null): { id: string; ts: number } | null => {
            if (!raw) return null;
            try {
              const parsed = JSON.parse(raw) as unknown;
              if (!parsed || typeof parsed !== "object") return null;
              const rec = parsed as Partial<{ id: unknown; ts: unknown }>;
              if (typeof rec.id !== "string" || typeof rec.ts !== "number") return null;
              return { id: rec.id, ts: rec.ts };
            } catch {
              return null;
            }
          };

          const ttlMs = 10_000;
          const started = now();
          while (true) {
            const t = now();
            const existing = tryParseLock(window.localStorage.getItem(lockKey));
            if (!existing || t - existing.ts > ttlMs) {
              window.localStorage.setItem(lockKey, JSON.stringify({ id: lockId, ts: t }));
            }
            const confirm = tryParseLock(window.localStorage.getItem(lockKey));
            if (confirm?.id === lockId) break;
            if (t - started > ttlMs) break;
            await sleep(25);
          }

          try {
            return await run();
          } finally {
            const confirm = tryParseLock(window.localStorage.getItem(lockKey));
            if (confirm?.id === lockId) window.localStorage.removeItem(lockKey);
          }
        };

        {
          const ensured = await ensureIssuerKeys();
          issuerPkB64 = ensured.issuerPkB64;
          issuerSkB64 = ensured.issuerSkB64;
        }

        const canIssue = Boolean(issuerSkB64);

        if (!localPkB64 && !localSkB64) {
          if (localTokensB64.length > 0) {
            throw new Error("auth enabled but local keys are missing; re-import an invite link or reset auth");
          }
          if (!canIssue) {
            throw new Error("auth enabled but no local keys/tokens; import an invite link");
          }
          const { sk, pk } = await generateEd25519KeyPair();
          localPkB64 = base64urlEncode(pk);
          localSkB64 = base64urlEncode(sk);
          await saveLocalKeys(docId, replicaLabel, localPkB64, localSkB64);
        } else if (!localPkB64 && localSkB64) {
          const localSk = base64urlDecode(localSkB64);
          const localPk = await deriveEd25519PublicKey(localSk);
          localPkB64 = base64urlEncode(localPk);
          await saveLocalKeys(docId, replicaLabel, localPkB64, localSkB64);
        } else if (localPkB64 && !localSkB64) {
          throw new Error("auth enabled but local private key is missing; import an invite link or reset auth");
        }

        if (localTokensB64.length === 0) {
          if (!canIssue || !issuerSkB64) {
            throw new Error("auth enabled but no capability token; import an invite link");
          }
          if (!localPkB64) throw new Error("auth enabled but local public key is missing");

          const issuerSk = base64urlDecode(issuerSkB64);
          const subjectPk = base64urlDecode(localPkB64);
          const tokenBytes = createCapabilityTokenV1({
            issuerPrivateKey: issuerSk,
            subjectPublicKey: subjectPk,
            docId,
            rootNodeId: ROOT_ID,
            actions: ["write_structure", "write_payload", "delete", "tombstone"],
          });
          localTokensB64 = [base64urlEncode(tokenBytes)];
          await saveLocalTokens(docId, replicaLabel, localTokensB64);
        }

        const next = await loadAuthMaterial(docId, replicaLabel);
        if (cancelled) return;
        setAuthMaterial(next);
        setAuthError(null);
      } catch (err) {
        if (cancelled) return;
        localAuthRef.current = null;
        setAuthError(err instanceof Error ? err.message : String(err));
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [authEnabled, client, docId, replicaLabel, joinMode]);

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
	            scopeEvaluator: createTreecrdtSqliteSubtreeScopeEvaluator(client.runner),
	            getLocalIdentityChain,
	            onPeerIdentityChain,
	          }
	        : null;

    if (authEnabled && !peerAuthConfig) {
      setSyncError(authError ?? "Auth enabled: initializing keys/tokens...");
      return;
    }

    setSyncError((prev) => (prev && prev.includes("initializing keys/tokens") ? null : prev));

    const debugSync =
      typeof window !== "undefined" && new URLSearchParams(window.location.search).has("debugSync");

    const channel = new BroadcastChannel(`treecrdt-sync-v0:${docId}`);
    const baseBackend = createPlaygroundBackend(client, docId, { enablePendingSidecar: authEnabled });
    const backend = {
      ...baseBackend,
      maxLamport: async () => BigInt(lamportRef.current),
      listOpRefs: async (filter: Filter) => {
        const refs = await baseBackend.listOpRefs(filter);
        if (debugSync) {
          const name =
            "all" in filter
              ? "all"
              : `children(${bytesToHex(filter.children.parent)})`;
          console.debug(`[sync:${replicaLabel}] listOpRefs(${name}) -> ${refs.length}`);
        }
        return refs;
      },
      applyOps: async (ops: Operation[]) => {
        if (debugSync && ops.length > 0) {
          console.debug(`[sync:${replicaLabel}] applyOps(${ops.length})`);
        }
        await baseBackend.applyOps(ops);
        await ingestPayloadOps(ops);
        ingestOps(ops);
        if (ops.length > 0) {
          let max = 0;
          for (const op of ops) max = Math.max(max, op.meta.lamport);
          lamportRef.current = Math.max(lamportRef.current, max);
          setHeadLamport(lamportRef.current);
        }
        scheduleRefreshParents(Object.keys(treeStateRef.current.childrenByParent));
        scheduleRefreshNodeCount();
      },
    };
    const connections = new Map<string, { transport: DuplexTransport<any>; peer: SyncPeer<Operation>; detach: () => void }>();
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
        peer_id: replicaLabel,
        to_peer_id: toPeerId,
        ts: Date.now(),
      } as const satisfies PresenceAckMessage;
      channel.postMessage(msg);
    };

    const ensureAckSent = (peerId: string) => {
      if (!peerId || peerId === replicaLabel) return;
      if (peerAckSentRef.current.has(peerId)) return;
      peerAckSentRef.current.add(peerId);
      sendPresenceAck(peerId);
    };

    const ensureConnection = (peerId: string) => {
      if (!peerId || peerId === replicaLabel) return;
      if (connections.has(peerId)) return;

      const rawTransport = createBroadcastDuplex<Operation>(
        channel,
        replicaLabel,
        peerId,
        treecrdtSyncV0ProtobufCodec
      );
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
	      const peer = new SyncPeer<Operation>(backend, {
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
      const detach = peer.attach(transport);
      connections.set(peerId, { transport, peer, detach });

      maybeStartLiveForPeer(peerId);
    };

    const onBroadcast = (ev: MessageEvent<any>) => {
      const data = ev.data as unknown;
      if (!data || typeof data !== "object") return;
      const msg = data as Partial<PresenceMessage | PresenceAckMessage>;

      if (msg.t === "presence") {
        if (typeof msg.peer_id !== "string" || typeof msg.ts !== "number") return;
        if (msg.peer_id === replicaLabel) return;
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
        if (msg.peer_id === replicaLabel) return;
        if (toPeerId !== replicaLabel) return;
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
      const msg: PresenceMessage = { t: "presence", peer_id: replicaLabel, ts: Date.now() };
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
  }, [authEnabled, authError, authMaterial, client, docId, getLocalIdentityChain, onPeerIdentityChain, replicaLabel, revealIdentity, status]);

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

  useEffect(() => {
    liveChildrenParentsRef.current = liveChildrenParents;

    const connections = syncConnRef.current;

    for (const peerId of connections.keys()) {
      for (const parentId of liveChildrenParents) {
        startLiveChildren(peerId, parentId);
      }
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
  }, [liveChildrenParents]);

  useEffect(() => {
    liveAllEnabledRef.current = liveAllEnabled;
    const connections = syncConnRef.current;
    if (liveAllEnabled) {
      for (const peerId of connections.keys()) startLiveAll(peerId);
    } else {
      stopAllLiveAll();
    }
  }, [liveAllEnabled]);

  useEffect(() => {
    if (!authCanSyncAll && liveAllEnabled) setLiveAllEnabled(false);
  }, [authCanSyncAll, liveAllEnabled]);

  const verifyLocalOps = async (ops: Operation[]) => {
    if (!authEnabled) return;
    const auth = localAuthRef.current;
    if (!auth?.signOps || !auth.verifyOps) throw new Error("auth is enabled but not configured");
    const ctx = { docId, purpose: "reconcile" as const, filterId: "__local__" };
    const authEntries = await auth.signOps(ops, ctx);
    const res = await auth.verifyOps(ops, authEntries, ctx);
    const dispositions = (res as any)?.dispositions as Array<{ status: string; message?: string }> | undefined;
    const rejected = dispositions?.find((d) => d.status !== "allow");
    if (rejected?.status === "pending_context") {
      throw new Error(rejected.message ?? "missing subtree context to authorize op");
    }
  };

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

      for (const conn of syncConnRef.current.values()) void conn.peer.notifyLocalUpdate();
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
    setBusy(true);
    try {
      const stateBefore = treeStateRef.current;
      const placement = after ? { type: "after" as const, after } : { type: "first" as const };
      const op = await client.local.move(replica, nodeId, newParent, placement);
      for (const conn of syncConnRef.current.values()) void conn.peer.notifyLocalUpdate();
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
    const normalizedCount = Math.max(0, Math.min(MAX_COMPOSER_NODE_COUNT, Math.floor(count)));
    if (normalizedCount <= 0) return;
    setBusy(true);
    try {
      const stateBefore = treeStateRef.current;
      const ops: Operation[] = [];
      const fanoutLimit = Math.max(0, Math.floor(opts.fanout ?? fanout));
      const valueBase = newNodeValue.trim();
      const shouldSetValue = valueBase.length > 0;

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

      for (const conn of syncConnRef.current.values()) void conn.peer.notifyLocalUpdate();
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
    setBusy(true);
    try {
      const stateBefore = treeStateRef.current;
      const valueBase = newNodeValue.trim();
      const payload = valueBase.length > 0 ? textEncoder.encode(valueBase) : null;
      const encryptedPayload = await encryptPayloadBytes(payload);
      const nodeId = makeNodeId();
      const op = await client.local.insert(replica, parentId, nodeId, { type: "last" }, encryptedPayload);
      for (const conn of syncConnRef.current.values()) void conn.peer.notifyLocalUpdate();
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
    const siblings = childrenByParent[ROOT_ID] ?? [];
    const without = siblings.filter((id) => id !== nodeId);
    const after = without.length === 0 ? null : without[without.length - 1]!;
    await appendMoveAfter(nodeId, ROOT_ID, after);
  };

  const handleSync = async (filter: Filter) => {
    if (!onlineRef.current) {
      setSyncError("Offline: toggle Online to sync.");
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
      for (const conn of connections.values()) {
        await conn.peer.syncOnce(conn.transport, filter, {
          maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
          maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
          codewordsPerMessage: 2048,
        });
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
    const parentIds = Array.from(parents).filter((id) => /^[0-9a-f]{32}$/i.test(id));
    parentIds.sort();

    if (!onlineRef.current) {
      setSyncError("Offline: toggle Online to sync.");
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
      for (const conn of connections.values()) {
        for (const parentId of parentIds) {
          await conn.peer.syncOnce(
            conn.transport,
            { children: { parent: hexToBytes16(parentId) } },
            {
              maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
              maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
              codewordsPerMessage: 2048,
            }
          );
        }
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

  const handleReset = async () => {
    stopAllLiveAll();
    stopAllLiveChildren();
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

  const stateBadge = status === "ready" ? "bg-emerald-500/80" : status === "error" ? "bg-rose-500/80" : "bg-amber-400/80";
  const peerTotal = peers.length + 1;
  const authCanIssue = Boolean(authMaterial.issuerSkB64);
  const authIssuerPkHex = authMaterial.issuerPkB64 ? bytesToHex(base64urlDecode(authMaterial.issuerPkB64)) : null;
  const authLocalKeyIdHex = authMaterial.localPkB64
    ? bytesToHex(deriveKeyIdV1(base64urlDecode(authMaterial.localPkB64)))
    : null;
  const authLocalTokenIdHex =
    authMaterial.localTokensB64.length > 0
      ? bytesToHex(deriveTokenIdV1(base64urlDecode(authMaterial.localTokensB64[0]!)))
      : null;
  const authTokenScope = authToken?.caps?.[0]?.res ?? null;
  const authTokenActions = authToken?.caps?.[0]?.actions ?? null;
  const localReplicaHex = replica ? bytesToHex(replica) : null;
  const deviceWrapKeyB64 = getDeviceWrapKeyB64();
  const sealedIssuerKeyB64 = getSealedIssuerKeyB64(docId);
  const sealedIdentityKeyB64 = getSealedIdentityKeyB64();
  const sealedDeviceSigningKeyB64 = getSealedDeviceSigningKeyB64();

  return (
    <div className="mx-auto max-w-6xl px-4 pb-12 pt-8 space-y-6">
      <header className="flex flex-col gap-3 rounded-2xl bg-slate-900/60 p-6 shadow-xl shadow-black/20 ring-1 ring-slate-800/60 backdrop-blur">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2 text-lg font-semibold text-slate-50">TreeCRDT</div>
        </div>
        <div className="flex flex-col items-start gap-3 text-xs text-slate-400">
          <div>
            Peer: <span className="font-mono text-slate-200">{replicaLabel}</span>
          </div>
          <div>
            Replica (pubkey):{" "}
            <span className="font-mono text-slate-200">{replica ? bytesToHex(replica) : "(initializing)"}</span>
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <button
            className="rounded-lg border border-slate-700 px-3 py-2 text-sm font-semibold text-slate-200 transition hover:border-slate-500 hover:text-white"
            onClick={handleReset}
            disabled={status !== "ready"}
          >
            Reset session
          </button>
          <button
            className="rounded-lg border border-slate-700 px-3 py-2 text-sm font-semibold text-slate-200 transition hover:border-slate-500 hover:text-white"
            onClick={expandAll}
          >
            Expand all
          </button>
          <button
            className="rounded-lg border border-slate-700 px-3 py-2 text-sm font-semibold text-slate-200 transition hover:border-slate-500 hover:text-white"
            onClick={collapseAll}
          >
            Collapse all
          </button>
        </div>
        <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-slate-300">
          <div className="flex items-center gap-2">
            <span className="text-[11px] font-semibold text-slate-200">Memory</span>
            <button
              type="button"
              className={`relative h-7 w-12 rounded-full border border-slate-700 transition ${
                storage === "opfs" ? "bg-emerald-500/30" : "bg-slate-800/70"
              } ${!opfsSupport.available ? "opacity-70" : "hover:border-accent"}`}
              onClick={() => handleStorageToggle(storage === "opfs" ? "memory" : "opfs")}
              disabled={status === "booting"}
              title={
                opfsSupport.available
                  ? "Toggle storage (memory ↔ persistent OPFS)"
                  : "Will attempt OPFS via worker; may fall back to memory if browser blocks sync handles."
              }
              aria-label={storage === "opfs" ? "Switch to in-memory storage" : "Switch to persistent storage (OPFS)"}
            >
              <span
                className={`absolute top-0.5 h-5 w-5 rounded-full bg-white transition ${
                  storage === "opfs" ? "right-0.5" : "left-0.5"
                }`}
              />
            </button>
            <span className="text-[11px] font-semibold text-slate-200">Persistent</span>
          </div>
          <span className={`rounded-full px-3 py-1 text-xs font-semibold text-slate-900 ${stateBadge}`}>
            {status === "ready"
              ? storage === "opfs"
                ? "Ready (OPFS)"
                : "Ready (memory)"
              : status === "booting"
                ? "Starting wasm"
                : "Error"}
          </span>
        </div>
        {error && <div className="rounded-lg border border-rose-400/50 bg-rose-500/10 px-4 py-2 text-sm text-rose-50">{error}</div>}
      </header>

      <div className="grid gap-6 md:grid-cols-3">
        <section className={`${showOpsPanel ? "md:col-span-2" : "md:col-span-3"} space-y-4`}>
          <div className="rounded-2xl bg-slate-900/60 p-5 shadow-lg shadow-black/20 ring-1 ring-slate-800/60">
            <div className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-400">Composer</div>
            <form
              className="flex flex-col gap-3 md:flex-row md:items-end"
              onSubmit={(e) => {
                e.preventDefault();
                void handleAddNodes(parentChoice, nodeCount, { fanout });
              }}
            >
              <ParentPicker nodeList={nodeList} value={parentChoice} onChange={setParentChoice} disabled={status !== "ready"} />
              <label className="w-full md:w-52 space-y-2 text-sm text-slate-200">
                <span>Value (optional)</span>
                <input
                  type="text"
                  value={newNodeValue}
                  onChange={(e) => setNewNodeValue(e.target.value)}
                  placeholder="Stored as payload bytes"
                  className="w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                  disabled={status !== "ready" || busy}
                />
              </label>
              <label className="flex flex-col text-sm text-slate-200">
                <span>Node count</span>
                <input
                  type="number"
                  min={1}
                  max={MAX_COMPOSER_NODE_COUNT}
                  value={nodeCount}
                  onChange={(e) => {
                    const next = Number(e.target.value);
                    if (!Number.isFinite(next)) {
                      setNodeCount(0);
                      return;
                    }
                    setNodeCount(Math.max(0, Math.min(MAX_COMPOSER_NODE_COUNT, Math.floor(next))));
                  }}
                  className="w-28 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                  disabled={status !== "ready" || busy}
                />
              </label>
              <label className="flex flex-col text-sm text-slate-200">
                <span>Fanout</span>
                <select
                  value={fanout}
                  onChange={(e) => setFanout(Number(e.target.value) || 0)}
                  className="w-28 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                  disabled={status !== "ready" || busy}
                  title="Fanout > 0 distributes nodes in a k-ary tree; 0 inserts all nodes under the chosen parent."
                >
                  <option value={0}>Flat</option>
                  <option value={2}>2</option>
                  <option value={3}>3</option>
                  <option value={5}>5</option>
                  <option value={10}>10</option>
                  <option value={20}>20</option>
                  <option value={50}>50</option>
                </select>
              </label>
              <button
                type="submit"
                className="flex-shrink-0 rounded-lg bg-accent px-4 py-3 text-sm font-semibold text-white shadow-lg shadow-accent/30 transition hover:-translate-y-0.5 hover:bg-accent/90 disabled:opacity-50"
                disabled={status !== "ready" || busy || nodeCount <= 0}
              >
                Add node{nodeCount > 1 ? "s" : ""}
              </button>
            </form>
          </div>

          <div className="rounded-2xl bg-slate-900/60 p-5 shadow-lg shadow-black/20 ring-1 ring-slate-800/60">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
	            <div className="flex items-center gap-3">
	              <div className="text-sm font-semibold uppercase tracking-wide text-slate-400">Tree</div>
		              <div className="text-xs text-slate-500">
		                {totalNodes === null ? "…" : totalNodes} nodes
		                <span className="text-slate-600"> · {nodeList.length - 1} loaded</span>
		                {privateRootsCount > 0 && (
		                  <span className="text-slate-600">
		                    {" "}
		                    · <MdLockOutline className="inline text-[14px]" /> {privateRootsCount} private roots
		                  </span>
		                )}
		              </div>
			            </div>
		            <div className="flex items-center gap-2">
                  <button
                    className={`flex h-9 items-center gap-2 rounded-lg border px-3 text-xs font-semibold transition disabled:opacity-50 ${
                      online
                        ? "border-slate-700 bg-slate-800/70 text-slate-200 hover:border-accent hover:text-white"
                        : "border-amber-500/60 bg-amber-500/10 text-amber-100 hover:border-amber-400"
                    }`}
                    onClick={() => setOnline((v) => !v)}
                    disabled={status !== "ready" || busy}
                    title={online ? "Go offline (simulate no sync)" : "Go online (resume sync)"}
                    type="button"
                  >
                    {online ? <MdCloudQueue className="text-[18px]" /> : <MdCloudOff className="text-[18px]" />}
                    <span>{online ? "Online" : "Offline"}</span>
                  </button>
                <button
                  className="flex h-9 items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                  onClick={() => void (authCanSyncAll ? handleSync({ all: {} }) : handleScopedSync())}
                  disabled={status !== "ready" || busy || syncBusy || peers.length === 0 || !online}
                  title={authCanSyncAll ? "Sync all (one-shot)" : "Sync loaded parents (scoped)"}
                >
                  <MdSync className="text-[18px]" />
                  <span>Sync</span>
                </button>
                <button
                  className={`flex h-9 w-9 items-center justify-center rounded-lg border text-slate-200 transition disabled:opacity-50 ${
                    liveAllEnabled
                      ? "border-accent bg-accent/20 text-white shadow-sm shadow-accent/20"
                      : "border-slate-700 bg-slate-800/70 hover:border-accent hover:text-white"
                  }`}
                  onClick={() => setLiveAllEnabled((v) => !v)}
                  disabled={status !== "ready" || busy || !online || !authCanSyncAll}
                  aria-label="Live sync all"
                  aria-pressed={liveAllEnabled}
                  title={authCanSyncAll ? "Live sync all (polling)" : "Live sync all is not allowed by this token scope"}
                >
                  <MdOutlineRssFeed className="text-[20px]" />
                </button>
                <button
                  className={`flex h-9 items-center gap-2 rounded-lg border px-3 text-xs font-semibold transition ${
                    showPeersPanel
                      ? "border-slate-600 bg-slate-800/90 text-white"
                      : "border-slate-700 bg-slate-800/70 text-slate-200 hover:border-accent hover:text-white"
                  }`}
                  onClick={() => setShowPeersPanel((v) => !v)}
                  type="button"
                  title="Peers"
                >
                  <MdGroup className="text-[18px]" />
                  <span className="font-mono">{peerTotal}</span>
                </button>
                <button
                  className={`flex h-9 items-center gap-2 rounded-lg border px-3 text-xs font-semibold transition ${
                    showAuthPanel
                      ? "border-slate-600 bg-slate-800/90 text-white"
                      : authEnabled
                        ? "border-emerald-400/60 bg-emerald-500/10 text-white hover:border-accent"
                        : "border-slate-700 bg-slate-800/70 text-slate-200 hover:border-accent hover:text-white"
                  }`}
                  onClick={() => setShowAuthPanel((v) => !v)}
                  type="button"
                  title="Auth / ACL"
                >
                  <MdVpnKey className="text-[18px]" />
                  <span>Auth</span>
                </button>
                <button
                  className="flex h-9 w-9 items-center justify-center rounded-lg border border-slate-700 bg-slate-800/70 text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                  onClick={openNewPeerTab}
                  disabled={typeof window === "undefined"}
                  type="button"
                  title="Open a new peer tab"
                >
                  <MdOpenInNew className="text-[18px]" />
                </button>
                <button
                  className="flex h-9 w-9 items-center justify-center rounded-lg border border-slate-700 bg-slate-800/70 text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                  onClick={openNewIsolatedPeerTab}
                  disabled={typeof window === "undefined"}
                  type="button"
                  title="Open an isolated peer tab (separate storage namespace; requires invite)"
                >
                  <MdLockOutline className="text-[18px]" />
                </button>
                <button
                  className={`flex h-9 w-9 items-center justify-center rounded-lg border text-slate-200 transition ${
                    showOpsPanel
                      ? "border-slate-600 bg-slate-800/90 text-white"
                      : "border-slate-700 bg-slate-800/70 hover:border-accent hover:text-white"
                  }`}
                  onClick={() => setShowOpsPanel((v) => !v)}
                  type="button"
                  title="Toggle operations panel"
                >
                  <IoMdGitBranch className="text-[18px]" />
                </button>
              </div>
            </div>
            {syncError && (
              <div
                data-testid="sync-error"
                className="mb-3 rounded-lg border border-rose-400/50 bg-rose-500/10 px-3 py-2 text-xs text-rose-50"
              >
                {syncError}
              </div>
            )}
            {showPeersPanel && (
              <div className="mb-3 rounded-xl border border-slate-800/80 bg-slate-950/40 p-3 text-xs text-slate-300">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Document</div>
                    <div className="font-mono text-slate-200">{docId}</div>
                  </div>
                  <div className="text-right">
                    <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Peer</div>
                    <div className="font-mono text-slate-200">{replicaLabel}</div>
                  </div>
                </div>
                <div className="mt-3 flex items-center justify-between gap-2">
                  <div className="text-slate-400">Peers</div>
                  <button
                    className="rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white"
                    type="button"
                    onClick={openNewPeerTab}
                  >
                    Create peer
                  </button>
                </div>
                <div className="mt-2 max-h-32 overflow-auto pr-1">
                  <div className="flex items-center justify-between gap-2 py-1">
                    <span className="font-mono text-slate-200">
                      {replicaLabel} <span className="text-[10px] text-slate-500">(you)</span>
                    </span>
                    <span className="text-[10px] text-slate-500">-</span>
                  </div>
                  {peers.map((p) => (
                    <div key={p.id} className="flex items-center justify-between gap-2 py-1">
                      <span className="font-mono text-slate-200">{p.id}</span>
                      <span className="text-[10px] text-slate-500">{Math.max(0, Date.now() - p.lastSeen)}ms</span>
                    </div>
                  ))}
                </div>
                {peers.length === 0 && (
                  <div className="mt-2 text-slate-500">
                    Only you right now. Open another tab (same `doc`, different `replica`).
                  </div>
                )}
              </div>
            )}
            {showAuthPanel && (
              <div className="mb-3 rounded-xl border border-slate-800/80 bg-slate-950/40 p-3 text-xs text-slate-300">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Auth (COSE+CWT)</div>
                    <div className="mt-1 text-[11px] text-slate-400">
                      {authEnabled ? "Enabled (ops must be signed and authorized)" : "Disabled (no signature/ACL checks)"}
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      className={`rounded-lg border px-3 py-1.5 text-xs font-semibold transition ${
                        authEnabled
                          ? "border-emerald-400/60 bg-emerald-500/10 text-emerald-50 hover:border-accent"
                          : "border-slate-700 bg-slate-800/70 text-slate-200 hover:border-accent hover:text-white"
                      }`}
                      type="button"
                      onClick={() => setAuthEnabled((v) => !v)}
                      disabled={authBusy}
                    >
                      {authEnabled ? "Disable" : "Enable"}
                    </button>
                    <button
                      className="rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                      type="button"
                      onClick={resetAuth}
                      disabled={authBusy}
                      title="Clears this tab's auth keys/tokens"
                    >
                      Reset
                    </button>
                  </div>
                </div>

                {authError && (
                  <div className="mt-3 rounded-lg border border-rose-400/50 bg-rose-500/10 px-3 py-2 text-xs text-rose-50">
                    {authError}
                  </div>
                )}

                <div className="mt-3 grid grid-cols-1 gap-2 md:grid-cols-3">
                  <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 px-3 py-2">
                    <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Issuer</div>
                    <div className="mt-1 font-mono text-slate-200">
                      {authIssuerPkHex ? `${authIssuerPkHex.slice(0, 16)}…` : "-"}
                    </div>
                    <div className="mt-1 text-[11px] text-slate-500">{authCanIssue ? "can mint invites" : "verify-only"}</div>
                  </div>
                  <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 px-3 py-2">
                    <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Local key_id</div>
                    <div className="mt-1 font-mono text-slate-200">
                      {authLocalKeyIdHex ? `${authLocalKeyIdHex.slice(0, 16)}…` : "-"}
                    </div>
                  </div>
                  <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 px-3 py-2">
                    <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Token id</div>
                    <div className="mt-1 font-mono text-slate-200">
                      {authLocalTokenIdHex ? `${authLocalTokenIdHex.slice(0, 16)}…` : "-"}
                    </div>
                    <div className="mt-1 text-[11px] text-slate-500">
                      {authTokenScope
                        ? (() => {
                            const rootId = authTokenScope.rootNodeId ?? ROOT_ID;
                            return `scope=${rootId === ROOT_ID ? "doc-wide" : `${rootId.slice(0, 8)}…`}${
                              authTokenScope.maxDepth !== undefined ? ` depth≤${authTokenScope.maxDepth}` : ""
                            }${
                              authTokenScope.excludeNodeIds && authTokenScope.excludeNodeIds.length > 0
                                ? ` exclude=${authTokenScope.excludeNodeIds.length}`
                                : ""
                            }`;
                          })()
                        : "-"}
                    </div>
                    {authTokenActions && authTokenActions.length > 0 && (
                      <div className="mt-1 text-[11px] text-slate-500" title={authTokenActions.join(", ")}>
                        {authTokenActions.join(", ")}
                      </div>
                    )}
                  </div>
                </div>

                <div className="mt-3 grid grid-cols-1 gap-2 md:grid-cols-2">
                  <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
                    <div className="flex items-center justify-between gap-2">
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Device wrap key</div>
                      <button
                        className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                        type="button"
                        onClick={() =>
                          void copyToClipboard(deviceWrapKeyB64 ?? "").catch((err) =>
                            setAuthError(err instanceof Error ? err.message : String(err))
                          )
                        }
                        disabled={authBusy || !deviceWrapKeyB64}
                        title="Copy device wrap key"
                      >
                        <MdContentCopy className="text-[16px]" />
                        Copy
                      </button>
                    </div>
                    <div className="mt-1 font-mono text-slate-200" title={deviceWrapKeyB64 ?? ""}>
                      {deviceWrapKeyB64 ? `${deviceWrapKeyB64.slice(0, 24)}…` : "(initializing)"}
                    </div>
                    <div className="mt-2 flex flex-col gap-2 sm:flex-row sm:items-center">
                      <input
                        type="text"
                        value={wrapKeyImportText}
                        onChange={(e) => setWrapKeyImportText(e.target.value)}
                        placeholder="Paste base64url wrap key"
                        className="w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                        disabled={authBusy}
                      />
                      <button
                        className="rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-white shadow-lg shadow-accent/30 transition hover:bg-accent/90 disabled:opacity-50"
                        type="button"
                        onClick={() => {
                          try {
                            importDeviceWrapKeyB64(wrapKeyImportText);
                            setWrapKeyImportText("");
                            void refreshAuthMaterial().catch((err) =>
                              setAuthError(err instanceof Error ? err.message : String(err))
                            );
                          } catch (err) {
                            setAuthError(err instanceof Error ? err.message : String(err));
                          }
                        }}
                        disabled={authBusy || wrapKeyImportText.trim().length === 0}
                        title="Import device wrap key"
                      >
                        Import
                      </button>
                    </div>
                    <div className="mt-1 text-[11px] text-slate-500">
                      Back up this key (e.g. Supabase). Needed to decrypt doc key blobs.
                    </div>
                  </div>

                  <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
                    <div className="flex items-center justify-between gap-2">
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Doc key blob</div>
                      <button
                        className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                        type="button"
                        onClick={() =>
                          void copyToClipboard(sealedIssuerKeyB64 ?? "").catch((err) =>
                            setAuthError(err instanceof Error ? err.message : String(err))
                          )
                        }
                        disabled={authBusy || !sealedIssuerKeyB64}
                        title="Copy sealed issuer key blob (base64url)"
                      >
                        <MdContentCopy className="text-[16px]" />
                        Copy
                      </button>
                    </div>
                    <div className="mt-1 font-mono text-slate-200" title={sealedIssuerKeyB64 ?? ""}>
                      {sealedIssuerKeyB64 ? `${sealedIssuerKeyB64.slice(0, 24)}…` : "-"}
                    </div>
                    <div className="mt-2 flex flex-col gap-2 sm:flex-row sm:items-center">
                      <input
                        type="text"
                        value={issuerKeyBlobImportText}
                        onChange={(e) => setIssuerKeyBlobImportText(e.target.value)}
                        placeholder="Paste sealed issuer key blob (base64url)"
                        className="w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                        disabled={authBusy}
                      />
                      <button
                        className="rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-white shadow-lg shadow-accent/30 transition hover:bg-accent/90 disabled:opacity-50"
                        type="button"
                        onClick={() => {
                          try {
                            setSealedIssuerKeyB64(docId, issuerKeyBlobImportText);
                            setIssuerKeyBlobImportText("");
                            void refreshAuthMaterial().catch((err) =>
                              setAuthError(err instanceof Error ? err.message : String(err))
                            );
                          } catch (err) {
                            setAuthError(err instanceof Error ? err.message : String(err));
                          }
                        }}
                        disabled={authBusy || issuerKeyBlobImportText.trim().length === 0}
                        title="Import sealed issuer key blob"
                      >
                        Import
                      </button>
                    </div>
                    <div className="mt-1 text-[11px] text-slate-500">
                      Encrypted at rest. Bound to this `docId` via AAD.
                    </div>
                  </div>
                </div>

                <div className="mt-3 grid grid-cols-1 gap-2 md:grid-cols-2">
                  <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
                    <div className="flex items-center justify-between gap-2">
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Identity key blob</div>
                      <button
                        className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                        type="button"
                        onClick={() =>
                          void copyToClipboard(sealedIdentityKeyB64 ?? "").catch((err) =>
                            setAuthError(err instanceof Error ? err.message : String(err))
                          )
                        }
                        disabled={authBusy || !sealedIdentityKeyB64}
                        title="Copy sealed identity key blob (base64url)"
                      >
                        <MdContentCopy className="text-[16px]" />
                        Copy
                      </button>
                    </div>
                    <div className="mt-1 font-mono text-slate-200" title={sealedIdentityKeyB64 ?? ""}>
                      {sealedIdentityKeyB64 ? `${sealedIdentityKeyB64.slice(0, 24)}…` : "-"}
                    </div>
                    <div className="mt-2 flex flex-col gap-2 sm:flex-row sm:items-center">
                      <input
                        type="text"
                        value={identityKeyBlobImportText}
                        onChange={(e) => setIdentityKeyBlobImportText(e.target.value)}
                        placeholder="Paste sealed identity key blob (base64url)"
                        className="w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                        disabled={authBusy}
                      />
                      <button
                        className="rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-white shadow-lg shadow-accent/30 transition hover:bg-accent/90 disabled:opacity-50"
                        type="button"
                        onClick={() => {
                          try {
                            setSealedIdentityKeyB64(identityKeyBlobImportText);
                            setIdentityKeyBlobImportText("");
                            localIdentityChainPromiseRef.current = null;
                          } catch (err) {
                            setAuthError(err instanceof Error ? err.message : String(err));
                          }
                        }}
                        disabled={authBusy || identityKeyBlobImportText.trim().length === 0}
                        title="Import sealed identity key blob"
                      >
                        Import
                      </button>
                    </div>
                    <div className="mt-1 text-[11px] text-slate-500">
                      Encrypted at rest. Requires the device wrap key to open.
                    </div>
                  </div>

                  <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
                    <div className="flex items-center justify-between gap-2">
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                        Device signing key blob
                      </div>
                      <button
                        className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                        type="button"
                        onClick={() =>
                          void copyToClipboard(sealedDeviceSigningKeyB64 ?? "").catch((err) =>
                            setAuthError(err instanceof Error ? err.message : String(err))
                          )
                        }
                        disabled={authBusy || !sealedDeviceSigningKeyB64}
                        title="Copy sealed device signing key blob (base64url)"
                      >
                        <MdContentCopy className="text-[16px]" />
                        Copy
                      </button>
                    </div>
                    <div className="mt-1 font-mono text-slate-200" title={sealedDeviceSigningKeyB64 ?? ""}>
                      {sealedDeviceSigningKeyB64 ? `${sealedDeviceSigningKeyB64.slice(0, 24)}…` : "-"}
                    </div>
                    <div className="mt-2 flex flex-col gap-2 sm:flex-row sm:items-center">
                      <input
                        type="text"
                        value={deviceSigningKeyBlobImportText}
                        onChange={(e) => setDeviceSigningKeyBlobImportText(e.target.value)}
                        placeholder="Paste sealed device signing key blob (base64url)"
                        className="w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                        disabled={authBusy}
                      />
                      <button
                        className="rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-white shadow-lg shadow-accent/30 transition hover:bg-accent/90 disabled:opacity-50"
                        type="button"
                        onClick={() => {
                          try {
                            setSealedDeviceSigningKeyB64(deviceSigningKeyBlobImportText);
                            setDeviceSigningKeyBlobImportText("");
                            localIdentityChainPromiseRef.current = null;
                          } catch (err) {
                            setAuthError(err instanceof Error ? err.message : String(err));
                          }
                        }}
                        disabled={authBusy || deviceSigningKeyBlobImportText.trim().length === 0}
                        title="Import sealed device signing key blob"
                      >
                        Import
                      </button>
                    </div>
                    <div className="mt-1 text-[11px] text-slate-500">
                      Encrypted at rest. Requires the device wrap key to open.
                    </div>
                  </div>
                </div>

                <div className="mt-3 rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
                  <div className="flex items-center justify-between gap-2">
                    <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Pending ops</div>
                    <button
                      className="rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                      type="button"
                      onClick={() => void refreshPendingOps()}
                      disabled={!authEnabled || authBusy || !client}
                      title="Fetch pending ops stored due to missing ancestry context"
                    >
                      Refresh
                    </button>
                  </div>
                  <div className="mt-2 text-[11px] text-slate-400">{pendingOps.length} pending</div>
		                  {pendingOps.length > 0 && (
		                    <div className="mt-2 max-h-28 overflow-auto pr-1">
		                      {pendingOps.map((p) => (
		                        <div key={p.id} className="flex items-center justify-between gap-2 py-1">
		                          <span className="font-mono text-[11px] text-slate-200">
		                            {p.id} <span className="text-slate-500">{p.kind}</span>
		                          </span>
		                          <span className="text-[10px] text-slate-500">{p.message ?? ""}</span>
		                        </div>
		                      ))}
		                    </div>
		                  )}
		                </div>

		                <div className="mt-3 rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
		                  <div className="flex items-center justify-between gap-2">
		                    <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Identity</div>
		                    <button
		                      className={`rounded-lg border px-3 py-1.5 text-xs font-semibold transition disabled:opacity-50 ${
		                        revealIdentity
		                          ? "border-amber-400/70 bg-amber-500/10 text-amber-100 hover:border-amber-300"
		                          : "border-slate-700 bg-slate-800/70 text-slate-200 hover:border-accent hover:text-white"
		                      }`}
		                      type="button"
		                      onClick={() => setRevealIdentity((v) => !v)}
		                      disabled={authBusy}
		                      title={
		                        revealIdentity
		                          ? "Stop advertising an identity chain (unlinkable by default)"
		                          : "Advertise an identity chain (identity→device→replica) so peers can attribute signatures"
		                      }
		                    >
		                      {revealIdentity ? "Revealing" : "Private"}
		                    </button>
		                  </div>
		                  <div className="mt-1 text-[11px] text-slate-500">
		                    When enabled, this tab advertises an identity chain so peers can attribute signatures. This is linkable across
		                    documents; keep disabled for unlinkable-by-default privacy.
		                  </div>
		                </div>

		                <div className="mt-3 rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
		                  <div className="flex items-center justify-between gap-2">
		                    <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Private subtrees</div>
		                    <button
	                      className="rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
	                      type="button"
	                      onClick={clearPrivateRoots}
	                      disabled={authBusy || privateRootsCount === 0}
	                      title="Clear all private roots for this doc (local only)"
	                    >
	                      Clear
	                    </button>
	                  </div>
	                  <div className="mt-2 text-[11px] text-slate-400">{privateRootsCount} private roots</div>
	                  <div className="mt-1 text-[11px] text-slate-500">
	                    Private roots are excluded from new invites (read/write scope). Stored locally for this `docId`.
	                  </div>
	                  {privateRootEntries.length > 0 && (
	                    <div className="mt-2 max-h-28 overflow-auto pr-1">
	                      {privateRootEntries.map((r) => (
	                        <div key={r.id} className="flex items-center justify-between gap-2 py-1">
	                          <span className="min-w-0 truncate font-mono text-[11px] text-slate-200" title={r.id}>
	                            {r.label} <span className="text-slate-500">{r.id.slice(0, 12)}…</span>
	                          </span>
	                          <div className="flex items-center gap-2">
	                            <button
	                              className="rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-[11px] font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
	                              type="button"
	                              onClick={() => togglePrivateRoot(r.id)}
	                              disabled={authBusy}
	                              title="Make public (remove from private roots)"
	                            >
	                              Make public
	                            </button>
	                            <button
	                              className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-[11px] font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
	                              type="button"
	                              onClick={() =>
	                                void copyToClipboard(r.id).catch((err) =>
	                                  setAuthError(err instanceof Error ? err.message : String(err))
	                                )
	                              }
	                              disabled={authBusy}
	                              title="Copy node id"
	                            >
	                              <MdContentCopy className="text-[14px]" />
	                              Copy
	                            </button>
	                          </div>
	                        </div>
	                      ))}
	                    </div>
	                  )}
	                </div>
	
	                <div className="mt-3 rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
	                  <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Create invite link</div>
	                  <div className="mt-2 flex flex-wrap items-end gap-3">
                    <label className="w-full md:w-60 space-y-2 text-sm text-slate-200">
                      <span>Subtree root</span>
                      <select
                        className="w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                        value={inviteRoot}
                        onChange={(e) => setInviteRoot(e.target.value)}
                        disabled={authBusy}
                      >
                        {nodeList.map(({ id, label, depth }) => (
                          <option key={id} value={id}>
                            {"".padStart(depth * 2, " ")}
                            {label}
                          </option>
                        ))}
                      </select>
                    </label>

                    <label className="w-full md:w-40 space-y-2 text-sm text-slate-200">
                      <span>Max depth (optional)</span>
                      <input
                        type="number"
                        min={0}
                        value={inviteMaxDepth}
                        onChange={(e) => setInviteMaxDepth(e.target.value)}
                        placeholder="∞"
                        className="w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                        disabled={authBusy}
                      />
                    </label>

                    <div className="flex flex-col gap-2 text-sm text-slate-200">
                      <span>Actions</span>
                      <div className="flex flex-wrap gap-3 text-xs text-slate-200">
                        {(["write_structure", "write_payload", "delete", "tombstone"] as const).map((name) => (
                          <label key={name} className="flex items-center gap-2">
                            <input
                              type="checkbox"
                              checked={inviteActions[name]}
                              onChange={(e) =>
                                setInviteActions((prev) => ({ ...prev, [name]: e.target.checked }))
                              }
                              disabled={authBusy}
                            />
                            <span className="font-mono text-[11px]">{name}</span>
                          </label>
                        ))}
                      </div>
                    </div>

                    <button
                      className="rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-white shadow-lg shadow-accent/30 transition hover:-translate-y-0.5 hover:bg-accent/90 disabled:opacity-50"
                      type="button"
                      onClick={() => void generateInviteLink()}
                      disabled={!authEnabled || authBusy || !authCanIssue}
                      title={authCanIssue ? "Generate an invite link" : "This tab cannot mint invites (issuer SK not present)"}
                    >
	                      Generate
	                    </button>
	                  </div>

	                  <div className="mt-2 text-[11px] text-slate-500">
	                    {inviteExcludeNodeIds.length === 0 ? (
	                      <span>No private roots are excluded from this invite.</span>
	                    ) : (
	                      <span>
	                        Excluding {inviteExcludeNodeIds.length} private root{inviteExcludeNodeIds.length === 1 ? "" : "s"} from this invite:{" "}
	                        {inviteExcludeNodeIds
	                          .slice(0, 3)
	                          .map((id) => nodeLabelForId(id))
	                          .join(", ")}
	                        {inviteExcludeNodeIds.length > 3 ? ` (+${inviteExcludeNodeIds.length - 3} more)` : ""}
	                      </span>
	                    )}
	                  </div>
	
	                  {inviteLink && (
	                    <div className="mt-3 flex flex-col gap-2">
	                      <div className="flex items-center justify-between gap-2">
                        <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Link</div>
                        <div className="flex items-center gap-2">
                          <button
                            className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                            type="button"
                            onClick={() =>
                              void copyToClipboard(inviteLink).catch((err) =>
                                setAuthError(err instanceof Error ? err.message : String(err))
                              )
                            }
                            disabled={authBusy}
                            title="Copy link"
                          >
                            <MdContentCopy className="text-[16px]" />
                            Copy
                          </button>
                          <button
                            className="rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                            type="button"
                            onClick={() => window.open(inviteLink, "_blank", "noopener,noreferrer")}
                            disabled={authBusy}
                          >
                            Open
                          </button>
                        </div>
                      </div>
                      <textarea
                        className="w-full rounded-lg border border-slate-700 bg-slate-900/60 px-3 py-2 font-mono text-[11px] text-slate-200 outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                        value={inviteLink}
                        readOnly
                        rows={2}
                      />
                      <div className="text-[11px] text-slate-500">
                        Open the link in a new tab to join as a new replica with the granted subtree scope.
                      </div>
                    </div>
                  )}
                </div>

                <div className="mt-3 rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
                  <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Import invite</div>
                  <div className="mt-2 flex flex-wrap items-end gap-2">
                    <textarea
                      className="w-full flex-1 rounded-lg border border-slate-700 bg-slate-900/60 px-3 py-2 font-mono text-[11px] text-slate-200 outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                      rows={2}
                      value={inviteImportText}
                      onChange={(e) => setInviteImportText(e.target.value)}
                      placeholder="Paste an invite URL (or invite=...)"
                      disabled={authBusy}
                    />
                    <button
                      className="rounded-lg border border-slate-700 bg-slate-800/70 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                      type="button"
                      onClick={() => void importInviteLink()}
                      disabled={authBusy || inviteImportText.trim().length === 0}
                    >
                      Import
                    </button>
                  </div>
                </div>
              </div>
            )}
            <div ref={treeParentRef} className="max-h-[560px] overflow-auto">
              <div
                style={{ height: `${treeVirtualizer.getTotalSize()}px`, position: "relative" }}
                className="w-full"
              >
                {treeVirtualizer.getVirtualItems().map((item) => {
                  const entry = visibleNodes[item.index];
                  if (!entry) return null;
                  return (
                    <div
                      key={item.key}
                      data-index={item.index}
                      ref={treeVirtualizer.measureElement}
                      className="absolute left-0 top-0 w-full"
                      style={{ transform: `translateY(${item.start}px)` }}
                    >
                      <TreeRow
                        node={entry.node}
                        depth={entry.depth}
                        collapse={collapse}
                        onToggle={toggleCollapse}
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
                        onTogglePrivateRoot={togglePrivateRoot}
                        liveChildren={liveChildrenParents.has(entry.node.id)}
                        meta={index}
                        childrenByParent={childrenByParent}
                      />
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </section>

        {showOpsPanel && (
          <aside className="space-y-3 rounded-2xl bg-slate-900/60 p-5 shadow-lg shadow-black/20 ring-1 ring-slate-800/60">
            <div className="text-sm font-semibold uppercase tracking-wide text-slate-400">Operations</div>
            <div className="flex items-center justify-between text-xs text-slate-400">
              <span>Ops: {ops.length}</span>
              <span>Head lamport: {headLamport}</span>
            </div>
            <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-3 shadow-inner shadow-black/30">
              <div ref={opsParentRef} className="max-h-[520px] overflow-auto pr-2 text-xs">
                {ops.length === 0 && <div className="text-slate-500">No operations yet.</div>}
                {ops.length > 0 && (
                  <div
                    style={{ height: `${opsVirtualizer.getTotalSize()}px`, position: "relative" }}
                    className="w-full"
                  >
                    {opsVirtualizer.getVirtualItems().map((item) => {
                      const op = ops[item.index];
                      if (!op) return null;
	                      const signerHex = bytesToHex(op.meta.id.replica);
	                      const signerShort =
	                        signerHex.length > 24 ? `${signerHex.slice(0, 12)}…${signerHex.slice(-8)}` : signerHex;
	                      const signerKeyIdHex = bytesToHex(deriveKeyIdV1(op.meta.id.replica));
	                      const signerKeyIdShort =
	                        signerKeyIdHex.length > 16 ? `${signerKeyIdHex.slice(0, 8)}…${signerKeyIdHex.slice(-4)}` : signerKeyIdHex;
	                      const identity = identityByReplicaRef.current.get(signerHex);
	                      const identityKeyIdHex = identity ? bytesToHex(deriveKeyIdV1(identity.identityPk)) : null;
	                      const identityKeyIdShort =
	                        identityKeyIdHex && identityKeyIdHex.length > 16
	                          ? `${identityKeyIdHex.slice(0, 8)}…${identityKeyIdHex.slice(-4)}`
	                          : identityKeyIdHex;
	                      const identityPkHex = identity ? bytesToHex(identity.identityPk) : null;
	                      const isLocalSigner = localReplicaHex ? signerHex === localReplicaHex : false;
	                      return (
                        <div
                          key={item.key}
                          data-index={item.index}
                          ref={opsVirtualizer.measureElement}
                          className="absolute left-0 top-0 w-full"
                          style={{ transform: `translateY(${item.start}px)` }}
                        >
                          <div className="mb-2 rounded-lg border border-slate-800/80 bg-slate-900/60 px-3 py-2 text-slate-100">
                            <div className="flex items-center justify-between">
                              <span className="font-semibold text-accent">{op.kind.type}</span>
                              <div className="flex items-center gap-2">
                                {authEnabled ? (
                                  <span
                                    className="rounded-full border border-emerald-400/40 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-semibold text-emerald-100"
                                    title={
                                      isLocalSigner
                                        ? "Auth enabled: this op will be signed when syncing"
                                        : "Auth enabled: this op was verified before apply"
                                    }
                                  >
                                    signed
                                  </span>
                                ) : (
                                  <span
                                    className="rounded-full border border-slate-700 bg-slate-800/60 px-2 py-0.5 text-[10px] font-semibold text-slate-300"
                                    title="Auth disabled: ops are not required to carry signatures/capabilities"
                                  >
                                    unsigned
                                  </span>
                                )}
                                <span className="font-mono text-slate-400">lamport {op.meta.lamport}</span>
                              </div>
                            </div>
                            <div className="mt-1 text-slate-300">{renderKind(op.kind)}</div>
                            <div className="mt-1 flex flex-wrap items-center justify-between gap-2 text-[10px] text-slate-500">
                              <span className="font-mono">counter {op.meta.id.counter}</span>
                              <span className="font-mono" title={signerHex}>
                                signer {signerShort}
                                {isLocalSigner ? " (local)" : ""}
                              </span>
                            </div>
	                            <div className="mt-0.5 text-[10px] text-slate-500">
	                              <span className="font-mono" title={signerKeyIdHex}>
	                                keyId {signerKeyIdShort}
	                              </span>
	                            </div>
	                            {identity && identityKeyIdHex && (
	                              <div className="mt-0.5 text-[10px] text-slate-500">
	                                <span className="font-mono" title={identityPkHex ?? ""}>
	                                  identity {identityKeyIdShort}
	                                </span>
	                              </div>
	                            )}
	                          </div>
	                        </div>
	                      );
                    })}
                  </div>
                )}
              </div>
            </div>
          </aside>
        )}
      </div>
    </div>
  );
}

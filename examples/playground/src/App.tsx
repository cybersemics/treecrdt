import React, { useEffect, useMemo, useRef, useState } from "react";
import { type Operation, type OperationKind } from "@treecrdt/interface";
import { bytesToHex } from "@treecrdt/interface/ids";
import { createTreecrdtClient, type TreecrdtClient } from "@treecrdt/wa-sqlite/client";
import { detectOpfsSupport } from "@treecrdt/wa-sqlite/opfs";
import { SyncPeer, type Filter, type SyncSubscription } from "@treecrdt/sync";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync/protobuf";
import type { DuplexTransport } from "@treecrdt/sync/transport";
import {
  MdCloudOff,
  MdCloudQueue,
  MdGroup,
  MdOpenInNew,
  MdOutlineRssFeed,
  MdSync,
} from "react-icons/md";
import { IoMdGitBranch } from "react-icons/io";

import { createBroadcastDuplex, createPlaygroundBackend, hexToBytes16, type PresenceMessage } from "./sync-v0";
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
  makeNodeId,
  makeSessionKey,
  persistDocId,
  persistOpfsKey,
  persistStorage,
  pickReplicaId,
} from "./playground/persist";
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

  const counterRef = useRef(0);
  const lamportRef = useRef(0);
  const onlineRef = useRef(true);
  const replicaId = useMemo(pickReplicaId, []);
  const opfsSupport = useMemo(detectOpfsSupport, []);
  const showOpsPanelRef = useRef(false);
  const textEncoder = useMemo(() => new TextEncoder(), []);
  const textDecoder = useMemo(() => new TextDecoder(), []);

  const replicaKey = useMemo(
    () => (replica: Operation["meta"]["id"]["replica"]) => (typeof replica === "string" ? replica : bytesToHex(replica)),
    []
  );

  const payloadByNodeRef = useRef<Map<string, PayloadRecord>>(new Map());

  const ingestPayloadOps = React.useCallback((incoming: Operation[]) => {
    if (incoming.length === 0) return;
    const payloads = payloadByNodeRef.current;
    let changed = false;
    for (const op of incoming) {
      const kind = op.kind;
      const node = kind.type === "payload" ? kind.node : kind.type === "insert" ? kind.node : null;
      const payload =
        kind.type === "payload"
          ? kind.payload
          : kind.type === "insert"
            ? kind.payload
            : undefined;
      if (!node || payload === undefined) continue;
      const candidate: PayloadRecord = {
        lamport: op.meta.lamport,
        replica: replicaKey(op.meta.id.replica),
        counter: op.meta.id.counter,
        payload,
      };
      const existing = payloads.get(node);
      if (!existing || compareOpMeta(candidate, existing) > 0) {
        payloads.set(node, candidate);
        changed = true;
      }
    }
    if (changed) setPayloadVersion((v) => v + 1);
  }, []);

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
            ingestPayloadOps(ops);
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
          active.meta.replicaMaxCounter(replicaId),
        ]);
        lamportRef.current = Math.max(lamportRef.current, lamport);
        setHeadLamport(lamportRef.current);
        counterRef.current = Math.max(counterRef.current, counter);
      } catch (err) {
        console.error("Failed to refresh meta", err);
      }
    },
    [client, replicaId]
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

  const makeNewReplicaId = () => `replica-${crypto.randomUUID().slice(0, 8)}`;

  const openNewPeerTab = () => {
    if (typeof window === "undefined") return;
    const url = new URL(window.location.href);
    url.searchParams.set("doc", docId);
    url.searchParams.set("replica", makeNewReplicaId());
    window.open(url.toString(), "_blank", "noopener,noreferrer");
  };

  const { index, childrenByParent } = treeState;

  const nodeLabelForId = React.useCallback(
    (id: string) => {
      if (id === ROOT_ID) return "Root";
      const payload = payloadByNodeRef.current.get(id)?.payload ?? null;
      const decoded = payload === null ? null : textDecoder.decode(payload);
      if (decoded === null) return id;
      return decoded.length === 0 ? "(empty)" : decoded;
    },
    [payloadVersion, textDecoder]
  );

  const nodeList = useMemo(() => flattenForSelectState(childrenByParent, nodeLabelForId), [childrenByParent, nodeLabelForId]);
  const visibleNodes = useMemo(() => {
    const acc: Array<{ node: DisplayNode; depth: number }> = [];
    const isCollapsed = (id: string) => {
      return collapse.defaultCollapsed ? !collapse.overrides.has(id) : collapse.overrides.has(id);
    };
    const stack: Array<{ id: string; depth: number }> = [{ id: ROOT_ID, depth: 0 }];
    while (stack.length > 0) {
      const entry = stack.pop();
      if (!entry) break;
      const payload = payloadByNodeRef.current.get(entry.id)?.payload ?? null;
      const value = payload === null ? "" : textDecoder.decode(payload);
      const label = entry.id === ROOT_ID ? "Root" : payload === null ? entry.id : value.length === 0 ? "(empty)" : value;
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
    if (!client || status !== "ready") return;
    if (!docId) return;
    if (typeof BroadcastChannel === "undefined") {
      setSyncError("BroadcastChannel is not available in this environment.");
      return;
    }

    const debugSync =
      typeof window !== "undefined" && new URLSearchParams(window.location.search).has("debugSync");

    const channel = new BroadcastChannel(`treecrdt-sync-v0:${docId}`);
    const baseBackend = createPlaygroundBackend(client, docId);
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
          console.debug(`[sync:${replicaId}] listOpRefs(${name}) -> ${refs.length}`);
        }
        return refs;
      },
      applyOps: async (ops: Operation[]) => {
        if (debugSync && ops.length > 0) {
          console.debug(`[sync:${replicaId}] applyOps(${ops.length})`);
        }
        await baseBackend.applyOps(ops);
        ingestPayloadOps(ops);
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

    const updatePeers = () => {
      setPeers(
        Array.from(lastSeen.entries())
          .map(([id, ts]) => ({ id, lastSeen: ts }))
          .sort((a, b) => (a.id === b.id ? 0 : a.id < b.id ? -1 : 1))
      );
    };

    const ensureConnection = (peerId: string) => {
      if (!peerId || peerId === replicaId) return;
      if (connections.has(peerId)) return;

      const rawTransport = createBroadcastDuplex<Operation>(
        channel,
        replicaId,
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
      });
      const detach = peer.attach(transport);
      connections.set(peerId, { transport, peer, detach });

      if (liveAllEnabledRef.current) startLiveAll(peerId);
      for (const parentId of liveChildrenParentsRef.current) {
        startLiveChildren(peerId, parentId);
      }
    };

    const onPresence = (ev: MessageEvent<any>) => {
      if (!onlineRef.current) return;
      const data = ev.data as unknown;
      if (!data || typeof data !== "object") return;
      const msg = data as Partial<PresenceMessage>;
      if (msg.t !== "presence") return;
      if (typeof msg.peer_id !== "string" || typeof msg.ts !== "number") return;
      if (msg.peer_id === replicaId) return;
      const wasNew = !lastSeen.has(msg.peer_id);
      lastSeen.set(msg.peer_id, msg.ts);
      ensureConnection(msg.peer_id);
      if (wasNew) {
        updatePeers();
      }
    };

    channel.addEventListener("message", onPresence);

    const sendPresence = () => {
      if (!onlineRef.current) return;
      const msg: PresenceMessage = { t: "presence", peer_id: replicaId, ts: Date.now() };
      channel.postMessage(msg);
    };

    sendPresence();
    updatePeers();
    const interval = window.setInterval(sendPresence, 1000);
    const pruneInterval = window.setInterval(() => {
      const now = Date.now();
      let changed = false;
      for (const [id, ts] of Array.from(lastSeen.entries())) {
        if (now - ts > PLAYGROUND_PEER_TIMEOUT_MS) {
          lastSeen.delete(id);
          const conn = connections.get(id);
          if (conn) {
            conn.detach();
            connections.delete(id);
          }
          stopLiveAllForPeer(id);
          stopLiveChildrenForPeer(id);
          changed = true;
        }
      }
      if (changed) {
        updatePeers();
      }
    }, 500);

    return () => {
      window.clearInterval(interval);
      window.clearInterval(pruneInterval);
      channel.removeEventListener("message", onPresence);
      stopAllLiveAll();
      stopAllLiveChildren();
      channel.close();

      for (const conn of connections.values()) {
        conn.detach();
        (conn.transport as any).close?.();
      }
      connections.clear();
      setPeers([]);
    };
  }, [client, docId, replicaId, status]);

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
      ingestPayloadOps(fetched);
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

  const appendOperation = async (kind: OperationKind) => {
    if (!client) return;
    setBusy(true);
    try {
      const stateBefore = treeStateRef.current;
      let op: Operation;
      if (kind.type === "payload") {
        op = await client.local.payload(replicaId, kind.node, kind.payload);
      } else if (kind.type === "delete") {
        op = await client.local.delete(replicaId, kind.node);
      } else {
        throw new Error(`unsupported operation kind: ${kind.type}`);
      }

      for (const conn of syncConnRef.current.values()) void conn.peer.notifyLocalUpdate();
      ingestPayloadOps([op]);
      ingestOps([op], { assumeSorted: true });
      scheduleRefreshParents(parentsAffectedByOps(stateBefore, [op]));
      scheduleRefreshNodeCount();
      lamportRef.current = Math.max(lamportRef.current, op.meta.lamport);
      counterRef.current = Math.max(counterRef.current, op.meta.id.counter);
      setHeadLamport(lamportRef.current);
    } catch (err) {
      console.error("Failed to append op", err);
      setError("Failed to append operation (see console)");
    } finally {
      setBusy(false);
    }
  };

  const appendMoveAfter = async (nodeId: string, newParent: string, after: string | null) => {
    if (!client) return;
    setBusy(true);
    try {
      const stateBefore = treeStateRef.current;
      const placement = after ? { type: "after" as const, after } : { type: "first" as const };
      const op = await client.local.move(replicaId, nodeId, newParent, placement);
      for (const conn of syncConnRef.current.values()) void conn.peer.notifyLocalUpdate();
      ingestPayloadOps([op]);
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
    if (!client) return;
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
          ops.push(await client.local.insert(replicaId, parentId, nodeId, { type: "last" }, payload));
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
          ops.push(await client.local.insert(replicaId, targetParent, nodeId, { type: "last" }, payload));

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
      ingestPayloadOps(ops);
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
    if (!client) return;
    setBusy(true);
    try {
      const stateBefore = treeStateRef.current;
      const valueBase = newNodeValue.trim();
      const payload = valueBase.length > 0 ? textEncoder.encode(valueBase) : null;
      const nodeId = makeNodeId();
      const op = await client.local.insert(replicaId, parentId, nodeId, { type: "last" }, payload);
      for (const conn of syncConnRef.current.values()) void conn.peer.notifyLocalUpdate();
      ingestPayloadOps([op]);
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
    if (currentlyCollapsed) void ensureChildrenLoaded(id);
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
  const collapseAll = () => setCollapse({ defaultCollapsed: true, overrides: new Set([ROOT_ID]) });

  const stateBadge = status === "ready" ? "bg-emerald-500/80" : status === "error" ? "bg-rose-500/80" : "bg-amber-400/80";
  const peerTotal = peers.length + 1;

  return (
    <div className="mx-auto max-w-6xl px-4 pb-12 pt-8 space-y-6">
      <header className="flex flex-col gap-3 rounded-2xl bg-slate-900/60 p-6 shadow-xl shadow-black/20 ring-1 ring-slate-800/60 backdrop-blur">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2 text-lg font-semibold text-slate-50">TreeCRDT</div>
        </div>
        <div className="flex flex-col items-start gap-3 text-xs text-slate-400">
          <div>
            Replica: <span className="font-mono text-slate-200">{replicaId}</span>
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
                  onClick={() => void handleSync({ all: {} })}
                  disabled={status !== "ready" || busy || syncBusy || peers.length === 0 || !online}
                  title="Sync all (one-shot)"
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
                  disabled={status !== "ready" || busy || !online}
                  aria-label="Live sync all"
                  aria-pressed={liveAllEnabled}
                  title="Live sync all (polling)"
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
                  className="flex h-9 w-9 items-center justify-center rounded-lg border border-slate-700 bg-slate-800/70 text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                  onClick={openNewPeerTab}
                  disabled={typeof window === "undefined"}
                  type="button"
                  title="Open a new peer tab"
                >
                  <MdOpenInNew className="text-[18px]" />
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
              <div className="mb-3 rounded-lg border border-rose-400/50 bg-rose-500/10 px-3 py-2 text-xs text-rose-50">
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
                    <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Replica</div>
                    <div className="font-mono text-slate-200">{replicaId}</div>
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
                      {replicaId} <span className="text-[10px] text-slate-500">(you)</span>
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
                              <span className="font-mono text-slate-400">lamport {op.meta.lamport}</span>
                            </div>
                            <div className="mt-1 text-slate-300">{renderKind(op.kind)}</div>
                            <div className="text-[10px] text-slate-500">counter {op.meta.id.counter}</div>
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

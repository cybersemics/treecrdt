import React, { useEffect, useMemo, useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { Operation, OperationKind } from "@treecrdt/interface";
import { bytesToHex } from "@treecrdt/interface/ids";
import { createTreecrdtClient, type TreecrdtClient } from "@treecrdt/wa-sqlite/client";
import { detectOpfsSupport } from "@treecrdt/wa-sqlite/opfs";
import { SyncPeer, treecrdtSyncV0ProtobufCodec, type Filter } from "@treecrdt/sync";
import type { DuplexTransport } from "@treecrdt/sync/transport";

import { createBroadcastDuplex, createPlaygroundBackend, hexToBytes16, type PresenceMessage } from "./sync-v0";

const ROOT_ID = "00000000000000000000000000000000"; // 16-byte zero, hex-encoded

type DisplayNode = {
  id: string;
  label: string;
  children: DisplayNode[];
};

type NodeMeta = {
  parentId: string | null;
  order: number;
  childCount: number;
  deleted: boolean;
};

type DerivedTree = {
  root: DisplayNode;
  index: Record<string, NodeMeta>;
  childrenByParent: Record<string, string[]>;
};

type Status = "booting" | "ready" | "error";
type StorageMode = "memory" | "opfs";

type PeerInfo = { id: string; lastSeen: number };

export default function App() {
  const [client, setClient] = useState<TreecrdtClient | null>(null);
  const clientRef = useRef<TreecrdtClient | null>(null);
  const [ops, setOps] = useState<Operation[]>([]);
  const [status, setStatus] = useState<Status>("booting");
  const [error, setError] = useState<string | null>(null);
  const [docId, setDocId] = useState<string>(() => initialDocId());
  const [storage, setStorage] = useState<StorageMode>(() => initialStorage());
  const [sessionKey, setSessionKey] = useState<string>(() =>
    initialStorage() === "opfs" ? ensureOpfsKey() : makeSessionKey()
  );
  const [parentChoice, setParentChoice] = useState(ROOT_ID);
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());
  const [busy, setBusy] = useState(false);
  const [syncBusy, setSyncBusy] = useState(false);
  const [syncError, setSyncError] = useState<string | null>(null);
  const [peers, setPeers] = useState<PeerInfo[]>([]);
  const [nodeCount, setNodeCount] = useState(1);

  const counterRef = useRef(0);
  const lamportRef = useRef(0);
  const replicaId = useMemo(pickReplicaId, []);
  const opfsSupport = useMemo(detectOpfsSupport, []);

  const syncConnRef = useRef<
    Map<string, { transport: DuplexTransport<any>; peer: SyncPeer<Operation>; detach: () => void }>
  >(new Map());

  const { root, index, childrenByParent } = useMemo(() => rebuildTree(ops), [ops]);

  const nodeList = useMemo(() => flatten(root), [root]);
  const headLamport = useMemo(() => ops.reduce((max, op) => Math.max(max, op.meta.lamport), 0), [ops]);
  const visibleNodes = useMemo(() => {
    const acc: Array<{ node: DisplayNode; depth: number }> = [];
    const walk = (n: DisplayNode, depth: number) => {
      acc.push({ node: n, depth });
      if (collapsed.has(n.id)) return;
      for (const child of n.children) walk(child, depth + 1);
    };
    walk(root, 0);
    return acc;
  }, [root, collapsed]);
  const treeParentRef = useRef<HTMLDivElement | null>(null);
  const opsParentRef = useRef<HTMLDivElement | null>(null);
  const treeEstimateSize = React.useCallback(() => 116, []);
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

    const channel = new BroadcastChannel(`treecrdt-sync-v0:${docId}`);
    const backend = createPlaygroundBackend(client, docId);
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

      const transport = createBroadcastDuplex<Operation>(
        channel,
        replicaId,
        peerId,
        treecrdtSyncV0ProtobufCodec
      );
      const peer = new SyncPeer<Operation>(backend);
      const detach = peer.attach(transport);
      connections.set(peerId, { transport, peer, detach });
    };

    const onPresence = (ev: MessageEvent<any>) => {
      const data = ev.data as unknown;
      if (!data || typeof data !== "object") return;
      const msg = data as Partial<PresenceMessage>;
      if (msg.t !== "presence") return;
      if (typeof msg.peer_id !== "string" || typeof msg.ts !== "number") return;
      if (msg.peer_id === replicaId) return;
      lastSeen.set(msg.peer_id, msg.ts);
      ensureConnection(msg.peer_id);
      updatePeers();
    };

    channel.addEventListener("message", onPresence);

    const sendPresence = () => {
      const msg: PresenceMessage = { t: "presence", peer_id: replicaId, ts: Date.now() };
      channel.postMessage(msg);
    };

    sendPresence();
    const interval = window.setInterval(sendPresence, 1000);
    const pruneInterval = window.setInterval(() => {
      const now = Date.now();
      for (const [id, ts] of lastSeen) {
        if (now - ts > 5000) {
          lastSeen.delete(id);
          const conn = connections.get(id);
          if (conn) {
            conn.detach();
            connections.delete(id);
          }
        }
      }
      updatePeers();
    }, 2000);

    return () => {
      window.clearInterval(interval);
      window.clearInterval(pruneInterval);
      channel.removeEventListener("message", onPresence);
      channel.close();

      for (const conn of connections.values()) conn.detach();
      connections.clear();
      setPeers([]);
    };
  }, [client, docId, replicaId, status]);

  useEffect(() => {
    counterRef.current = Math.max(counterRef.current, findMaxCounter(ops));
    lamportRef.current = Math.max(lamportRef.current, headLamport);
  }, [ops, headLamport]);

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
      setStatus("ready");
      await refreshOps(c);
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
    setCollapsed(new Set());
    counterRef.current = 0;
    lamportRef.current = 0;
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
      const sorted = [...fetched].sort(
        (a, b) => a.meta.lamport - b.meta.lamport || a.meta.id.counter - b.meta.id.counter
      );
      setOps(sorted);
      setCollapsed(new Set());
      setParentChoice((prev) => (opts.preserveParent ? prev : ROOT_ID));
    } catch (err) {
      console.error("Failed to refresh ops", err);
      setError("Failed to refresh operations (see console)");
    }
  };

  const appendOperation = async (kind: OperationKind) => {
    if (!client) return;
    setBusy(true);
    try {
      counterRef.current += 1;
      lamportRef.current = Math.max(lamportRef.current, headLamport) + 1;
      const op: Operation = {
        meta: {
          id: { replica: replicaId, counter: counterRef.current },
          lamport: lamportRef.current,
        },
        kind,
      };
      await client.ops.append(op);
      await refreshOps(undefined, { preserveParent: true });
    } catch (err) {
      console.error("Failed to append op", err);
      setError("Failed to append operation (see console)");
    } finally {
      setBusy(false);
    }
  };

  const handleAddNodes = async (parentId: string, count: number) => {
    if (!client) return;
    if (count <= 0) return;
    setBusy(true);
    try {
      const ops: Operation[] = [];
      lamportRef.current = Math.max(lamportRef.current, headLamport);
      const basePosition = (childrenByParent[parentId] ?? []).length;
      for (let i = 0; i < count; i++) {
        counterRef.current += 1;
        lamportRef.current += 1;
        const nodeId = makeNodeId();
        const op: Operation = {
          meta: { id: { replica: replicaId, counter: counterRef.current }, lamport: lamportRef.current },
          kind: { type: "insert", parent: parentId, node: nodeId, position: basePosition + i },
        };
        ops.push(op);
      }
      await client.ops.appendMany(ops);
      await refreshOps(undefined, { preserveParent: true });
    } catch (err) {
      console.error("Failed to add nodes", err);
      setError("Failed to add nodes (see console)");
    } finally {
      setBusy(false);
    }
  };

  const handleInsert = async (parentId: string) => {
    await handleAddNodes(parentId, 1);
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
    await appendOperation({ type: "move", node: nodeId, newParent: meta.parentId, position: targetIdx });
  };

  const handleMoveToRoot = async (nodeId: string) => {
    if (nodeId === ROOT_ID) return;
    const position = childrenByParent[ROOT_ID]?.length ?? 0;
    await appendOperation({ type: "move", node: nodeId, newParent: ROOT_ID, position });
  };

  const handleSync = async (filter: Filter) => {
    const connections = syncConnRef.current;
    if (connections.size === 0) {
      setSyncError("No peers discovered yet.");
      return;
    }

    setSyncBusy(true);
    setSyncError(null);
    try {
      for (const conn of connections.values()) {
        await conn.peer.syncOnce(conn.transport, filter, { maxCodewords: 50_000, codewordsPerMessage: 512 });
      }
      await refreshOps(undefined, { preserveParent: true });
    } catch (err) {
      console.error("Sync failed", err);
      setSyncError(err instanceof Error ? err.message : String(err));
    } finally {
      setSyncBusy(false);
    }
  };

  const handleReset = async () => {
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
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const expandAll = () => setCollapsed(new Set());
  const collapseAll = () => setCollapsed(new Set(nodeList.map((n) => n.id)));

  const stateBadge = status === "ready" ? "bg-emerald-500/80" : status === "error" ? "bg-rose-500/80" : "bg-amber-400/80";

  return (
    <div className="mx-auto max-w-6xl px-4 pb-12 pt-8 space-y-6">
      <header className="flex flex-col gap-3 rounded-2xl bg-slate-900/60 p-6 shadow-xl shadow-black/20 ring-1 ring-slate-800/60 backdrop-blur">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2 text-lg font-semibold text-slate-50">TreeCRDT</div>
        </div>
        <div className="flex flex-col  items-left gap-3 text-xs text-slate-400">
          <div>
            Replica: <span className="font-mono text-slate-200">{replicaId}</span>
          </div>
          <p className="max-w-3xl text-sm text-slate-300">
            Experiment with the TreeCRDT SQLite extension running inside wa-sqlite. Add, reorder, and delete nodes; every action
            becomes a CRDT operation persisted in the selected storage (in-memory or OPFS). The right rail shows the live log of operations.
          </p>
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
            <span className="font-semibold">Persistent</span>
            <button
              type="button"
              className={`relative h-7 w-12 rounded-full border border-slate-700 transition ${
                storage === "opfs" ? "bg-emerald-500/30" : "bg-slate-800/70"
              } ${!opfsSupport.available ? "opacity-70" : "hover:border-accent"}`}
              onClick={() => handleStorageToggle(storage === "opfs" ? "memory" : "opfs")}
              disabled={status === "booting"}
              title={
                opfsSupport.available
                  ? "Toggle persistent OPFS storage"
                  : "Will attempt OPFS via worker; may fall back to memory if browser blocks sync handles."
              }
            >
              <span
                className={`absolute top-0.5 h-5 w-5 rounded-full bg-white transition ${
                  storage === "opfs" ? "right-0.5" : "left-0.5"
                }`}
              />
            </button>
            <span className="text-[11px] text-slate-400">{storage === "opfs" ? "OPFS" : "Memory"}</span>
          </div>
          <span className={`rounded-full px-3 py-1 text-xs font-semibold text-slate-900 ${stateBadge}`}>
            {status === "ready"
              ? storage === "opfs"
                ? "Persistent (OPFS) ready"
                : "In-memory ready"
              : status === "booting"
                ? "Starting wasm"
                : "Error"}
          </span>
        </div>
        {error && <div className="rounded-lg border border-rose-400/50 bg-rose-500/10 px-4 py-2 text-sm text-rose-50">{error}</div>}
      </header>

      <div className="grid gap-6 md:grid-cols-3">
        <section className="md:col-span-2 space-y-4">
          <div className="rounded-2xl bg-slate-900/60 p-5 shadow-lg shadow-black/20 ring-1 ring-slate-800/60">
            <div className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-400">Composer</div>
            <form
              className="flex flex-col gap-3 md:flex-row md:items-end"
              onSubmit={(e) => {
                e.preventDefault();
                void handleAddNodes(parentChoice, nodeCount);
              }}
            >
              <label className="w-full md:w-52 space-y-2 text-sm text-slate-200">
                <span>Parent</span>
                <select
                  className="w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                  value={parentChoice}
                  onChange={(e) => setParentChoice(e.target.value)}
                  disabled={status !== "ready"}
                >
                  {nodeList.map(({ id, label, depth }) => (
                    <option key={id} value={id}>
                      {"".padStart(depth * 2, " ")}
                      {label}
                    </option>
                  ))}
                </select>
              </label>
              <label className="flex flex-col text-sm text-slate-200">
                <span>Node count</span>
                <input
                  type="number"
                  min={1}
                  max={5000}
                  value={nodeCount}
                  onChange={(e) => setNodeCount(Number(e.target.value) || 0)}
                  className="w-28 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                  disabled={status !== "ready" || busy}
                />
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
            <div className="mb-3 flex items-center justify-between text-sm font-semibold uppercase tracking-wide text-slate-400">
              <span>Tree</span>
              <div className="flex items-center gap-2 text-xs text-slate-500">{nodeList.length - 1} nodes</div>
            </div>
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
                      className="absolute left-0 top-0 w-full"
                      style={{ transform: `translateY(${item.start}px)` }}
                    >
                      <TreeRow
                        node={entry.node}
                        depth={entry.depth}
                        collapsed={collapsed}
                        onToggle={toggleCollapse}
                        onAddChild={(id) => {
                          setParentChoice(id);
                          void handleInsert(id);
                        }}
                        onDelete={handleDelete}
                        onMove={handleMove}
                        onMoveToRoot={handleMoveToRoot}
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

        <aside className="space-y-3 rounded-2xl bg-slate-900/60 p-5 shadow-lg shadow-black/20 ring-1 ring-slate-800/60">
          <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-4 shadow-inner shadow-black/30 space-y-3">
            <div className="flex items-center justify-between">
              <div className="text-sm font-semibold uppercase tracking-wide text-slate-400">Sync</div>
              <span className="text-[10px] text-slate-500">v0 draft</span>
            </div>
            <div className="text-xs text-slate-400">
              Doc: <span className="font-mono text-slate-200">{docId}</span>
            </div>
            <label className="block text-xs text-slate-400">
              <span className="mb-1 block">Doc ID</span>
              <input
                className="w-full rounded-lg border border-slate-800 bg-slate-900/60 px-3 py-2 font-mono text-xs text-slate-100 outline-none focus:border-accent"
                value={docId}
                readOnly
                disabled
                title="Doc ID is part of opRef hashing; change via ?doc=... and reset session."
              />
            </label>
            <div className="text-xs text-slate-400">
              Peer: <span className="font-mono text-slate-200">{replicaId}</span>
            </div>
            <div className="flex flex-wrap gap-2">
              <button
                className="rounded-lg border border-slate-700 px-3 py-2 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                onClick={() => void handleSync({ all: {} })}
                disabled={status !== "ready" || busy || syncBusy}
              >
                Sync all
              </button>
              <button
                className="rounded-lg border border-slate-700 px-3 py-2 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                onClick={() => void handleSync({ children: { parent: hexToBytes16(parentChoice) } })}
                disabled={status !== "ready" || busy || syncBusy}
                title={`children(parent=${parentChoice})`}
              >
                Sync children
              </button>
            </div>
            <div className="text-xs text-slate-400">
              Peers: <span className="font-mono text-slate-200">{peers.length}</span>
            </div>
            {peers.length === 0 ? (
              <div className="text-xs text-slate-500">
                Open another playground tab with a different `?replica=` and the same `?doc=`.
              </div>
            ) : (
              <div className="max-h-32 overflow-auto pr-1 text-xs">
                {peers.map((p) => (
                  <div key={p.id} className="flex items-center justify-between gap-2 py-1">
                    <span className="font-mono text-slate-200">{p.id}</span>
                    <span className="text-[10px] text-slate-500">{Math.max(0, Date.now() - p.lastSeen)}ms</span>
                  </div>
                ))}
              </div>
            )}
            {syncError && (
              <div className="rounded-lg border border-rose-400/50 bg-rose-500/10 px-3 py-2 text-xs text-rose-50">
                {syncError}
              </div>
            )}
          </div>
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
      </div>
    </div>
  );
}

function TreeRow({
  node,
  depth,
  collapsed,
  onToggle,
  onAddChild,
  onDelete,
  onMove,
  onMoveToRoot,
  meta,
  childrenByParent,
}: {
  node: DisplayNode;
  depth: number;
  collapsed: Set<string>;
  onToggle: (id: string) => void;
  onAddChild: (id: string) => void;
  onDelete: (id: string) => void;
  onMove: (id: string, direction: "up" | "down") => void;
  onMoveToRoot: (id: string) => void;
  meta: Record<string, NodeMeta>;
  childrenByParent: Record<string, string[]>;
}) {
  const isCollapsed = collapsed.has(node.id);
  const isRoot = node.id === ROOT_ID;
  const metaInfo = meta[node.id];
  const siblings = metaInfo?.parentId ? childrenByParent[metaInfo.parentId] ?? [] : [];
  const canMoveUp = !isRoot && metaInfo && siblings.indexOf(node.id) > 0;
  const canMoveDown =
    !isRoot &&
    metaInfo &&
    siblings.indexOf(node.id) !== -1 &&
    siblings.indexOf(node.id) < siblings.length - 1;

  return (
    <div
      className="rounded-xl border border-slate-800/70 bg-slate-900/60 p-3 shadow-sm shadow-black/30"
      style={{ paddingLeft: `${depth * 16}px` }}
    >
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex items-center gap-3">
          <button
            className="flex h-8 w-8 items-center justify-center rounded-lg border border-slate-700 bg-slate-800/70 text-slate-200 transition hover:border-accent hover:text-white"
            onClick={() => onToggle(node.id)}
          >
            {isCollapsed ? "+" : "-"}
          </button>
          <div>
            <div className="text-lg font-semibold text-white">{node.label}</div>
            <div className="font-mono text-[11px] text-slate-500">{node.id}</div>
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            className="rounded-lg border border-slate-700 px-3 py-1 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white"
            onClick={() => onAddChild(node.id)}
          >
            + Child
          </button>
          {!isRoot && (
            <>
              <button
                className="rounded-lg border border-slate-700 px-3 py-1 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                onClick={() => onMove(node.id, "up")}
                disabled={!canMoveUp}
              >
                ↑ Move up
              </button>
              <button
                className="rounded-lg border border-slate-700 px-3 py-1 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                onClick={() => onMove(node.id, "down")}
                disabled={!canMoveDown}
              >
                ↓ Move down
              </button>
              <button
                className="rounded-lg border border-slate-700 px-3 py-1 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white"
                onClick={() => onMoveToRoot(node.id)}
              >
                ⇱ To root
              </button>
              <button
                className="rounded-lg border border-rose-400/80 px-3 py-1 text-xs font-semibold text-rose-100 transition hover:bg-rose-500/10"
                onClick={() => onDelete(node.id)}
              >
                Delete
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

function rebuildTree(ops: Operation[]): DerivedTree {
  type NodeState = { id: string; parentId: string | null; children: string[]; deleted: boolean };
  const nodes = new Map<string, NodeState>();
  const ensure = (id: string): NodeState => {
    if (!nodes.has(id)) {
      nodes.set(id, { id, parentId: null, children: [], deleted: false });
    }
    return nodes.get(id)!;
  };

  ensure(ROOT_ID);

  const insertAt = (arr: string[], value: string, position: number) => {
    const next = arr.filter((id) => id !== value);
    const clamped = Math.max(0, Math.min(position ?? next.length, next.length));
    next.splice(clamped, 0, value);
    return next;
  };

  const sortedOps = [...ops].sort((a, b) => a.meta.lamport - b.meta.lamport || a.meta.id.counter - b.meta.id.counter);
  for (const op of sortedOps) {
    if (op.kind.type === "insert") {
      const parent = ensure(op.kind.parent);
      const node = ensure(op.kind.node);
      node.deleted = false;
      node.parentId = parent.id;
      parent.children = insertAt(parent.children, node.id, op.kind.position ?? parent.children.length);
    } else if (op.kind.type === "move") {
      const node = ensure(op.kind.node);
      const targetParent = ensure(op.kind.newParent);
      if (node.parentId) {
        const currentParent = ensure(node.parentId);
        currentParent.children = currentParent.children.filter((id) => id !== node.id);
      }
      node.parentId = targetParent.id;
      targetParent.children = insertAt(
        targetParent.children,
        node.id,
        op.kind.position ?? targetParent.children.length
      );
    } else if (op.kind.type === "delete" || op.kind.type === "tombstone") {
      if (op.kind.node === ROOT_ID) continue;
      const node = ensure(op.kind.node);
      node.deleted = true;
      if (node.parentId) {
        const parent = ensure(node.parentId);
        parent.children = parent.children.filter((id) => id !== node.id);
      }
    }
  }

  const childrenByParent: Record<string, string[]> = {};
  const index: Record<string, NodeMeta> = {};
  const labelFor = (id: string) => {
    if (id === ROOT_ID) return "Root";
    return id.slice(0, 6);
  };

  const build = (id: string, parentId: string | null, order: number): DisplayNode | null => {
    const node = nodes.get(id);
    if (!node || node.deleted) return null;
    const renderedChildren: DisplayNode[] = [];
    let childOrder = 0;
    for (const childId of node.children) {
      const built = build(childId, id, childOrder);
      if (built) {
        renderedChildren.push(built);
        childOrder += 1;
      }
    }
    childrenByParent[id] = renderedChildren.map((c) => c.id);
    index[id] = { parentId, order, childCount: renderedChildren.length, deleted: node.deleted };
    return { id, label: labelFor(id), children: renderedChildren };
  };

  const root = build(ROOT_ID, null, 0) ?? { id: ROOT_ID, label: "Root", children: [] };
  if (!childrenByParent[ROOT_ID]) {
    childrenByParent[ROOT_ID] = root.children.map((c) => c.id);
  }
  if (!index[ROOT_ID]) {
    index[ROOT_ID] = { parentId: null, order: 0, childCount: root.children.length, deleted: false };
  }

  return { root, index, childrenByParent };
}

function flatten(node: DisplayNode, depth = 0): Array<DisplayNode & { depth: number }> {
  const self = { ...node, depth };
  return [self, ...node.children.flatMap((child) => flatten(child, depth + 1))];
}

function renderKind(kind: OperationKind): string {
  if (kind.type === "insert") {
    return `insert ${kind.node} under ${kind.parent} @${kind.position}`;
  }
  if (kind.type === "move") {
    return `move ${kind.node} to ${kind.newParent} @${kind.position}`;
  }
  return `${kind.type} ${kind.node}`;
}

function findMaxCounter(ops: Operation[]): number {
  return ops.reduce((max, op) => Math.max(max, op.meta.id.counter), 0);
}

function pickReplicaId(): string {
  if (typeof window === "undefined") return `replica-${Math.random().toString(16).slice(2, 6)}`;
  const override = new URLSearchParams(window.location.search).get("replica");
  if (override && override.trim()) return override.trim();
  const key = "treecrdt-playground-replica";
  const existing = window.localStorage.getItem(key);
  if (existing) return existing;
  const next = `replica-${crypto.randomUUID().slice(0, 8)}`;
  window.localStorage.setItem(key, next);
  return next;
}

function initialStorage(): StorageMode {
  if (typeof window === "undefined") return "memory";
  const param = new URLSearchParams(window.location.search).get("storage");
  return param === "opfs" ? "opfs" : "memory";
}

function initialDocId(): string {
  if (typeof window === "undefined") return "treecrdt-playground";
  const param = new URLSearchParams(window.location.search).get("doc");
  if (param && param.trim()) return param.trim();
  const key = "treecrdt-playground-doc";
  const existing = window.localStorage.getItem(key);
  if (existing) return existing;
  const next = "treecrdt-playground";
  window.localStorage.setItem(key, next);
  return next;
}

function persistDocId(docId: string) {
  if (typeof window === "undefined") return;
  const key = "treecrdt-playground-doc";
  window.localStorage.setItem(key, docId);
  const url = new URL(window.location.href);
  if (docId) url.searchParams.set("doc", docId);
  else url.searchParams.delete("doc");
  window.history.replaceState({}, "", url);
}

function persistStorage(mode: StorageMode) {
  if (typeof window === "undefined") return;
  const url = new URL(window.location.href);
  if (mode === "opfs") {
    url.searchParams.set("storage", "opfs");
  } else {
    url.searchParams.delete("storage");
  }
  window.history.replaceState({}, "", url);
}

function makeNodeId(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return bytesToHex(bytes);
}

function makeSessionKey(): string {
  const bytes = new Uint8Array(8);
  crypto.getRandomValues(bytes);
  return bytesToHex(bytes);
}

function opfsKeyStore(): { get: () => string | null; set: (val: string) => string } {
  if (typeof window === "undefined") {
    return { get: () => null, set: (val) => val };
  }
  const key = "treecrdt-playground-opfs-key";
  return {
    get: () => window.localStorage.getItem(key),
    set: (val: string) => {
      window.localStorage.setItem(key, val);
      return val;
    },
  };
}

function ensureOpfsKey(): string {
  const store = opfsKeyStore();
  const existing = store.get();
  if (existing) return existing;
  return store.set(makeSessionKey());
}

function persistOpfsKey(val: string): string {
  const store = opfsKeyStore();
  return store.set(val);
}

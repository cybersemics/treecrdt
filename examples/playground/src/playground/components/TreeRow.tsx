import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import {
  MdAccountTree,
  MdAdd,
  MdChevronRight,
  MdCheck,
  MdDeleteOutline,
  MdEdit,
  MdExpandMore,
  MdGroup,
  MdHome,
  MdKeyboardArrowDown,
  MdKeyboardArrowUp,
  MdLockOpen,
  MdLockOutline,
  MdOutlineRssFeed,
  MdShare,
} from "react-icons/md";

import { ROOT_ID } from "../constants";
import type { CollapseState, DisplayNode, NodeMeta, PeerInfo } from "../types";

type IssuedGrantRecordRow = {
  recipientPkHex: string;
  tokenIdHex: string;
  rootNodeId: string;
  actions: string[];
  maxDepth?: number;
  excludeCount: number;
  ts: number;
};

type CapabilityAction = "write_structure" | "write_payload" | "delete" | "grant";

const CAPABILITY_ACTION_ORDER: CapabilityAction[] = [
  "write_structure",
  "write_payload",
  "delete",
  "grant",
];
const DEFAULT_CAPABILITY_ACTIONS: CapabilityAction[] = ["write_structure", "write_payload", "grant"];

const CAPABILITY_META: Record<
  CapabilityAction,
  { label: string; icon: React.ComponentType<{ className?: string }> }
> = {
  write_structure: { label: "Write structure", icon: MdAccountTree },
  write_payload: { label: "Write payload", icon: MdEdit },
  delete: { label: "Delete", icon: MdDeleteOutline },
  grant: { label: "Grant", icon: MdShare },
};

function isCapabilityAction(value: string): value is CapabilityAction {
  return CAPABILITY_ACTION_ORDER.includes(value as CapabilityAction);
}

function toggleCapabilityAction(actions: CapabilityAction[], action: CapabilityAction): CapabilityAction[] {
  const set = new Set(actions);
  if (set.has(action)) set.delete(action);
  else set.add(action);
  return CAPABILITY_ACTION_ORDER.filter((name) => set.has(name));
}

type MembersMenuLayout = {
  top: number;
  left: number;
  width: number;
  maxHeight: number;
  listMaxHeight: number;
};

export function TreeRow({
  node,
  depth,
  collapse,
  liveChildren,
  onToggle,
  onSetValue,
  onAddChild,
  onDelete,
  onMove,
  onMoveToRoot,
  onToggleLiveChildren,
  privateRoots,
  onTogglePrivateRoot,
  onShare,
  peers,
  selfPeerId,
  authEnabled,
  canManageCapabilities,
  authBusy,
  issuedGrantRecords,
  hardRevokedTokenIds,
  onToggleHardRevokedTokenId,
  onGrantToReplicaPubkey,
  scopeRootId,
  canWritePayload,
  canWriteStructure,
  canDelete,
  meta,
  childrenByParent,
}: {
  node: DisplayNode;
  depth: number;
  collapse: CollapseState;
  liveChildren: boolean;
  onToggle: (id: string) => void;
  onSetValue: (id: string, value: string) => void | Promise<void>;
  onAddChild: (id: string) => void;
  onDelete: (id: string) => void;
  onMove: (id: string, direction: "up" | "down") => void;
  onMoveToRoot: (id: string) => void;
  onToggleLiveChildren: (id: string) => void;
  privateRoots: Set<string>;
  onTogglePrivateRoot: (id: string) => void;
  onShare: (id: string) => void;
  peers: PeerInfo[];
  selfPeerId: string | null;
  authEnabled: boolean;
  canManageCapabilities: boolean;
  authBusy: boolean;
  issuedGrantRecords: IssuedGrantRecordRow[];
  hardRevokedTokenIds: string[];
  onToggleHardRevokedTokenId: (tokenIdHex: string) => void;
  onGrantToReplicaPubkey: (opts: { recipientKey: string; rootNodeId: string; actions?: string[] }) => Promise<void>;
  scopeRootId: string;
  canWritePayload: boolean;
  canWriteStructure: boolean;
  canDelete: boolean;
  meta: Record<string, NodeMeta>;
  childrenByParent: Record<string, string[]>;
}) {
  const nodeIdLower = node.id.toLowerCase();
  const isCollapsed = collapse.defaultCollapsed ? !collapse.overrides.has(node.id) : collapse.overrides.has(node.id);
  const isRoot = node.id === ROOT_ID;
  const isScopeRoot = scopeRootId !== ROOT_ID && node.id === scopeRootId;
  const metaInfo = meta[node.id];
  const isPrivateRoot = !isRoot && privateRoots.has(node.id);
  let isPrivateInherited = false;
  if (!isRoot && !isPrivateRoot) {
    let parentId = metaInfo?.parentId ?? null;
    let hops = 0;
    while (parentId && hops < 10_000) {
      if (privateRoots.has(parentId)) {
        isPrivateInherited = true;
        break;
      }
      parentId = meta[parentId]?.parentId ?? null;
      hops += 1;
    }
  }
  const isPrivate = !isRoot && (isPrivateRoot || isPrivateInherited);
  const siblings = metaInfo?.parentId ? childrenByParent[metaInfo.parentId] ?? [] : [];
  const canMoveUp = !isRoot && metaInfo && siblings.indexOf(node.id) > 0;
  const canMoveDown =
    !isRoot && metaInfo && siblings.indexOf(node.id) !== -1 && siblings.indexOf(node.id) < siblings.length - 1;
  const childrenLoaded = Object.prototype.hasOwnProperty.call(childrenByParent, node.id);
  const childCount = childrenLoaded ? (childrenByParent[node.id]?.length ?? 0) : null;
  const toggleDisabled = childrenLoaded && childCount === 0 && isCollapsed;
  const [isEditing, setIsEditing] = useState(false);
  const [draftValue, setDraftValue] = useState(node.value);
  const [showMembersMenu, setShowMembersMenu] = useState(false);
  const [manualRecipientKey, setManualRecipientKey] = useState("");
  const [manualGrantActions, setManualGrantActions] = useState<CapabilityAction[]>(DEFAULT_CAPABILITY_ACTIONS);
  const [memberGrantActions, setMemberGrantActions] = useState<Record<string, CapabilityAction[]>>({});
  const [membersMenuLayout, setMembersMenuLayout] = useState<MembersMenuLayout | null>(null);
  const membersButtonRef = useRef<HTMLButtonElement | null>(null);
  const membersMenuRef = useRef<HTMLDivElement | null>(null);
  const canEditValue = canWritePayload && !isRoot;
  const canInsertChild = canWriteStructure;
  const canMoveStructure = canWriteStructure;
  const canMoveToDocRoot = canWriteStructure && scopeRootId === ROOT_ID;
  const showMembersButton = !isRoot && isPrivate;
  const discoveredPeers = useMemo(
    () =>
      peers
        .map((peer) => ({ id: peer.id.toLowerCase(), lastSeen: peer.lastSeen }))
        .filter((peer) => peer.id !== (selfPeerId ?? "").toLowerCase()),
    [peers, selfPeerId]
  );
  const latestScopedGrantByPeer = useMemo(() => {
    const out = new Map<string, IssuedGrantRecordRow>();
    for (const row of issuedGrantRecords) {
      if (row.rootNodeId !== nodeIdLower) continue;
      if (out.has(row.recipientPkHex)) continue;
      out.set(row.recipientPkHex, row);
    }
    return out;
  }, [issuedGrantRecords, nodeIdLower]);
  const scopedGrantCount = latestScopedGrantByPeer.size;
  const memberRows = useMemo(() => {
    const seenByPeer = new Map(discoveredPeers.map((peer) => [peer.id, peer.lastSeen]));
    const ids = new Set<string>();
    for (const id of latestScopedGrantByPeer.keys()) ids.add(id);
    for (const peer of discoveredPeers) ids.add(peer.id);

    const rows = Array.from(ids.values()).map((id) => ({
      id,
      lastSeen: seenByPeer.get(id) ?? null,
      latest: latestScopedGrantByPeer.get(id) ?? null,
    }));
    rows.sort((a, b) => a.id.localeCompare(b.id));
    return rows;
  }, [discoveredPeers, latestScopedGrantByPeer]);
  const getDefaultActionsForPeer = useCallback(
    (peerId: string): CapabilityAction[] => {
      const latest = latestScopedGrantByPeer.get(peerId);
      const fromLatest = latest?.actions.filter((action): action is CapabilityAction => isCapabilityAction(action)) ?? [];
      if (fromLatest.length > 0) return CAPABILITY_ACTION_ORDER.filter((action) => fromLatest.includes(action));
      return [...DEFAULT_CAPABILITY_ACTIONS];
    },
    [latestScopedGrantByPeer]
  );
  const getSelectedActionsForPeer = useCallback(
    (peerId: string): CapabilityAction[] => memberGrantActions[peerId] ?? getDefaultActionsForPeer(peerId),
    [getDefaultActionsForPeer, memberGrantActions]
  );

  const updateMembersMenuLayout = useCallback(() => {
    if (typeof window === "undefined") return;
    const button = membersButtonRef.current;
    if (!button) return;

    const rect = button.getBoundingClientRect();
    const viewportPadding = 8;
    const anchorGap = 8;
    const preferredWidth = 340;
    const width = Math.max(260, Math.min(preferredWidth, window.innerWidth - viewportPadding * 2));
    const left = Math.max(
      viewportPadding,
      Math.min(rect.right - width, window.innerWidth - width - viewportPadding)
    );
    const spaceBelow = window.innerHeight - rect.bottom - anchorGap - viewportPadding;
    const spaceAbove = rect.top - anchorGap - viewportPadding;
    const placeAbove = spaceBelow < 280 && spaceAbove > spaceBelow;
    const maxViewportHeight = Math.max(180, window.innerHeight - viewportPadding * 2);
    const chosenSpace = placeAbove ? spaceAbove : spaceBelow;
    const maxHeight = Math.max(180, Math.min(560, maxViewportHeight, Math.max(chosenSpace, 180)));
    const unclampedTop = placeAbove ? rect.top - anchorGap - maxHeight : rect.bottom + anchorGap;
    const top = Math.max(viewportPadding, Math.min(unclampedTop, window.innerHeight - viewportPadding - maxHeight));
    const listMaxHeight = Math.max(100, Math.min(320, maxHeight - 220));
    setMembersMenuLayout({ top, left, width, maxHeight, listMaxHeight });
  }, []);

  useEffect(() => {
    if (!isEditing) setDraftValue(node.value);
  }, [isEditing, node.value]);

  useEffect(() => {
    if (!canEditValue && isEditing) setIsEditing(false);
  }, [canEditValue, isEditing]);

  useEffect(() => {
    if (showMembersButton) return;
    setShowMembersMenu(false);
    setMembersMenuLayout(null);
  }, [showMembersButton]);

  useEffect(() => {
    if (!showMembersMenu) return;
    setMemberGrantActions((prev) => {
      let changed = false;
      const next = { ...prev };
      for (const row of memberRows) {
        if (next[row.id] && next[row.id]!.length > 0) continue;
        next[row.id] = getDefaultActionsForPeer(row.id);
        changed = true;
      }
      return changed ? next : prev;
    });
  }, [getDefaultActionsForPeer, memberRows, showMembersMenu]);

  useEffect(() => {
    if (!showMembersMenu) return;
    const onMouseDown = (event: MouseEvent) => {
      const target = event.target as Node;
      if (membersMenuRef.current?.contains(target)) return;
      if (membersButtonRef.current?.contains(target)) return;
      setShowMembersMenu(false);
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setShowMembersMenu(false);
    };
    document.addEventListener("mousedown", onMouseDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("mousedown", onMouseDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [showMembersMenu]);

  useEffect(() => {
    if (!showMembersMenu) {
      setMembersMenuLayout(null);
      return;
    }
    updateMembersMenuLayout();
    const onScrollOrResize = () => updateMembersMenuLayout();
    window.addEventListener("resize", onScrollOrResize);
    window.addEventListener("scroll", onScrollOrResize, true);
    return () => {
      window.removeEventListener("resize", onScrollOrResize);
      window.removeEventListener("scroll", onScrollOrResize, true);
    };
  }, [showMembersMenu, updateMembersMenuLayout]);

  return (
    <div
      className="group rounded-lg bg-slate-950/40 px-2 py-2 ring-1 ring-slate-800/50 transition hover:bg-slate-950/55 hover:ring-slate-700/70"
      style={{ paddingLeft: `${8 + depth * 16}px` }}
      data-testid="tree-row"
      data-node-id={node.id}
    >
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex min-w-0 items-center gap-2">
          <button
            className="flex h-8 w-8 items-center justify-center rounded-md border border-slate-800/70 bg-slate-900/60 text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
            onClick={() => onToggle(node.id)}
            disabled={toggleDisabled}
            aria-label={isCollapsed ? "Expand node" : "Collapse node"}
            title={isCollapsed ? "Expand" : "Collapse"}
          >
            {isCollapsed ? <MdChevronRight className="text-[22px]" /> : <MdExpandMore className="text-[22px]" />}
          </button>
          <div className="min-w-0 flex items-center gap-2">
            <div className="min-w-0">
              {isRoot ? (
                <div className="truncate text-sm font-semibold text-white">{node.label}</div>
              ) : isEditing ? (
                <form
                  className="flex items-center gap-2"
                  onSubmit={(e) => {
                    e.preventDefault();
                    setIsEditing(false);
                    void onSetValue(node.id, draftValue);
                  }}
                >
                  <input
                    type="text"
                    value={draftValue}
                    onChange={(e) => setDraftValue(e.target.value)}
                    className="w-56 max-w-full rounded-md border border-slate-700 bg-slate-900/60 px-2 py-1 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                  />
                  <button
                    type="submit"
                    className="rounded-md border border-slate-700 bg-slate-900/60 px-2 py-1 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white"
                    title="Save"
                  >
                    Save
                  </button>
                  <button
                    type="button"
                    className="rounded-md border border-slate-700 bg-slate-900/60 px-2 py-1 text-xs font-semibold text-slate-200 transition hover:border-slate-500 hover:text-white"
                    onClick={() => setIsEditing(false)}
                    title="Cancel"
                  >
                    Cancel
                  </button>
                </form>
              ) : (
                <button
                  type="button"
                  className="block w-full text-left"
                  onClick={() => {
                    if (canEditValue) setIsEditing(true);
                  }}
                  disabled={!canEditValue}
                  title={canEditValue ? "Click to edit" : "Read-only (no write_payload permission)"}
                >
                  <span className="block truncate text-sm font-semibold text-white">{node.label}</span>
                </button>
              )}
            </div>
            {!isEditing && isScopeRoot && (
              <span
                className="flex-shrink-0 rounded-full border border-emerald-400/60 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-semibold text-emerald-50"
                title="Scoped access: your capability token starts at this subtree root. Nodes outside this subtree are hidden."
              >
                scoped access
              </span>
            )}
            {!isEditing && isPrivateRoot && (
              <span
                className="flex-shrink-0 rounded-full border border-amber-400/60 bg-amber-500/10 px-2 py-0.5 text-[10px] font-semibold text-amber-100"
                title="Private root: excluded from new invites (read/write scope). Payload privacy is handled separately via E2EE."
              >
                private root
              </span>
            )}
            {!isEditing && isPrivateInherited && (
              <span
                className="flex-shrink-0 rounded-full border border-slate-700 bg-slate-900/60 px-2 py-0.5 text-[10px] font-semibold text-slate-300"
                title="Private (inherited): excluded from new invites (read/write scope). Payload privacy is handled separately via E2EE."
              >
                private
              </span>
            )}
          </div>
        </div>
        <div className="flex flex-wrap gap-1.5">
          <button
            className="flex h-9 w-9 items-center justify-center rounded-lg border border-slate-800/70 bg-slate-900/60 text-slate-200 transition hover:border-accent hover:text-white"
            onClick={() => onShare(node.id)}
            aria-label="Share subtree (invite)"
            title="Share subtree invite link and permissions"
          >
            <MdShare className="text-[20px]" />
          </button>
          {showMembersButton && (
            <div className="relative">
              <button
                ref={membersButtonRef}
                className={`flex h-9 items-center gap-1.5 rounded-lg border px-2 text-slate-100 transition ${
                  showMembersMenu
                    ? "border-accent bg-accent/20 shadow-sm shadow-accent/20"
                    : "border-slate-800/70 bg-slate-900/60 hover:border-accent hover:text-white"
                }`}
                onClick={() =>
                  setShowMembersMenu((v) => {
                    const next = !v;
                    if (next) updateMembersMenuLayout();
                    return next;
                  })
                }
                aria-label="Members and capabilities"
                aria-expanded={showMembersMenu}
                title="Members for this private subtree"
                type="button"
              >
                <MdGroup className="text-[18px]" />
                <span className="font-mono text-[10px] font-semibold">{memberRows.length}</span>
              </button>
              {showMembersMenu &&
                membersMenuLayout &&
                typeof document !== "undefined" &&
                createPortal(
                  <div
                    ref={membersMenuRef}
                    className="fixed z-[120] flex flex-col overflow-y-auto rounded-xl border border-slate-700/80 bg-slate-950/95 p-3 shadow-2xl shadow-black/50"
                    style={{
                      top: `${membersMenuLayout.top}px`,
                      left: `${membersMenuLayout.left}px`,
                      width: `${membersMenuLayout.width}px`,
                      maxHeight: `${membersMenuLayout.maxHeight}px`,
                    }}
                  >
                  <div className="flex items-start justify-between gap-2">
                    <div className="min-w-0">
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Members</div>
                      <div className="mt-0.5 truncate font-mono text-[11px] text-slate-200" title={node.label}>
                        {node.label}
                      </div>
                      <div className="mt-0.5 text-[11px] text-slate-400">
                        {memberRows.length} peer(s) · {scopedGrantCount} scoped grant(s)
                      </div>
                    </div>
                    <button
                      className="h-8 shrink-0 rounded-lg border border-slate-700 bg-slate-800/70 px-3 text-[11px] font-semibold text-slate-200 transition hover:border-accent hover:text-white"
                      type="button"
                      onClick={() => {
                        setShowMembersMenu(false);
                        onShare(node.id);
                      }}
                    >
                      Share
                    </button>
                  </div>

                  <div className="mt-3 rounded-lg border border-slate-800/70 bg-slate-900/30 p-2.5">
                    <div className="grid grid-cols-[minmax(0,1fr)_auto] items-end gap-2">
                      <label className="space-y-1">
                        <span className="text-[10px] uppercase tracking-wide text-slate-500">Grant to pubkey</span>
                        <input
                          value={manualRecipientKey}
                          onChange={(e) => setManualRecipientKey(e.target.value)}
                          placeholder="hex or base64url"
                          className="h-9 w-full rounded-lg border border-slate-700 bg-slate-800/70 px-2.5 text-[11px] text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/40"
                          disabled={authBusy}
                        />
                      </label>
                      <button
                        className="h-9 min-w-[84px] rounded-lg border border-slate-700 bg-slate-800/70 px-2.5 text-[11px] font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                        type="button"
                        onClick={() => {
                          const recipientKey = manualRecipientKey.trim();
                          if (!recipientKey) return;
                          void onGrantToReplicaPubkey({
                            recipientKey,
                            rootNodeId: node.id,
                            actions: [...manualGrantActions],
                          });
                          setManualRecipientKey("");
                        }}
                        disabled={
                          authBusy ||
                          !canManageCapabilities ||
                          manualRecipientKey.trim().length === 0 ||
                          manualGrantActions.length === 0
                        }
                        title={
                          canManageCapabilities
                            ? "Grant current subtree scope"
                            : authEnabled
                              ? "Verify-only tabs can’t mint grants"
                              : "Enable Auth to mint grants"
                        }
                      >
                        + Grant
                      </button>
                    </div>
                    <div className="mt-2">
                      <div className="text-[10px] uppercase tracking-wide text-slate-500">Capabilities</div>
                      <div className="mt-1 flex flex-wrap gap-1">
                        {CAPABILITY_ACTION_ORDER.map((action) => {
                          const enabled = manualGrantActions.includes(action);
                          const Icon = CAPABILITY_META[action].icon;
                          return (
                            <button
                              key={`manual-${action}`}
                              type="button"
                              className={`relative flex h-7 w-7 items-center justify-center rounded-md border text-slate-100 transition ${
                                enabled
                                  ? "border-emerald-400/70 bg-emerald-500/10"
                                  : "border-slate-700 bg-slate-800/60 hover:border-accent"
                              }`}
                              title={CAPABILITY_META[action].label}
                              aria-label={CAPABILITY_META[action].label}
                              aria-pressed={enabled}
                              onClick={() =>
                                setManualGrantActions((prev) => toggleCapabilityAction(prev, action))
                              }
                              disabled={authBusy}
                            >
                              <Icon className="text-[14px]" />
                              {enabled && <MdCheck className="absolute -right-1 -top-1 text-[12px] text-emerald-300" />}
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  </div>

                  <div className="mt-2 space-y-1.5 overflow-auto pr-1" style={{ maxHeight: `${membersMenuLayout.listMaxHeight}px` }}>
                    {memberRows.length === 0 ? (
                      <div className="rounded-lg border border-slate-800/80 bg-slate-900/30 px-2.5 py-2 text-[11px] text-slate-500">
                        No peers discovered for this scope yet.
                      </div>
                    ) : (
                      memberRows.map((row) => {
                        const latestTokenId = row.latest?.tokenIdHex ?? null;
                        const revoked = latestTokenId ? hardRevokedTokenIds.includes(latestTokenId) : false;
                        const seenAgoMs = row.lastSeen === null ? null : Math.max(0, Date.now() - row.lastSeen);
                        const grantAgoMs = row.latest ? Math.max(0, Date.now() - row.latest.ts) : null;
                        const selectedActions = getSelectedActionsForPeer(row.id);
                        return (
                          <div key={row.id} className="rounded-lg border border-slate-800/80 bg-slate-900/30 px-2.5 py-2">
                            <div className="flex items-start justify-between gap-2">
                              <div className="min-w-0">
                                <div className="truncate font-mono text-[11px] text-slate-200">{row.id}</div>
                                <div className="text-[10px] text-slate-500">
                                  {seenAgoMs === null ? "not seen in this tab yet" : `seen ${seenAgoMs}ms ago`}
                                  {grantAgoMs !== null ? ` · grant ${grantAgoMs}ms ago` : ""}
                                </div>
                                {!row.latest && (
                                  <div className="text-[10px] text-slate-500">capability unknown (not issued for this scope)</div>
                                )}
                              </div>
                              {row.latest ? (
                                <span
                                  className={`shrink-0 rounded-full border px-2 py-0.5 text-[10px] font-semibold ${
                                    revoked
                                      ? "border-rose-400/60 bg-rose-500/10 text-rose-100"
                                      : "border-emerald-400/60 bg-emerald-500/10 text-emerald-100"
                                  }`}
                                >
                                  {revoked ? "revoked" : "active"}
                                </span>
                              ) : null}
                            </div>
                            <div className="mt-1.5">
                              <div className="text-[10px] uppercase tracking-wide text-slate-500">Capabilities</div>
                              <div className="mt-1 flex flex-wrap gap-1">
                                {CAPABILITY_ACTION_ORDER.map((action) => {
                                  const enabled = selectedActions.includes(action);
                                  const Icon = CAPABILITY_META[action].icon;
                                  return (
                                    <button
                                      key={`${row.id}-${action}`}
                                      type="button"
                                      className={`relative flex h-7 w-7 items-center justify-center rounded-md border text-slate-100 transition ${
                                        enabled
                                          ? "border-emerald-400/70 bg-emerald-500/10"
                                          : "border-slate-700 bg-slate-800/60 hover:border-accent"
                                      }`}
                                      title={CAPABILITY_META[action].label}
                                      aria-label={CAPABILITY_META[action].label}
                                      aria-pressed={enabled}
                                      onClick={() =>
                                        setMemberGrantActions((prev) => ({
                                          ...prev,
                                          [row.id]: toggleCapabilityAction(getSelectedActionsForPeer(row.id), action),
                                        }))
                                      }
                                      disabled={authBusy}
                                    >
                                      <Icon className="text-[14px]" />
                                      {enabled && (
                                        <MdCheck className="absolute -right-1 -top-1 text-[12px] text-emerald-300" />
                                      )}
                                    </button>
                                  );
                                })}
                              </div>
                            </div>
                            <div className="mt-2 grid grid-cols-3 gap-1.5">
                              <button
                                className="h-7 rounded-lg border border-slate-700 bg-slate-800/70 px-2 text-[10px] font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                                type="button"
                                onClick={() =>
                                  void onGrantToReplicaPubkey({
                                    recipientKey: row.id,
                                    rootNodeId: node.id,
                                    actions: [...selectedActions],
                                  })
                                }
                                disabled={!canManageCapabilities || authBusy || selectedActions.length === 0}
                              >
                                Grant
                              </button>
                              <button
                                className="h-7 rounded-lg border border-slate-700 bg-slate-800/70 px-2 text-[10px] font-semibold text-slate-200 transition hover:border-accent hover:text-white"
                                type="button"
                                onClick={() => {
                                  setShowMembersMenu(false);
                                  onShare(node.id);
                                }}
                              >
                                Share…
                              </button>
                              <button
                                className={`h-7 rounded-lg border px-2 text-[10px] font-semibold transition disabled:opacity-50 ${
                                  revoked
                                    ? "border-emerald-400/50 bg-emerald-500/10 text-emerald-100 hover:border-emerald-300"
                                    : "border-rose-400/50 bg-rose-500/10 text-rose-100 hover:border-rose-300"
                                }`}
                                type="button"
                                onClick={() => {
                                  if (!latestTokenId) return;
                                  onToggleHardRevokedTokenId(latestTokenId);
                                }}
                                disabled={!canManageCapabilities || authBusy || !latestTokenId}
                                title={latestTokenId ?? "No scoped token issued yet"}
                              >
                                {revoked ? "Unrevoke" : "Revoke"}
                              </button>
                            </div>
                          </div>
                        );
                      })
                    )}
                  </div>
                  </div>,
                  document.body
                )}
            </div>
          )}
          <button
            className={`flex h-9 w-9 items-center justify-center rounded-lg border text-slate-200 transition ${
              isRoot
                ? "border-slate-800/70 bg-slate-900/60 opacity-50"
                : isPrivateRoot
                  ? "border-amber-400/80 bg-amber-500/15 text-amber-100 shadow-sm shadow-amber-500/20"
                  : isPrivateInherited
                    ? "border-slate-700 bg-slate-900/60 text-slate-300"
                    : "border-slate-800/70 bg-slate-900/60 hover:border-accent hover:text-white"
            }`}
            onClick={() => onTogglePrivateRoot(node.id)}
            disabled={isRoot}
            aria-label="Toggle node privacy"
            aria-pressed={isPrivate}
            title={
              isRoot
                ? "Root privacy is controlled by the invite scope"
                : isPrivateRoot
                  ? "Make subtree public (affects new invites)"
                  : isPrivateInherited
                    ? "Subtree is private via ancestor. Click to mark this node as a private root too"
                    : "Make subtree private (exclude from new invites)"
            }
          >
            {isPrivate ? <MdLockOutline className="text-[20px]" /> : <MdLockOpen className="text-[20px]" />}
          </button>
          <button
            className={`flex h-9 w-9 items-center justify-center rounded-lg border text-slate-200 transition ${
              liveChildren
                ? "border-accent bg-accent/20 text-white shadow-sm shadow-accent/20"
                : "border-slate-800/70 bg-slate-900/60 hover:border-accent hover:text-white"
            }`}
            onClick={() => onToggleLiveChildren(node.id)}
            aria-label="Live sync children"
            aria-pressed={liveChildren}
            title="Live sync children"
          >
            <MdOutlineRssFeed className="text-[20px]" />
          </button>
          <button
            className="flex h-9 w-9 items-center justify-center rounded-lg border border-slate-800/70 bg-slate-900/60 text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
            onClick={() => onAddChild(node.id)}
            aria-label="Add child"
            title={canInsertChild ? "Add child" : "Read-only (no write_structure permission)"}
            disabled={!canInsertChild}
          >
            <MdAdd className="text-[22px]" />
          </button>
          {!isRoot && (
            <>
              <button
                className="flex h-9 w-9 items-center justify-center rounded-lg border border-slate-800/70 bg-slate-900/60 text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                onClick={() => onMove(node.id, "up")}
                disabled={!canMoveStructure || !canMoveUp}
                aria-label="Move up"
                title={canMoveStructure ? "Move up" : "Read-only (no write_structure permission)"}
              >
                <MdKeyboardArrowUp className="text-[22px]" />
              </button>
              <button
                className="flex h-9 w-9 items-center justify-center rounded-lg border border-slate-800/70 bg-slate-900/60 text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                onClick={() => onMove(node.id, "down")}
                disabled={!canMoveStructure || !canMoveDown}
                aria-label="Move down"
                title={canMoveStructure ? "Move down" : "Read-only (no write_structure permission)"}
              >
                <MdKeyboardArrowDown className="text-[22px]" />
              </button>
              <button
                className="flex h-9 w-9 items-center justify-center rounded-lg border border-slate-800/70 bg-slate-900/60 text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                onClick={() => onMoveToRoot(node.id)}
                aria-label="Move to root"
                title={
                  canMoveToDocRoot
                    ? "Move to root"
                    : scopeRootId !== ROOT_ID
                      ? "Not available in scoped access (can’t move nodes outside your subtree)"
                      : "Read-only (no write_structure permission)"
                }
                disabled={!canMoveToDocRoot}
              >
                <MdHome className="text-[20px]" />
              </button>
              <button
                className="flex h-9 w-9 items-center justify-center rounded-lg border border-rose-400/80 bg-rose-500/10 text-rose-100 transition hover:bg-rose-500/20 disabled:opacity-50"
                onClick={() => onDelete(node.id)}
                aria-label="Delete"
                title={canDelete ? "Delete" : "Read-only (no delete permission)"}
                disabled={!canDelete}
              >
                <MdDeleteOutline className="text-[20px]" />
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

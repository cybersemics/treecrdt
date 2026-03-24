import React from "react";
import type { Virtualizer } from "@tanstack/react-virtual";
import { IoMdGitBranch } from "react-icons/io";
import {
  MdCloudOff,
  MdCloudQueue,
  MdExpandLess,
  MdExpandMore,
  MdGroup,
  MdLockOutline,
  MdOpenInNew,
  MdOutlineRssFeed,
  MdSync,
  MdVpnKey,
} from "react-icons/md";

import { ROOT_ID } from "../constants";
import type { CollapseState, DisplayNode, NodeMeta, PeerInfo } from "../types";

import { PeersPanel } from "./PeersPanel";
import { SharingAuthPanel } from "./SharingAuthPanel";
import { TreeRow } from "./TreeRow";

type VisibleNodeEntry = { node: DisplayNode; depth: number };

export function TreePanel({
  totalNodes,
  loadedNodes,
  privateRootsCount,
  online,
  setOnline,
  ready,
  busy,
  syncBusy,
  liveBusy,
  peerCount,
  authCanSyncAll,
  onSync,
  liveAllEnabled,
  setLiveAllEnabled,
  showPeersPanel,
  setShowPeersPanel,
  showAuthPanel,
  setShowAuthPanel,
  authEnabled,
  openNewPeerTab,
  openNewIsolatedPeerTab,
  authCanIssue,
  authCanDelegate,
  showOpsPanel,
  setShowOpsPanel,
  syncError,
  peersPanelProps,
  sharingAuthPanelProps,
  treeParentRef,
  treeVirtualizer,
  visibleNodes,
  collapse,
  toggleCollapse,
  openShareForNode,
  grantSubtreeToReplicaPubkey,
  onSetValue,
  onAddChild,
  onDelete,
  onMove,
  onMoveToRoot,
  onToggleLiveChildren,
  privateRoots,
  togglePrivateRoot,
  peers,
  selfPeerId,
  canManageCapabilities,
  authBusy,
  issuedGrantRecords,
  hardRevokedTokenIds,
  toggleHardRevokedTokenId,
  scopeRootId,
  canWritePayload,
  canWriteStructure,
  canDelete,
  liveChildrenParents,
  meta,
  childrenByParent,
}: {
  totalNodes: number | null;
  loadedNodes: number;
  privateRootsCount: number;
  online: boolean;
  setOnline: React.Dispatch<React.SetStateAction<boolean>>;
  ready: boolean;
  busy: boolean;
  syncBusy: boolean;
  liveBusy: boolean;
  peerCount: number;
  authCanSyncAll: boolean;
  onSync: () => void;
  liveAllEnabled: boolean;
  setLiveAllEnabled: React.Dispatch<React.SetStateAction<boolean>>;
  showPeersPanel: boolean;
  setShowPeersPanel: React.Dispatch<React.SetStateAction<boolean>>;
  showAuthPanel: boolean;
  setShowAuthPanel: React.Dispatch<React.SetStateAction<boolean>>;
  authEnabled: boolean;
  openNewPeerTab: () => void;
  openNewIsolatedPeerTab: (opts: { autoInvite: boolean; rootNodeId?: string }) => Promise<void>;
  authCanIssue: boolean;
  authCanDelegate: boolean;
  showOpsPanel: boolean;
  setShowOpsPanel: React.Dispatch<React.SetStateAction<boolean>>;
  syncError: string | null;
  peersPanelProps: React.ComponentProps<typeof PeersPanel>;
  sharingAuthPanelProps: React.ComponentProps<typeof SharingAuthPanel>;
  treeParentRef: React.RefObject<HTMLDivElement>;
  treeVirtualizer: Virtualizer<HTMLDivElement, Element>;
  visibleNodes: VisibleNodeEntry[];
  collapse: CollapseState;
  toggleCollapse: (id: string) => void;
  openShareForNode: (nodeId: string) => void;
  grantSubtreeToReplicaPubkey: (opts: {
    recipientKey: string;
    rootNodeId: string;
    actions?: string[];
    supersedesTokenIds?: string[];
  }) => Promise<boolean>;
  onSetValue: (nodeId: string, value: string) => void | Promise<void>;
  onAddChild: (nodeId: string) => void;
  onDelete: (nodeId: string) => void;
  onMove: (nodeId: string, direction: "up" | "down") => void;
  onMoveToRoot: (nodeId: string) => void;
  onToggleLiveChildren: (nodeId: string) => void;
  privateRoots: Set<string>;
  togglePrivateRoot: (nodeId: string) => void;
  peers: PeerInfo[];
  selfPeerId: string | null;
  canManageCapabilities: boolean;
  authBusy: boolean;
  issuedGrantRecords: Array<{
    recipientPkHex: string;
    tokenIdHex: string;
    rootNodeId: string;
    actions: string[];
    maxDepth?: number;
    excludeCount: number;
    ts: number;
  }>;
  hardRevokedTokenIds: string[];
  toggleHardRevokedTokenId: (tokenIdHex: string) => void;
  scopeRootId: string;
  canWritePayload: boolean;
  canWriteStructure: boolean;
  canDelete: boolean;
  liveChildrenParents: Set<string>;
  meta: Record<string, NodeMeta>;
  childrenByParent: Record<string, string[]>;
}) {
  const measureTreeElement = React.useCallback(
    (element: Element | null) => {
      if (!element) return;
      // Defer measurement to avoid virtualizer-triggered state updates during React render.
      if (typeof window !== "undefined") {
        window.requestAnimationFrame(() => {
          treeVirtualizer.measureElement(element);
        });
        return;
      }
      treeVirtualizer.measureElement(element);
    },
    [treeVirtualizer]
  );

  return (
    <div className="rounded-2xl bg-slate-900/60 p-5 shadow-lg shadow-black/20 ring-1 ring-slate-800/60">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-3">
          <div className="text-sm font-semibold uppercase tracking-wide text-slate-400">Tree</div>
          <div className="text-xs text-slate-500">
            {totalNodes === null ? "…" : totalNodes} nodes
            <span className="text-slate-600"> · {loadedNodes} loaded</span>
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
            disabled={!ready || busy}
            title={online ? "Go offline (simulate no sync)" : "Go online (resume sync)"}
            type="button"
          >
            {online ? <MdCloudQueue className="text-[18px]" /> : <MdCloudOff className="text-[18px]" />}
            <span>{online ? "Online" : "Offline"}</span>
          </button>
          <button
            className={`flex h-9 items-center gap-2 rounded-lg border px-3 text-xs font-semibold transition disabled:opacity-50 ${
              syncBusy
                ? "border-accent bg-accent/15 text-white"
                : "border-slate-700 bg-slate-800/70 text-slate-200 hover:border-accent hover:text-white"
            }`}
            onClick={onSync}
            disabled={!ready || busy || syncBusy || peerCount === 0 || !online}
            title={authCanSyncAll ? "Sync all (one-shot)" : "Sync loaded parents (scoped)"}
          >
            <MdSync className={`text-[18px] ${syncBusy ? "animate-spin" : ""}`} />
            <span>{syncBusy ? "Syncing..." : "Sync"}</span>
          </button>
          <button
            className={`flex h-9 w-9 items-center justify-center rounded-lg border text-slate-200 transition disabled:opacity-50 ${
              liveBusy
                ? "border-accent bg-accent/15 text-white shadow-sm shadow-accent/20"
                : liveAllEnabled
                ? "border-accent bg-accent/20 text-white shadow-sm shadow-accent/20"
                : "border-slate-700 bg-slate-800/70 hover:border-accent hover:text-white"
            }`}
            onClick={() => setLiveAllEnabled((v) => !v)}
            disabled={!ready || busy || !online || !authCanSyncAll}
            aria-label="Live sync all"
            aria-pressed={liveAllEnabled}
            aria-busy={liveBusy}
            title={
              !authCanSyncAll
                ? "Live sync all is not allowed by this token scope"
                : liveBusy
                  ? "Live sync all is starting or pushing updates"
                  : liveAllEnabled
                    ? "Live sync all is active"
                    : "Live sync all"
            }
          >
            <MdOutlineRssFeed className={`text-[20px] ${liveAllEnabled ? "animate-pulse" : ""}`} />
          </button>
          <button
            className={`flex h-9 items-center gap-2 rounded-lg border px-3 text-xs font-semibold transition ${
              showPeersPanel
                ? "border-slate-600 bg-slate-800/90 text-white"
                : "border-slate-700 bg-slate-800/70 text-slate-200 hover:border-accent hover:text-white"
            }`}
            onClick={() => setShowPeersPanel((v) => !v)}
            type="button"
            title={showPeersPanel ? "Hide connections panel" : "Show connections panel"}
            aria-expanded={showPeersPanel}
            aria-controls="playground-connections-panel"
          >
            <MdGroup className="text-[18px]" />
            <span>Connections</span>
            <span className="rounded-full bg-slate-950/70 px-2 py-0.5 font-mono text-[11px] text-slate-300">
              {peerCount}
            </span>
            {showPeersPanel ? (
              <MdExpandLess className="text-[18px]" aria-hidden />
            ) : (
              <MdExpandMore className="text-[18px]" aria-hidden />
            )}
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
            title={showAuthPanel ? "Hide Sharing & Auth panel" : "Show Sharing & Auth panel"}
            aria-expanded={showAuthPanel}
            aria-controls="playground-auth-panel"
          >
            <MdVpnKey className="text-[18px]" aria-hidden />
            <span>Auth</span>
            {showAuthPanel ? (
              <MdExpandLess className="text-[18px]" aria-hidden />
            ) : (
              <MdExpandMore className="text-[18px]" aria-hidden />
            )}
          </button>
          <button
            className="flex h-9 items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
            onClick={openNewPeerTab}
            disabled={typeof window === "undefined"}
            type="button"
            title="New view (same storage): shares local state and can see private subtrees"
            aria-label="New view (same storage)"
          >
            <MdOpenInNew className="text-[18px]" />
            <span>New view</span>
          </button>
          <button
            className="flex h-9 items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
            onClick={(e) => void openNewIsolatedPeerTab({ autoInvite: !e.altKey, rootNodeId: ROOT_ID })}
            disabled={typeof window === "undefined" || (authEnabled && !(authCanIssue || authCanDelegate))}
            type="button"
            title={
              authEnabled && !(authCanIssue || authCanDelegate)
                ? "Verify-only tabs can’t mint invites. Open a minting peer (or import a grant with share permission)."
                : "New device (isolated): separate storage (no shared keys/private-roots). Auto-invite; Alt+click opens join-only."
            }
            aria-label="New device (isolated)"
          >
            <MdLockOutline className="text-[18px]" />
            <span>New device</span>
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
      {showPeersPanel && <PeersPanel {...peersPanelProps} />}
      {showAuthPanel && <SharingAuthPanel {...sharingAuthPanelProps} />}
      <div ref={treeParentRef} className="max-h-[560px] overflow-auto">
        <div style={{ height: `${treeVirtualizer.getTotalSize()}px`, position: "relative" }} className="w-full">
          {treeVirtualizer.getVirtualItems().map((item) => {
            const entry = visibleNodes[item.index];
            if (!entry) return null;
            return (
              <div
                key={item.key}
                data-index={item.index}
                ref={measureTreeElement}
                className="absolute left-0 top-0 w-full"
                style={{ transform: `translateY(${item.start}px)` }}
              >
                <TreeRow
                  node={entry.node}
                  depth={entry.depth}
                  collapse={collapse}
                  onToggle={toggleCollapse}
                  onShare={openShareForNode}
                  onSetValue={onSetValue}
                  onAddChild={onAddChild}
                  onDelete={onDelete}
                  onMove={onMove}
                  onMoveToRoot={onMoveToRoot}
                  onToggleLiveChildren={onToggleLiveChildren}
                  privateRoots={privateRoots}
                  onTogglePrivateRoot={togglePrivateRoot}
                  peers={peers}
                  selfPeerId={selfPeerId}
                  busy={busy}
                  authEnabled={authEnabled}
                  canManageCapabilities={canManageCapabilities}
                  authBusy={authBusy}
                  issuedGrantRecords={issuedGrantRecords}
                  hardRevokedTokenIds={hardRevokedTokenIds}
                  onToggleHardRevokedTokenId={toggleHardRevokedTokenId}
                  onGrantToReplicaPubkey={grantSubtreeToReplicaPubkey}
                  scopeRootId={scopeRootId}
                  canWritePayload={canWritePayload}
                  canWriteStructure={canWriteStructure}
                  canDelete={canDelete}
                  liveChildren={liveChildrenParents.has(entry.node.id)}
                  meta={meta}
                  childrenByParent={childrenByParent}
                />
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

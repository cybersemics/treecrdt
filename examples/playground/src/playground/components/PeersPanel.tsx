import React from "react";
import { MdLockOutline, MdOpenInNew } from "react-icons/md";

import { ROOT_ID } from "../constants";
import type { PeerInfo } from "../types";

export function PeersPanel({
  docId,
  selfPeerId,
  syncServerUrl,
  setSyncServerUrl,
  syncServerConnected,
  syncServerBusy,
  syncServerError,
  connectSyncServer,
  disconnectSyncServer,
  authEnabled,
  authCanIssue,
  authCanDelegate,
  openNewIsolatedPeerTab,
  openNewPeerTab,
  peers,
}: {
  docId: string;
  selfPeerId: string | null;
  syncServerUrl: string;
  setSyncServerUrl: (next: string) => void;
  syncServerConnected: boolean;
  syncServerBusy: boolean;
  syncServerError: string | null;
  connectSyncServer: () => void;
  disconnectSyncServer: () => void;
  authEnabled: boolean;
  authCanIssue: boolean;
  authCanDelegate: boolean;
  openNewIsolatedPeerTab: (opts: { autoInvite: boolean; rootNodeId?: string }) => Promise<void>;
  openNewPeerTab: () => void;
  peers: PeerInfo[];
}) {
  return (
    <div className="mb-3 rounded-xl border border-slate-800/80 bg-slate-950/40 p-3 text-xs text-slate-300">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Document</div>
          <div className="font-mono text-slate-200">{docId}</div>
        </div>
        <div className="text-right">
          <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Public key</div>
          <div className="font-mono text-slate-200">{selfPeerId ?? "-"}</div>
        </div>
      </div>
      <div className="mt-3 flex items-center justify-between gap-2">
        <div className="text-slate-400">Peers</div>
        <div className="flex items-center gap-2">
          <button
            className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
            type="button"
            onClick={() => void openNewIsolatedPeerTab({ autoInvite: true, rootNodeId: ROOT_ID })}
            disabled={typeof window === "undefined" || (authEnabled && !(authCanIssue || authCanDelegate))}
            title={
              authEnabled && !(authCanIssue || authCanDelegate)
                ? "Verify-only tabs can’t mint invites. Open a minting peer (or import a grant with share permission)."
                : "New device (isolated): separate storage, auto-invite"
            }
          >
            <MdLockOutline className="text-[16px]" />
            New device
          </button>
          <button
            className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
            type="button"
            onClick={openNewPeerTab}
            disabled={typeof window === "undefined"}
            title="New view (same storage): shares local state"
          >
            <MdOpenInNew className="text-[16px]" />
            New view
          </button>
        </div>
      </div>

      <div className="mt-3">
        <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Remote sync server</div>
        <div className="mt-1 flex items-center gap-2">
          <input
            className="h-8 w-full rounded-lg border border-slate-800 bg-slate-950/40 px-2 font-mono text-[11px] text-slate-200 placeholder:text-slate-600 focus:border-accent focus:outline-none"
            value={syncServerUrl}
            onChange={(e) => setSyncServerUrl(e.target.value)}
            placeholder="wss://your-host/sync  (or https://your-host)"
            disabled={syncServerBusy || syncServerConnected}
            spellCheck={false}
          />
          {syncServerConnected ? (
            <button
              className="h-8 shrink-0 rounded-lg border border-slate-700 bg-slate-800/70 px-3 text-[11px] font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
              type="button"
              onClick={disconnectSyncServer}
              disabled={syncServerBusy}
              title="Disconnect"
            >
              Disconnect
            </button>
          ) : (
            <button
              className="h-8 shrink-0 rounded-lg border border-slate-700 bg-slate-800/70 px-3 text-[11px] font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
              type="button"
              onClick={connectSyncServer}
              disabled={syncServerBusy || !syncServerUrl.trim()}
              title="Connect"
            >
              {syncServerBusy ? "Connecting…" : "Connect"}
            </button>
          )}
        </div>
        {syncServerError && <div className="mt-1 text-[11px] text-rose-200">{syncServerError}</div>}
        <div className="mt-1 text-[10px] text-slate-500">
          If the playground is loaded over https you must use wss
        </div>
      </div>

      <div className="mt-2 max-h-32 overflow-auto pr-1">
        <div className="flex items-center justify-between gap-2 py-1">
          <span className="font-mono text-slate-200">
            {selfPeerId ?? "-"} <span className="text-[10px] text-slate-500">(you)</span>
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
        <div className="mt-2 text-slate-500">Only you right now. Open another tab with the same `doc`.</div>
      )}
    </div>
  );
}

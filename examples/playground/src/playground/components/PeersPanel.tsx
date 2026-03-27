import React from "react";
import { MdCheckCircle, MdCloudOff, MdCloudQueue, MdErrorOutline, MdSync } from "react-icons/md";

import { PLAYGROUND_PUBLIC_SYNC_SERVER_URL } from "../constants";
import type { PeerInfo, RemoteSyncStatus, SyncTransportMode } from "../types";

function formatPeerId(id: string): string {
  if (id.startsWith("remote:")) return `remote(${id.slice("remote:".length)})`;
  return id.length > 18 ? `${id.slice(0, 8)}…${id.slice(-6)}` : id;
}

function transportModeButtonClass(active: boolean): string {
  return active
    ? "border-accent bg-accent/15 text-white"
    : "border-slate-700 bg-slate-900/70 text-slate-300 hover:border-slate-500 hover:text-white";
}

function remoteStatusTone(status: RemoteSyncStatus): string {
  switch (status.state) {
    case "connected":
      return "border-emerald-500/40 bg-emerald-500/10 text-emerald-100";
    case "connecting":
      return "border-sky-500/40 bg-sky-500/10 text-sky-100";
    case "disabled":
      return "border-slate-700 bg-slate-900/70 text-slate-400";
    case "missing_url":
      return "border-amber-500/30 bg-amber-500/10 text-amber-100";
    case "invalid":
    case "error":
      return "border-rose-500/40 bg-rose-500/10 text-rose-100";
  }
}

function RemoteStatusIcon({ status }: { status: RemoteSyncStatus }) {
  if (status.state === "connected") {
    return <MdCheckCircle className="text-[14px]" />;
  }
  if (status.state === "connecting") {
    return <MdSync className="text-[14px]" />;
  }
  if (status.state === "disabled") {
    return <MdCloudOff className="text-[14px]" />;
  }
  return <MdErrorOutline className="text-[14px]" />;
}

export function PeersPanel({
  online,
  setOnline,
  syncTransportMode,
  setSyncTransportMode,
  syncServerUrl,
  setSyncServerUrl,
  remoteSyncStatus,
  peers,
}: {
  online: boolean;
  setOnline: React.Dispatch<React.SetStateAction<boolean>>;
  syncTransportMode: SyncTransportMode;
  setSyncTransportMode: React.Dispatch<React.SetStateAction<SyncTransportMode>>;
  syncServerUrl: string;
  setSyncServerUrl: React.Dispatch<React.SetStateAction<string>>;
  remoteSyncStatus: RemoteSyncStatus;
  peers: PeerInfo[];
}) {
  const requiresRemoteUrl = syncTransportMode !== "local";
  const hasRemoteUrl = syncServerUrl.trim().length > 0;

  return (
    <div
      id="playground-connections-panel"
      className="mb-3 rounded-xl border border-slate-800/80 bg-slate-950/40 p-3 text-xs text-slate-300"
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Connections</div>
          <div className="mt-1 max-w-xl text-[11px] text-slate-400">
            Choose how this tab syncs. Local tabs use `BroadcastChannel`. Remote server uses a websocket sync endpoint.
          </div>
        </div>
        <button
          className={`flex h-8 items-center gap-2 rounded-lg border px-3 text-[11px] font-semibold transition ${
            online
              ? "border-slate-700 bg-slate-800/70 text-slate-200 hover:border-accent hover:text-white"
              : "border-amber-500/60 bg-amber-500/10 text-amber-100 hover:border-amber-400"
          }`}
          onClick={() => setOnline((v) => !v)}
          type="button"
          title={online ? "Pause sync activity" : "Resume sync activity"}
        >
          {online ? <MdCloudQueue className="text-[16px]" /> : <MdCloudOff className="text-[16px]" />}
          <span>{online ? "Sync enabled" : "Sync paused"}</span>
        </button>
      </div>

      <div className="mt-3 rounded-lg border border-slate-800/70 bg-slate-950/30 p-2">
        <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Transport</div>
        <div className="mt-2 flex flex-wrap gap-2">
          <button
            type="button"
            className={`rounded-md border px-3 py-1.5 text-[11px] font-semibold transition ${transportModeButtonClass(syncTransportMode === "local")}`}
            onClick={() => setSyncTransportMode("local")}
          >
            Local tabs
          </button>
          <button
            type="button"
            className={`rounded-md border px-3 py-1.5 text-[11px] font-semibold transition ${transportModeButtonClass(syncTransportMode === "remote")}`}
            onClick={() => setSyncTransportMode("remote")}
          >
            Remote server
          </button>
          <button
            type="button"
            className={`rounded-md border px-3 py-1.5 text-[11px] font-semibold transition ${transportModeButtonClass(syncTransportMode === "hybrid")}`}
            onClick={() => setSyncTransportMode("hybrid")}
          >
            Hybrid
          </button>
        </div>
        <div className="mt-2 text-[11px] text-slate-500">
          {syncTransportMode === "local" && "Only same-origin tabs in this browser will sync."}
          {syncTransportMode === "remote" && "Only the configured websocket sync server will be used."}
          {syncTransportMode === "hybrid" && "Use both same-origin tabs and the configured websocket sync server."}
        </div>
      </div>

      <div className="mt-3 rounded-lg border border-slate-800/70 bg-slate-950/30 p-2">
        <div className="flex items-center justify-between gap-2">
          <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Remote sync server</div>
          <div
            className={`inline-flex items-center gap-1.5 rounded-full border px-2 py-1 text-[10px] font-semibold ${remoteStatusTone(remoteSyncStatus)}`}
            title={remoteSyncStatus.detail}
          >
            <RemoteStatusIcon status={remoteSyncStatus} />
            <span>
              {remoteSyncStatus.state === "connected" && "Connected"}
              {remoteSyncStatus.state === "connecting" && "Connecting"}
              {remoteSyncStatus.state === "disabled" && "Inactive"}
              {remoteSyncStatus.state === "missing_url" && "Missing URL"}
              {remoteSyncStatus.state === "invalid" && "Invalid URL"}
              {remoteSyncStatus.state === "error" && "Unreachable"}
            </span>
          </div>
        </div>
        <div className="mt-1 flex items-center gap-2">
          <input
            className="w-full rounded-md border border-slate-700 bg-slate-900/70 px-2 py-1.5 font-mono text-[11px] text-slate-100 placeholder:text-slate-500"
            value={syncServerUrl}
            onChange={(event) => {
              const next = event.target.value;
              setSyncServerUrl(next);
              if (syncTransportMode === "local" && next.trim().length > 0) {
                setSyncTransportMode("hybrid");
              }
            }}
            placeholder={`${PLAYGROUND_PUBLIC_SYNC_SERVER_URL} or ws://localhost:8787/sync`}
            spellCheck={false}
          />
          <button
            type="button"
            className="rounded-md border border-slate-700 px-2 py-1.5 text-[11px] font-semibold text-slate-300 transition hover:border-slate-500 hover:text-white"
            onClick={() => {
              setSyncServerUrl(PLAYGROUND_PUBLIC_SYNC_SERVER_URL);
              if (syncTransportMode === "local") {
                setSyncTransportMode("hybrid");
              }
            }}
            title="Use the public emhub.net sync server"
          >
            Use public
          </button>
          <button
            type="button"
            className="rounded-md border border-slate-700 px-2 py-1.5 text-[11px] font-semibold text-slate-300 transition hover:border-slate-500 hover:text-white disabled:opacity-50"
            onClick={() => setSyncServerUrl("")}
            disabled={syncServerUrl.trim().length === 0}
            title="Clear remote sync server URL"
          >
            Clear
          </button>
        </div>
        <div className="mt-2 text-[11px] text-slate-400">{remoteSyncStatus.detail}</div>
        {!requiresRemoteUrl && !hasRemoteUrl && (
          <div className="mt-1 text-[10px] text-slate-500">Optional in local-only mode.</div>
        )}
      </div>

      <div className="mt-3 rounded-lg border border-slate-800/70 bg-slate-950/30 p-2">
        <div className="flex items-center justify-between gap-2">
          <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Connected peers</div>
          <div className="text-[10px] text-slate-500">{peers.length}</div>
        </div>
        <div className="mt-2 max-h-32 overflow-auto pr-1">
          {peers.map((p) => (
            <div key={p.id} className="flex items-center justify-between gap-2 py-1">
              <span className="font-mono text-slate-200">{formatPeerId(p.id)}</span>
              <span className="text-[10px] text-slate-500">{Math.max(0, Date.now() - p.lastSeen)}ms</span>
            </div>
          ))}
          {peers.length === 0 && (
            <div className="text-[11px] text-slate-500">
              No peers connected yet. Open another tab or configure a remote server.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

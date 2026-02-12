import React from "react";
import { MdContentCopy, MdGroup, MdLockOutline, MdOpenInNew, MdVpnKey } from "react-icons/md";

import type { Status, StorageMode } from "../types";

export function PlaygroundHeader({
  status,
  storage,
  opfsAvailable,
  joinMode,
  profileId,
  selfPeerId,
  selfPeerIdShort,
  onCopyPubkey,
  onSelectStorage,
  onReset,
  onExpandAll,
  onCollapseAll,
  error,
}: {
  status: Status;
  storage: StorageMode;
  opfsAvailable: boolean;
  joinMode: boolean;
  profileId: string | null;
  selfPeerId: string | null;
  selfPeerIdShort: string | null;
  onCopyPubkey: () => void;
  onSelectStorage: (next: StorageMode) => void;
  onReset: () => void;
  onExpandAll: () => void;
  onCollapseAll: () => void;
  error: string | null;
}) {
  const stateBadge =
    status === "ready" ? "bg-emerald-500/80" : status === "error" ? "bg-rose-500/80" : "bg-amber-400/80";

  return (
    <header className="flex flex-col gap-2 rounded-2xl bg-slate-900/60 p-4 shadow-xl shadow-black/20 ring-1 ring-slate-800/60 backdrop-blur">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2 text-lg font-semibold text-slate-50">TreeCRDT</div>
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

      <div className="flex flex-wrap items-center gap-2 text-xs text-slate-300">
        <span
          className="flex items-center gap-2 rounded-full border border-slate-800/70 bg-slate-950/30 px-3 py-1 font-semibold text-slate-200"
          title={
            profileId
              ? joinMode
                ? "Isolated storage profile (join-only)"
                : "Isolated storage profile"
              : "Default (shared) storage profile"
          }
        >
          {joinMode ? (
            <MdLockOutline className="text-[14px]" />
          ) : profileId ? (
            <MdOpenInNew className="text-[14px]" />
          ) : (
            <MdGroup className="text-[14px]" />
          )}
          <span className="text-slate-400">Device</span>
          <span className="font-mono">{profileId ?? "default"}</span>
          {joinMode ? (
            <span className="text-slate-500">(join-only)</span>
          ) : profileId ? (
            <span className="text-slate-500">(isolated)</span>
          ) : null}
        </span>

        <button
          type="button"
          data-testid="self-pubkey"
          className="flex items-center gap-2 rounded-full border border-slate-800/70 bg-slate-950/30 px-3 py-1 font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
          onClick={onCopyPubkey}
          disabled={!selfPeerId}
          title={selfPeerId ?? "Initializing pubkey…"}
          aria-label="Copy my pubkey"
        >
          <MdVpnKey className="text-[14px]" />
          <span className="font-mono">{selfPeerIdShort ?? "(initializing)"}</span>
          <MdContentCopy className="text-[14px]" />
        </button>

        <div
          className="flex items-center overflow-hidden rounded-full border border-slate-800/70 bg-slate-950/30 text-xs font-semibold"
          aria-label="Storage selector"
        >
          <button
            type="button"
            className={`px-3 py-1 text-slate-200 transition hover:text-white ${
              storage === "memory" ? "bg-slate-900/70" : "bg-transparent"
            }`}
            onClick={() => {
              if (storage !== "memory") onSelectStorage("memory");
            }}
            disabled={status === "booting"}
            title="In-memory storage"
          >
            Memory
          </button>
          <button
            type="button"
            className={`px-3 py-1 text-slate-200 transition hover:text-white ${
              storage === "opfs" ? "bg-slate-900/70" : "bg-transparent"
            } ${!opfsAvailable ? "opacity-50" : ""}`}
            onClick={() => {
              if (storage !== "opfs") onSelectStorage("opfs");
            }}
            disabled={status === "booting" || !opfsAvailable}
            title={
              opfsAvailable ? "Persistent OPFS storage" : "OPFS may be blocked by the browser; falls back to memory."
            }
          >
            OPFS
          </button>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        <button
          className="rounded-lg border border-slate-700 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-slate-500 hover:text-white disabled:opacity-50"
          onClick={onReset}
          disabled={status !== "ready"}
        >
          Reset
        </button>
        <button
          className="rounded-lg border border-slate-700 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-slate-500 hover:text-white"
          onClick={onExpandAll}
        >
          Expand
        </button>
        <button
          className="rounded-lg border border-slate-700 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-slate-500 hover:text-white"
          onClick={onCollapseAll}
        >
          Collapse
        </button>
      </div>

      {error && (
        <div className="rounded-lg border border-rose-400/50 bg-rose-500/10 px-4 py-2 text-sm text-rose-50">
          {error}
        </div>
      )}
    </header>
  );
}


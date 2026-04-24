import React from "react";
import { MdExpandLess, MdExpandMore } from "react-icons/md";

import type { BulkAddProgress } from "../types";

import { ParentPicker } from "./ParentPicker";

export function ComposerPanel({
  composerOpen,
  setComposerOpen,
  nodeList,
  parentChoice,
  setParentChoice,
  newNodeValue,
  setNewNodeValue,
  nodeCount,
  setNodeCount,
  maxNodeCount,
  fanout,
  setFanout,
  onAddNodes,
  ready,
  busy,
  bulkAddProgress,
  canWritePayload,
  canWriteStructure,
}: {
  composerOpen: boolean;
  setComposerOpen: React.Dispatch<React.SetStateAction<boolean>>;
  nodeList: Array<{ id: string; label: string; depth: number }>;
  parentChoice: string;
  setParentChoice: (next: string) => void;
  newNodeValue: string;
  setNewNodeValue: React.Dispatch<React.SetStateAction<string>>;
  nodeCount: number;
  setNodeCount: React.Dispatch<React.SetStateAction<number>>;
  maxNodeCount: number;
  fanout: number;
  setFanout: React.Dispatch<React.SetStateAction<number>>;
  onAddNodes: (parentId: string, count: number, opts: { fanout: number }) => void | Promise<void>;
  ready: boolean;
  busy: boolean;
  bulkAddProgress: BulkAddProgress | null;
  canWritePayload: boolean;
  canWriteStructure: boolean;
}) {
  const containerPadding = composerOpen ? "p-5" : "p-3";
  const headerMargin = composerOpen ? "mb-3" : "mb-0";
  const [progressNowMs, setProgressNowMs] = React.useState(() => Date.now());

  React.useEffect(() => {
    if (!bulkAddProgress) return;
    setProgressNowMs(Date.now());
    const timer = window.setInterval(() => setProgressNowMs(Date.now()), 200);
    return () => window.clearInterval(timer);
  }, [bulkAddProgress]);

  const elapsedMs = bulkAddProgress ? Math.max(0, progressNowMs - bulkAddProgress.startedAtMs) : 0;
  const progressRatio = bulkAddProgress ? Math.min(1, bulkAddProgress.completed / Math.max(1, bulkAddProgress.total)) : 0;
  const etaMs =
    bulkAddProgress &&
    bulkAddProgress.phase === "creating" &&
    bulkAddProgress.completed > 0 &&
    bulkAddProgress.completed < bulkAddProgress.total
      ? Math.round((elapsedMs / bulkAddProgress.completed) * (bulkAddProgress.total - bulkAddProgress.completed))
      : null;

  const formatDuration = (ms: number): string => {
    const totalSeconds = Math.max(0, Math.round(ms / 1000));
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return minutes > 0 ? `${minutes}m ${String(seconds).padStart(2, "0")}s` : `${seconds}s`;
  };

  const submitLabel = bulkAddProgress
    ? bulkAddProgress.phase === "creating"
      ? `Loading ${Math.round(progressRatio * 100)}%`
      : "Finishing..."
    : `Add node${nodeCount > 1 ? "s" : ""}`;

  return (
    <div
      className={`rounded-2xl bg-slate-900/60 shadow-lg shadow-black/20 ring-1 ring-slate-800/60 ${containerPadding}`}
    >
      <div className={`${headerMargin} flex flex-wrap items-center justify-between gap-2`}>
        <div className="text-sm font-semibold uppercase tracking-wide text-slate-400">Composer</div>
        <button
          type="button"
          className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white"
          onClick={() => setComposerOpen((prev) => !prev)}
          aria-expanded={composerOpen}
          title={composerOpen ? "Hide composer" : "Show composer"}
        >
          {composerOpen ? <MdExpandLess className="text-[16px]" /> : <MdExpandMore className="text-[16px]" />}
          {composerOpen ? "Hide" : "Show"}
        </button>
      </div>
      {composerOpen ? (
        <>
          <form
            className="flex flex-col gap-3 md:flex-row md:items-end"
            onSubmit={(e) => {
              e.preventDefault();
              void onAddNodes(parentChoice, nodeCount, { fanout });
            }}
          >
            <ParentPicker nodeList={nodeList} value={parentChoice} onChange={setParentChoice} disabled={!ready} />
            <label className="w-full space-y-2 text-sm text-slate-200 md:w-52">
              <span>Value (optional)</span>
              <input
                type="text"
                value={newNodeValue}
                onChange={(e) => setNewNodeValue(e.target.value)}
                placeholder="Stored as payload bytes"
                className="w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                disabled={!ready || busy || !canWritePayload}
              />
            </label>
            <label className="flex flex-col text-sm text-slate-200">
              <span>Node count</span>
              <input
                type="number"
                min={1}
                max={maxNodeCount}
                value={nodeCount}
                onChange={(e) => {
                  const next = Number(e.target.value);
                  if (!Number.isFinite(next)) {
                    setNodeCount(0);
                    return;
                  }
                  setNodeCount(Math.max(0, Math.min(maxNodeCount, Math.floor(next))));
                }}
                className="w-28 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                disabled={!ready || busy}
              />
            </label>
            <label className="flex flex-col text-sm text-slate-200">
              <span>Fanout</span>
              <select
                value={fanout}
                onChange={(e) => setFanout(Number(e.target.value) || 0)}
                className="w-28 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                disabled={!ready || busy}
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
              disabled={!ready || busy || nodeCount <= 0 || !canWriteStructure}
              aria-busy={bulkAddProgress ? true : undefined}
            >
              {submitLabel}
            </button>
          </form>
          {bulkAddProgress ? (
            <div className="mt-3 rounded-xl border border-accent/30 bg-accent/10 p-3 text-xs text-slate-100" aria-live="polite">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <span className="font-semibold">
                  {bulkAddProgress.phase === "creating"
                    ? `Creating ${bulkAddProgress.completed} of ${bulkAddProgress.total} nodes`
                    : `Applying ${bulkAddProgress.total} generated nodes`}
                </span>
                <span className="font-mono text-[11px] text-slate-300">Elapsed {formatDuration(elapsedMs)}</span>
              </div>
              <div className="mt-2 h-2 overflow-hidden rounded-full bg-slate-800/80">
                <div
                  className="h-full rounded-full bg-accent transition-[width] duration-200"
                  style={{
                    width: `${bulkAddProgress.completed === 0 ? 0 : Math.max(6, Math.round(progressRatio * 100))}%`,
                  }}
                />
              </div>
              <div className="mt-2 flex flex-wrap items-center justify-between gap-2 text-[11px] text-slate-300">
                <span>
                  {bulkAddProgress.phase === "creating"
                    ? "Preparing inserts locally before the tree view refreshes."
                    : "Finalizing local tree state and sync updates."}
                </span>
                <span className="font-mono">
                  {etaMs !== null ? `ETA ~${formatDuration(etaMs)}` : bulkAddProgress.phase === "applying" ? "ETA settling..." : ""}
                </span>
              </div>
            </div>
          ) : null}
        </>
      ) : null}
    </div>
  );
}

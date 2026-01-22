import React, { useEffect, useState } from "react";
import {
  MdAdd,
  MdChevronRight,
  MdDeleteOutline,
  MdExpandMore,
  MdHome,
  MdKeyboardArrowDown,
  MdKeyboardArrowUp,
  MdOutlineRssFeed,
} from "react-icons/md";

import { ROOT_ID } from "../constants";
import type { CollapseState, DisplayNode, NodeMeta } from "../types";

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
  meta: Record<string, NodeMeta>;
  childrenByParent: Record<string, string[]>;
}) {
  const isCollapsed = collapse.defaultCollapsed ? !collapse.overrides.has(node.id) : collapse.overrides.has(node.id);
  const isRoot = node.id === ROOT_ID;
  const metaInfo = meta[node.id];
  const siblings = metaInfo?.parentId ? childrenByParent[metaInfo.parentId] ?? [] : [];
  const canMoveUp = !isRoot && metaInfo && siblings.indexOf(node.id) > 0;
  const canMoveDown =
    !isRoot && metaInfo && siblings.indexOf(node.id) !== -1 && siblings.indexOf(node.id) < siblings.length - 1;
  const childrenLoaded = Object.prototype.hasOwnProperty.call(childrenByParent, node.id);
  const childCount = childrenLoaded ? (childrenByParent[node.id]?.length ?? 0) : null;
  const toggleDisabled = childrenLoaded && childCount === 0 && isCollapsed;
  const [isEditing, setIsEditing] = useState(false);
  const [draftValue, setDraftValue] = useState(node.value);

  useEffect(() => {
    if (!isEditing) setDraftValue(node.value);
  }, [isEditing, node.value]);

  return (
    <div
      className="group rounded-lg bg-slate-950/40 px-2 py-2 ring-1 ring-slate-800/50 transition hover:bg-slate-950/55 hover:ring-slate-700/70"
      style={{ paddingLeft: `${depth * 16}px` }}
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
              <button type="button" className="block w-full text-left" onClick={() => setIsEditing(true)} title="Click to edit">
                <span className="block truncate text-sm font-semibold text-white">{node.label}</span>
              </button>
            )}
          </div>
        </div>
        <div className="flex flex-wrap gap-1.5">
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
            className="flex h-9 w-9 items-center justify-center rounded-lg border border-slate-800/70 bg-slate-900/60 text-slate-200 transition hover:border-accent hover:text-white"
            onClick={() => onAddChild(node.id)}
            aria-label="Add child"
            title="Add child"
          >
            <MdAdd className="text-[22px]" />
          </button>
          {!isRoot && (
            <>
              <button
                className="flex h-9 w-9 items-center justify-center rounded-lg border border-slate-800/70 bg-slate-900/60 text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                onClick={() => onMove(node.id, "up")}
                disabled={!canMoveUp}
                aria-label="Move up"
                title="Move up"
              >
                <MdKeyboardArrowUp className="text-[22px]" />
              </button>
              <button
                className="flex h-9 w-9 items-center justify-center rounded-lg border border-slate-800/70 bg-slate-900/60 text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                onClick={() => onMove(node.id, "down")}
                disabled={!canMoveDown}
                aria-label="Move down"
                title="Move down"
              >
                <MdKeyboardArrowDown className="text-[22px]" />
              </button>
              <button
                className="flex h-9 w-9 items-center justify-center rounded-lg border border-slate-800/70 bg-slate-900/60 text-slate-200 transition hover:border-accent hover:text-white"
                onClick={() => onMoveToRoot(node.id)}
                aria-label="Move to root"
                title="Move to root"
              >
                <MdHome className="text-[20px]" />
              </button>
              <button
                className="flex h-9 w-9 items-center justify-center rounded-lg border border-rose-400/80 bg-rose-500/10 text-rose-100 transition hover:bg-rose-500/20"
                onClick={() => onDelete(node.id)}
                aria-label="Delete"
                title="Delete"
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


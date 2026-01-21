import type { Operation } from "@treecrdt/interface";

import { ROOT_ID } from "./constants";
import type { NodeMeta, TreeState } from "./types";

export function applyChildrenLoaded(state: TreeState, parentId: string, children: string[]): TreeState {
  const nextChildrenByParent: Record<string, string[]> = { ...state.childrenByParent, [parentId]: children };
  const nextIndex: Record<string, NodeMeta> = { ...state.index };

  const ensureNode = (id: string): NodeMeta => {
    const existing = nextIndex[id];
    if (existing) return existing;
    const meta: NodeMeta = { parentId: null, order: 0, childCount: 0, deleted: false };
    nextIndex[id] = meta;
    return meta;
  };

  ensureNode(ROOT_ID);
  nextIndex[ROOT_ID] = { ...nextIndex[ROOT_ID]!, parentId: null, deleted: false };
  if (!Object.prototype.hasOwnProperty.call(nextChildrenByParent, ROOT_ID)) nextChildrenByParent[ROOT_ID] = [];

  const parentMeta = ensureNode(parentId);
  nextIndex[parentId] = {
    ...parentMeta,
    parentId: parentId === ROOT_ID ? null : parentMeta.parentId,
    deleted: false,
    childCount: children.length,
  };

  const newSet = new Set(children);
  const prevChildren = state.childrenByParent[parentId];
  if (prevChildren) {
    for (const childId of prevChildren) {
      if (newSet.has(childId)) continue;
      const meta = nextIndex[childId];
      if (meta && meta.parentId === parentId) {
        nextIndex[childId] = { ...meta, parentId: null, order: 0 };
      }
    }
  }

  for (let i = 0; i < children.length; i++) {
    const childId = children[i]!;
    const existing = ensureNode(childId);
    const loaded = Object.prototype.hasOwnProperty.call(nextChildrenByParent, childId);
    const childCount = loaded ? nextChildrenByParent[childId]!.length : existing.childCount;
    nextIndex[childId] = { ...existing, parentId, order: i, deleted: false, childCount };
  }

  return { index: nextIndex, childrenByParent: nextChildrenByParent };
}

export function parentsAffectedByOps(state: TreeState, ops: Operation[]): Set<string> {
  const out = new Set<string>();
  for (const op of ops) {
    const kind = op.kind;
    if (kind.type === "insert") {
      out.add(kind.parent);
    } else if (kind.type === "move") {
      out.add(kind.newParent);
      const prevParent = state.index[kind.node]?.parentId;
      if (prevParent) out.add(prevParent);
    } else if (kind.type === "payload") {
      // Payload ops do not affect tree structure.
    } else {
      const prevParent = state.index[kind.node]?.parentId;
      if (prevParent) out.add(prevParent);
    }
  }
  return out;
}

export function flattenForSelectState(
  childrenByParent: Record<string, string[]>,
  getLabel?: (id: string) => string
): Array<{ id: string; label: string; depth: number }> {
  const acc: Array<{ id: string; label: string; depth: number }> = [];
  const stack: Array<{ id: string; depth: number }> = [{ id: ROOT_ID, depth: 0 }];
  while (stack.length > 0) {
    const entry = stack.pop();
    if (!entry) break;
    const label = getLabel ? getLabel(entry.id) : entry.id === ROOT_ID ? "Root" : entry.id.slice(0, 6);
    acc.push({ id: entry.id, label, depth: entry.depth });
    const kids = childrenByParent[entry.id] ?? [];
    for (let i = kids.length - 1; i >= 0; i--) {
      stack.push({ id: kids[i]!, depth: entry.depth + 1 });
    }
  }
  return acc;
}


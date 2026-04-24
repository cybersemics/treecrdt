import type { MaterializationEvent } from '@treecrdt/interface/engine';

type PayloadUpdate = { node: string; payload: Uint8Array | null };

type MaterializationRefreshPlan = {
  payloadUpdates: PayloadUpdate[];
  parentsToRefresh: Set<string>;
};

export function materializationRefreshPlan(
  event: MaterializationEvent,
  loadedChildren: Record<string, readonly string[]>,
): MaterializationRefreshPlan {
  const payloadUpdates: PayloadUpdate[] = [];
  const parentsToRefresh = new Set<string>();

  const addLoadedParent = (id: string | null | undefined) => {
    if (id && Object.prototype.hasOwnProperty.call(loadedChildren, id)) {
      parentsToRefresh.add(id);
    }
  };

  for (const change of event.changes) {
    if (change.kind === 'payload') {
      payloadUpdates.push({ node: change.node, payload: change.payload });
      continue;
    }

    if (change.kind === 'insert') {
      payloadUpdates.push({ node: change.node, payload: change.payload });
      addLoadedParent(change.parentAfter);
    } else if (change.kind === 'move') {
      addLoadedParent(change.parentBefore);
      addLoadedParent(change.parentAfter);
    } else if (change.kind === 'delete') {
      addLoadedParent(change.parentBefore);
    } else if (change.kind === 'restore') {
      payloadUpdates.push({ node: change.node, payload: change.payload });
      addLoadedParent(change.parentAfter);
    }

    if (Object.prototype.hasOwnProperty.call(loadedChildren, change.node)) {
      parentsToRefresh.add(change.node);
    }
  }

  return { payloadUpdates, parentsToRefresh };
}

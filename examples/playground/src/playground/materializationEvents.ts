import type { MaterializationEvent } from '@justthrowaway/interface/engine';

type PayloadUpdate = { node: string; payload: Uint8Array | null };

type MaterializationRefreshPlan = {
  payloadUpdates: PayloadUpdate[];
  parentsToRefresh: Set<string>;
};

export function materializationRefreshPlan(
  event: MaterializationEvent,
): MaterializationRefreshPlan {
  const payloadUpdates: PayloadUpdate[] = [];
  const parentsToRefresh = new Set<string>();

  const addParent = (id: string | null | undefined) => {
    if (id) parentsToRefresh.add(id);
  };

  for (const change of event.changes) {
    if (change.kind === 'payload') {
      payloadUpdates.push({ node: change.node, payload: change.payload });
      continue;
    }

    if (change.kind === 'insert') {
      payloadUpdates.push({ node: change.node, payload: change.payload });
      addParent(change.parentAfter);
    } else if (change.kind === 'move') {
      addParent(change.parentBefore);
      addParent(change.parentAfter);
    } else if (change.kind === 'delete') {
      addParent(change.parentBefore);
    } else if (change.kind === 'restore') {
      payloadUpdates.push({ node: change.node, payload: change.payload });
      addParent(change.parentAfter);
    }

    parentsToRefresh.add(change.node);
  }

  return { payloadUpdates, parentsToRefresh };
}

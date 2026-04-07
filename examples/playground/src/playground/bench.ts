import { ROOT_ID } from './constants';
import type { Status } from './types';

export type PlaygroundBenchNodeTiming = {
  sourceLocalWriteStartedAtMs?: number;
  sourceLocalPersistedAtMs?: number;
  sourceLocalPreviewAppliedAtMs?: number;
  sourceRemoteQueuedAtMs?: number;
  sourceRemotePushStartedAtMs?: number;
  sourceRemotePushFinishedAtMs?: number;
  remoteOpsAppliedStartedAtMs?: number;
  payloadsRefreshedAtMs?: number;
  remoteOpsAppliedFinishedAtMs?: number;
  treeRefreshAppliedAtMs?: number;
  rowCommittedAtMs?: number;
  targetSocketMessageAtMs?: number;
  targetBackendApplyStartedAtMs?: number;
  targetBackendApplyFinishedAtMs?: number;
};

export type PlaygroundBenchState = {
  status: Status;
  totalNodes: number | null;
  headLamport: number;
  syncBusy: boolean;
  liveBusy: boolean;
};

export type PlaygroundBenchWindow = {
  nodes: Record<string, PlaygroundBenchNodeTiming>;
  lastRemoteSocketMessageAtMs?: number;
  seedBalancedTree?: (opts: { count: number; fanout: number }) => Promise<void>;
  getState?: () => PlaygroundBenchState;
};

declare global {
  interface Window {
    __treecrdtPlaygroundBench?: PlaygroundBenchWindow;
  }
}

function getPlaygroundBench(create = false): PlaygroundBenchWindow | undefined {
  if (typeof window === 'undefined') return undefined;
  if (!window.__treecrdtPlaygroundBench && create) {
    window.__treecrdtPlaygroundBench = { nodes: {} };
  }
  return window.__treecrdtPlaygroundBench;
}

// Keep benchmark-only globals behind one helper so shipped UI code does not open-code them.
export function recordBenchNodeTiming(
  nodeIds: Iterable<string>,
  patch: Partial<PlaygroundBenchNodeTiming>,
): void {
  const bench = getPlaygroundBench(true);
  if (!bench) return;
  for (const nodeId of nodeIds) {
    if (!nodeId || nodeId === ROOT_ID) continue;
    bench.nodes[nodeId] = { ...bench.nodes[nodeId], ...patch };
  }
}

export function registerBenchBindings(bindings: {
  seedBalancedTree?: PlaygroundBenchWindow['seedBalancedTree'];
  getState?: PlaygroundBenchWindow['getState'];
}): () => void {
  const bench = getPlaygroundBench(true);
  if (!bench) return () => {};
  if (bindings.seedBalancedTree) bench.seedBalancedTree = bindings.seedBalancedTree;
  if (bindings.getState) bench.getState = bindings.getState;
  return () => {
    const current = getPlaygroundBench(false);
    if (!current) return;
    if (bindings.seedBalancedTree) delete current.seedBalancedTree;
    if (bindings.getState) delete current.getState;
  };
}

export function setBenchLastRemoteSocketMessageAtNow(now = Date.now()): void {
  const bench = getPlaygroundBench(true);
  if (!bench) return;
  bench.lastRemoteSocketMessageAtMs = now;
}

export function getBenchLastRemoteSocketMessageAtMs(): number | undefined {
  return getPlaygroundBench(false)?.lastRemoteSocketMessageAtMs;
}

export function markBenchRowCommitted(nodeId: string, now = Date.now()): void {
  const bench = getPlaygroundBench(false);
  if (!bench || !nodeId || nodeId === ROOT_ID) return;
  const entry = bench.nodes[nodeId];
  if (!entry || typeof entry.rowCommittedAtMs === 'number') return;
  entry.rowCommittedAtMs = now;
}

import { createTreecrdtClient } from '@treecrdt/wa-sqlite';
import {
  runTreecrdtEngineConformanceScenario,
  treecrdtEngineConformanceScenarios,
} from '@treecrdt/engine-conformance';

type StorageKind = 'memory' | 'opfs';

export async function runTreecrdtEngineConformanceE2E(
  storage: StorageKind = 'memory',
): Promise<{ ok: true }> {
  const runId =
    typeof crypto !== 'undefined' && 'randomUUID' in crypto
      ? crypto.randomUUID()
      : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  // OPFS filenames are derived from docId, so a unique prefix per run keeps
  // persistent scenarios from seeing a previous run's data.
  const runKey = runId.replace(/[^a-z0-9]/gi, '').slice(0, 10) || 'run';

  for (const scenario of treecrdtEngineConformanceScenarios()) {
    await runTreecrdtEngineConformanceScenario(scenario, {
      docIdPrefix: `treecrdt-wa-engine-conformance-${storage}-${runKey}`,
      // Peer engines share the scenario docId, which maps to a single OPFS store;
      // they stay in-memory while persistence scenarios exercise OPFS below.
      openEngine: ({ docId }) => createTreecrdtClient({ docId }),
      openPersistentEngine:
        storage === 'opfs'
          ? ({ docId }) => createTreecrdtClient({ docId, persistent: true })
          : undefined,
    });
  }
  return { ok: true };
}

declare global {
  interface Window {
    runTreecrdtEngineConformanceE2E?: typeof runTreecrdtEngineConformanceE2E;
  }
}

if (typeof window !== 'undefined') {
  window.runTreecrdtEngineConformanceE2E = runTreecrdtEngineConformanceE2E;
}

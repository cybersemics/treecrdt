import { createTreecrdtClient } from '@treecrdt/wa-sqlite/client';
import {
  conformanceHashKey,
  conformanceSlugify,
  sqliteEngineConformanceScenarios,
} from '@treecrdt/sqlite-conformance';

type StorageKind = 'memory' | 'opfs';

function docIdFromScenario(name: string, storage: StorageKind): string {
  return `treecrdt-wa-sqlite-conformance-${storage}-${conformanceSlugify(name) || 'scenario'}`;
}

export async function runTreecrdtSqliteConformanceE2E(
  storage: StorageKind = 'memory',
): Promise<{ ok: true }> {
  const runId =
    typeof crypto !== 'undefined' && 'randomUUID' in crypto
      ? crypto.randomUUID()
      : `${Date.now()}-${Math.random().toString(16).slice(2)}`;

  const preferWorker = storage === 'opfs';

  for (const scenario of sqliteEngineConformanceScenarios()) {
    const docId = docIdFromScenario(scenario.name, storage);
    const engines: Array<{ close: () => Promise<void> }> = [];
    const runKey = runId.replace(/[^a-z0-9]/gi, '').slice(0, 10) || 'run';
    const scenarioKey = conformanceHashKey(scenario.name);
    const filenameFor = (name: string) => {
      const nameKey = (conformanceSlugify(name) || 'db').slice(0, 12);
      return `/treecrdt-c-${runKey}-${scenarioKey}-${nameKey}.db`;
    };
    const track = <T extends { close: () => Promise<void> }>(engine: T): T => {
      const originalClose = engine.close.bind(engine);
      let closed = false;
      engine.close = async () => {
        if (closed) return;
        closed = true;
        await originalClose();
      };
      engines.push(engine);
      return engine;
    };
    const openEngine = async (opts: { docId: string; name?: string }) => {
      const name = opts.name ?? 'main';
      const filename = storage === 'opfs' ? filenameFor(name) : undefined;
      return track(
        await createTreecrdtClient({ storage, preferWorker, docId: opts.docId, filename }),
      );
    };

    const client = await openEngine({ docId, name: 'main' });
    try {
      await scenario.run({
        docId,
        engine: client,
        createEngine: ({ docId, name }) => openEngine({ docId, name }),
        createPersistentEngine:
          storage === 'opfs' ? ({ docId, name }) => openEngine({ docId, name }) : undefined,
      });
    } finally {
      for (const e of engines.reverse()) {
        try {
          await e.close();
        } catch {
          // ignore close failures during cleanup
        }
      }
    }
  }
  return { ok: true };
}

declare global {
  interface Window {
    runTreecrdtSqliteConformanceE2E?: typeof runTreecrdtSqliteConformanceE2E;
  }
}

if (typeof window !== 'undefined') {
  window.runTreecrdtSqliteConformanceE2E = runTreecrdtSqliteConformanceE2E;
}

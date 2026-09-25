import { test } from 'vitest';
import type { TreecrdtEngine } from '@treecrdt/interface/engine';
import { treecrdtEngineConformanceScenarios } from '@treecrdt/engine-conformance';
import { createMemoryClient } from '../dist/index.node.js';
import { createMemorySyncBackend } from '../dist/sync.js';

// These shared scenarios exercise only operation ingestion, retrieval and clocks. The async SQLite engine's
// auth, persistence, pagination and write-event surfaces are deliberately not claimed by the memory client.
const compatible = new Set([
  'append/appendMany: idempotent + headLamport monotonic',
  'oprefs_all + ops.get: preserve order and reject missing refs',
]);

for (const scenario of treecrdtEngineConformanceScenarios().filter((item) =>
  compatible.has(item.name),
)) {
  test(`shared operation conformance: ${scenario.name}`, async () => {
    const client = await createMemoryClient();
    const docId = 'memory-conformance';
    const backend = createMemorySyncBackend(client, { docId });
    const unsupported = () => {
      throw new Error('This scenario requires an unsupported async-engine capability');
    };
    const operations: Pick<TreecrdtEngine, 'ops' | 'opRefs' | 'meta'> = {
      ops: {
        append: async (operation) => {
          client.appendOperations([operation]);
        },
        appendMany: async (batch) => {
          client.appendOperations(batch);
        },
        all: async () => client.operationsFrom(0),
        since: async (lamport) =>
          client.operationsFrom(0).filter((operation) => operation.meta.lamport > lamport),
        get: (refs) => backend.getOpsByOpRefs(refs),
        children: unsupported,
      },
      opRefs: { all: () => backend.listOpRefs({ all: {} }), children: unsupported },
      meta: { headLamport: async () => client.maxLamport(), replicaMaxCounter: unsupported },
    };
    try {
      await scenario.run({
        docId,
        engine: operations as TreecrdtEngine,
        createEngine: unsupported,
      });
    } finally {
      client.close();
    }
  });
}

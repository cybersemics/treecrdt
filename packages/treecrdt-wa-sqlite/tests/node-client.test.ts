import { expect, test } from 'vitest';
import {
  runTreecrdtEngineConformanceScenario,
  treecrdtEngineConformanceScenarios,
} from '@treecrdt/engine-conformance';
import { createTreecrdtClient } from '../dist/index.node.js';

const root = '0'.repeat(32);
const replica = Uint8Array.from({ length: 32 }, (_, i) => (i === 31 ? 1 : 0));

function nodeIdFromInt(n: number): string {
  return n.toString(16).padStart(32, '0');
}

async function createWaEngine(opts: { docId: string }) {
  return await createTreecrdtClient({
    storage: { type: 'memory' },
    runtime: { type: 'direct' },
    docId: opts.docId,
  });
}

test('createTreecrdtClient smoke: insert and read in Node', async () => {
  const client = await createWaEngine({ docId: 'wa-sqlite-node-smoke' });
  const node = nodeIdFromInt(1);

  try {
    await client.local.insert(replica, root, node, { type: 'last' }, null);
    expect(await client.tree.exists(node)).toBe(true);
    expect(await client.ops.all()).toHaveLength(1);
  } finally {
    await client.close();
  }
});

test('createTreecrdtClient rejects OPFS on Node', async () => {
  await expect(
    createTreecrdtClient({ storage: { type: 'opfs' }, docId: 'wa-sqlite-node-opfs' }),
  ).rejects.toThrow(/OPFS is not supported in Node/);
});

test('createTreecrdtClient rejects dedicated-worker runtime on Node', async () => {
  await expect(
    createTreecrdtClient({
      storage: { type: 'memory' },
      runtime: { type: 'dedicated-worker' },
      docId: 'wa-sqlite-node-worker',
    }),
  ).rejects.toThrow(/Worker runtimes are browser-only/);
});

test('createTreecrdtClient rejects shared-worker runtime on Node', async () => {
  await expect(
    createTreecrdtClient({
      storage: { type: 'memory' },
      runtime: { type: 'shared-worker' },
      docId: 'wa-sqlite-node-shared-worker',
    }),
  ).rejects.toThrow(/Worker runtimes are browser-only/);
});

for (const scenario of treecrdtEngineConformanceScenarios()) {
  test(`wa-sqlite engine conformance (node, memory): ${scenario.name}`, async () => {
    await runTreecrdtEngineConformanceScenario(scenario, {
      docIdPrefix: 'treecrdt-wa-node-conformance',
      openEngine: ({ docId }) => createWaEngine({ docId }),
    });
  });
}

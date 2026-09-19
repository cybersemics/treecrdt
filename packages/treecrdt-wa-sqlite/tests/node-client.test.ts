import { expect, test } from 'vitest';
import vm from 'node:vm';
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
  return await createTreecrdtClient({ docId: opts.docId });
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

test('createTreecrdtClient accepts cross-realm typed array payloads in Node', async () => {
  const client = await createWaEngine({ docId: 'wa-sqlite-node-cross-realm-payload' });
  const payload = vm.runInNewContext('new Uint8Array([1, 2, 3])') as Uint8Array;

  try {
    await client.local.payload(replica, root, payload);
    expect(await client.tree.getPayload(root)).toEqual(Uint8Array.from([1, 2, 3]));
  } finally {
    await client.close();
  }
});

test('createTreecrdtClient rejects persistent storage on Node', async () => {
  await expect(
    createTreecrdtClient({ docId: 'wa-sqlite-node-persistent', persistent: true }),
  ).rejects.toThrow(/persistent storage is not supported on Node/);
});

test('createTreecrdtClient rejects an empty docId', async () => {
  await expect(createTreecrdtClient({ docId: '' })).rejects.toThrow(/non-empty docId/);
});

for (const scenario of treecrdtEngineConformanceScenarios()) {
  test(`wa-sqlite engine conformance (node, memory): ${scenario.name}`, async () => {
    await runTreecrdtEngineConformanceScenario(scenario, {
      docIdPrefix: 'treecrdt-wa-node-conformance',
      openEngine: ({ docId }) => createWaEngine({ docId }),
    });
  });
}

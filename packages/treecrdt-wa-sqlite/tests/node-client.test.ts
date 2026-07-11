import { expect, test } from 'vitest';
import vm from 'node:vm';
import {
  runTreecrdtEngineConformanceScenario,
  treecrdtEngineConformanceScenarios,
} from '@treecrdt/engine-conformance';
import type { Operation } from '@treecrdt/interface';
import { createTreecrdtClient } from '../dist/index.node.js';
import { buildDirectClient } from '../dist/client.js';

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

test('appendMany sends one atomic backend call beyond the former RPC chunk boundary', async () => {
  const original: Operation = {
    meta: { id: { replica, counter: 1 }, lamport: 1 },
    kind: {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(1),
      orderKey: Uint8Array.from([0, 1]),
    },
  };
  const calls: Operation[][] = [];
  const client = await buildDirectClient(
    { storage: 'memory', docId: 'wa-sqlite-node-atomic-call' },
    async () => ({
      storage: 'memory',
      filename: ':memory:',
      db: { close: async () => {} } as any,
      api: {
        appendOps: async (ops: Operation[]) => {
          calls.push(ops);
          throw new Error('simulated storage rejection');
        },
      } as any,
    }),
  );

  try {
    const batch = Array.from({ length: 2_501 }, () => original);
    await expect(client.ops.appendMany(batch)).rejects.toThrow();
    expect(calls).toHaveLength(1);
    expect(calls[0]).toHaveLength(batch.length);
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

import type { Operation } from '@treecrdt/interface';
import { expect, test } from 'vitest';
import {
  createTreecrdtSyncBackendFromClient,
  type TreecrdtSyncBackendClient,
} from '../src/backend.js';

const operation: Operation = {
  meta: { id: { replica: new Uint8Array(32), counter: 1 }, lamport: 2 },
  kind: { type: 'payload', node: '1'.repeat(32), payload: null },
};

test('uses an explicit clock override, then the stored head, then the operation-log fallback', async () => {
  const client: TreecrdtSyncBackendClient = {
    opRefs: { all: () => [] },
    ops: { all: () => [operation], get: () => [], appendMany: () => {} },
  };
  const backend = createTreecrdtSyncBackendFromClient(client, 'document');
  expect(await backend.maxLamport()).toBe(2n);
  client.meta = { headLamport: async () => 3 };
  expect(await backend.maxLamport()).toBe(3n);
  const overridden = createTreecrdtSyncBackendFromClient(client, 'document', {
    maxLamport: () => 4n,
  });
  expect(await overridden.maxLamport()).toBe(4n);
  delete client.meta;
  delete client.ops.all;
  await expect(backend.maxLamport()).rejects.toThrow('missing client.meta.headLamport');
});

test('skips empty writes and turns synchronous storage failures into rejections', async () => {
  const backend = createTreecrdtSyncBackendFromClient(
    {
      opRefs: { all: () => [] },
      ops: {
        get: () => {
          throw new Error('read failed');
        },
        appendMany: () => {
          throw new Error('write failed');
        },
      },
    },
    'document',
  );
  await backend.applyOps([]);
  await expect(backend.applyOps([operation])).rejects.toThrow('write failed');
  await expect(backend.getOpsByOpRefs([new Uint8Array(16)])).rejects.toThrow('read failed');
});

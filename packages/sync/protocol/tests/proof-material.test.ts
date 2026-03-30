import { expect, test, vi } from 'vitest';

import type { Operation } from '@treecrdt/interface';

import { createReplayOnlySyncAuth } from '../dist/index.js';
import type { Capability } from '../dist/types.js';

test('replay-only auth filters non-auth capabilities during hello exchange', async () => {
  const storedCaps: Capability[] = [];
  const visibleCaps: Capability[] = [
    { name: 'auth.capability', value: 'token-1' },
    { name: 'peer.name', value: 'relay' },
    { name: 'auth.capability.replay', value: 'token-2' },
  ];
  const auth = createReplayOnlySyncAuth({
    docId: 'doc-replay-auth',
    authMaterialStore: {
      opAuth: {
        storeOpAuth: vi.fn(async () => {}),
        getOpAuthByOpRefs: vi.fn(async () => []),
      },
      capabilities: {
        storeCapabilities: async (caps) => {
          storedCaps.push(...caps);
        },
        listCapabilities: async () => visibleCaps,
      },
    },
  });

  const helloCaps: Capability[] = [
    { name: 'auth.capability', value: 'token-1' },
    { name: 'peer.name', value: 'author' },
    { name: 'auth.capability.replay', value: 'token-2' },
  ];

  const ackCaps = await auth.onHello?.(
    { capabilities: helloCaps, filters: [], maxLamport: 0n },
    { docId: 'doc-replay-auth' },
  );
  expect(storedCaps).toEqual([
    { name: 'auth.capability', value: 'token-1' },
    { name: 'auth.capability.replay', value: 'token-2' },
  ]);
  expect(ackCaps).toEqual([
    { name: 'auth.capability', value: 'token-1' },
    { name: 'auth.capability.replay', value: 'token-2' },
  ]);
  expect(await auth.helloCapabilities?.({ docId: 'doc-replay-auth' })).toEqual([
    { name: 'auth.capability', value: 'token-1' },
    { name: 'auth.capability.replay', value: 'token-2' },
  ]);
});

test('replay-only auth fails fast when auth sidecar is missing', async () => {
  const auth = createReplayOnlySyncAuth({
    docId: 'doc-missing-auth',
    authMaterialStore: {
      opAuth: {
        storeOpAuth: async () => {},
        getOpAuthByOpRefs: async () => [null],
      },
    },
  });

  const replica = new Uint8Array(32).fill(4);
  const op: Operation = {
    meta: {
      id: { replica, counter: 1 },
      lamport: 1,
    },
    kind: {
      type: 'insert',
      parent: '0'.repeat(32),
      node: '2'.repeat(32),
      orderKey: new Uint8Array([2]),
    },
  };

  await expect(
    auth.signOps?.([op], {
      docId: 'doc-missing-auth',
      purpose: 'reconcile',
      filterId: 'filter-2',
    }),
  ).rejects.toThrow('missing op auth for non-local replica');
});

import { expect, test } from 'vitest';

import {
  TREECRDT_IDENTITY_CHAIN_CAPABILITY,
  createTreecrdtAuthSession,
  type TreecrdtIdentityChainV1,
} from '../dist/index.js';

function testKey(byte: number): Uint8Array {
  return new Uint8Array(32).fill(byte);
}

function testIdentityChain(): TreecrdtIdentityChainV1 {
  return {
    identityPublicKey: testKey(1),
    deviceCertBytes: new Uint8Array([2]),
    replicaCertBytes: new Uint8Array([3]),
  };
}

test('auth session warms sync auth and exposes ready state', async () => {
  const session = createTreecrdtAuthSession({
    docId: 'doc-auth-session-ready',
    issuerPublicKeys: [],
    localPrivateKey: testKey(4),
    localPublicKey: testKey(5),
    allowUnsigned: true,
  });

  expect(session.getState().status).toBe('loading');
  await session.ready;
  expect(session.getState().status).toBe('ready');
});

test('auth session advertises async local identity chain without app-side wrappers', async () => {
  const session = createTreecrdtAuthSession({
    docId: 'doc-auth-session-identity',
    issuerPublicKeys: [],
    localPrivateKey: testKey(4),
    localPublicKey: testKey(5),
    allowUnsigned: true,
    localIdentityChain: async () => testIdentityChain(),
  });

  await session.ready;
  const caps =
    (await session.syncAuth.helloCapabilities?.({
      docId: 'doc-auth-session-identity',
    })) ?? [];

  expect(caps.some((cap) => cap.name === TREECRDT_IDENTITY_CHAIN_CAPABILITY)).toBe(true);
});

test('auth session accepts grouped backend and identity options', async () => {
  let listedCapabilities = 0;
  const session = createTreecrdtAuthSession({
    docId: 'doc-auth-session-grouped',
    trust: { issuerPublicKeys: [] },
    local: {
      privateKey: testKey(4),
      publicKey: testKey(5),
    },
    allowUnsigned: true,
    backend: {
      scopeEvaluator: async () => 'deny',
      capabilityStore: {
        listCapabilities: async () => {
          listedCapabilities += 1;
          return [];
        },
        storeCapabilities: async () => {},
      },
      opAuthStore: {
        storeOpAuth: async () => {},
        getOpAuthByOpRefs: async (opRefs) => opRefs.map(() => null),
      },
    },
    identity: {
      local: async () => testIdentityChain(),
      onPeer: () => undefined,
    },
  });

  await session.ready;
  expect(listedCapabilities).toBeGreaterThan(0);

  const caps =
    (await session.syncAuth.helloCapabilities?.({
      docId: 'doc-auth-session-grouped',
    })) ?? [];

  expect(caps.some((cap) => cap.name === TREECRDT_IDENTITY_CHAIN_CAPABILITY)).toBe(true);
});

test('auth session treats identity chain provider failures as best-effort', async () => {
  const errors: unknown[] = [];
  const session = createTreecrdtAuthSession({
    docId: 'doc-auth-session-identity-error',
    issuerPublicKeys: [],
    localPrivateKey: testKey(4),
    localPublicKey: testKey(5),
    allowUnsigned: true,
    localIdentityChain: async () => {
      throw new Error('identity unavailable');
    },
    onIdentityChainError: (err) => errors.push(err),
  });

  await session.ready;
  expect(session.getState().status).toBe('ready');
  expect(errors).toHaveLength(1);
});

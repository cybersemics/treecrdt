import { expect, test } from 'vitest';
import { hashes as ed25519Hashes, getPublicKey, utils as ed25519Utils } from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha512';

import type { Operation } from '@treecrdt/interface';
import {
  encodeTreecrdtOpSigInput,
  signTreecrdtOp,
  verifyTreecrdtOp,
} from '../dist/treecrdt-auth.js';

ed25519Hashes.sha512 = sha512;

const node = '00112233445566778899aabbccddeeff';
const proofRef = new Uint8Array(16).fill(7);
const meta = {
  id: { replica: new Uint8Array(32), counter: 1 },
  lamport: 1,
};

function operation(kind: Operation['kind'], state?: Uint8Array): Operation {
  return { meta: { ...meta, ...(state ? { knownState: state } : {}) }, kind };
}

function knownState(frontier = 0, ranges: Array<[number, number]> = []): Uint8Array {
  return new TextEncoder().encode(
    JSON.stringify({ entries: [{ replica: [1], frontier, ranges }] }),
  );
}

test('one signature format binds the explicit knownState field', async () => {
  const privateKey = ed25519Utils.randomSecretKey();
  const publicKey = await getPublicKey(privateKey);
  const state = knownState();
  const op = operation({ type: 'delete', node }, state);

  const input = encodeTreecrdtOpSigInput({ docId: 'doc', op, proofRef });
  const domain = new TextEncoder().encode('treecrdt/op-sig/v1');
  expect(input.slice(0, domain.length + 1)).toEqual(Uint8Array.from([...domain, 0]));
  expect(input.slice(domain.length + 1, domain.length + 17)).toEqual(proofRef);
  expect(input.slice(-(state.length + 5), -state.length)).toEqual(
    Uint8Array.from([1, 0, 0, 0, state.length]),
  );
  expect(input.slice(-state.length)).toEqual(state);

  const signature = await signTreecrdtOp({ docId: 'doc', op, proofRef, privateKey });
  await expect(
    verifyTreecrdtOp({ docId: 'doc', op, proofRef, signature, publicKey }),
  ).resolves.toBe(true);
  await expect(
    verifyTreecrdtOp({
      docId: 'doc',
      op,
      proofRef: new Uint8Array(16).fill(8),
      signature,
      publicKey,
    }),
  ).resolves.toBe(false);
  await expect(
    verifyTreecrdtOp({
      docId: 'doc',
      op: { ...op, meta: { ...op.meta, knownState: knownState(1) } },
      proofRef,
      signature,
      publicKey,
    }),
  ).resolves.toBe(false);
  await expect(
    verifyTreecrdtOp({
      docId: 'doc',
      op: operation(op.kind),
      proofRef,
      signature,
      publicKey,
    }),
  ).rejects.toThrow(/require.*knownState/i);

  expect(() =>
    encodeTreecrdtOpSigInput({
      docId: 'doc',
      op: operation({ type: 'delete', node }, new TextEncoder().encode('{ "entries": [] }')),
      proofRef,
    }),
  ).toThrow(/canonical/i);
});

test('canonical knownState accepts normalized gapped ranges', () => {
  const state = knownState(2, [
    [4, 5],
    [7, 7],
  ]);

  expect(() =>
    encodeTreecrdtOpSigInput({
      docId: 'doc',
      op: operation({ type: 'delete', node }, state),
      proofRef,
    }),
  ).not.toThrow();
});

test.each<[string, number, Array<[number, number]>]>([
  ['zero bounds', 0, [[0, 1]]],
  ['reversed ranges', 0, [[3, 2]]],
  ['frontier overlap', 2, [[2, 4]]],
  ['frontier adjacency', 2, [[3, 4]]],
  [
    'unsorted ranges',
    0,
    [
      [5, 6],
      [3, 3],
    ],
  ],
  [
    'overlapping ranges',
    0,
    [
      [3, 5],
      [5, 7],
    ],
  ],
  [
    'adjacent ranges',
    0,
    [
      [3, 4],
      [5, 6],
    ],
  ],
  ['unsafe counters', Number.MAX_SAFE_INTEGER + 1, []],
])('canonical knownState rejects %s', (_name, frontier, ranges) => {
  expect(() =>
    encodeTreecrdtOpSigInput({
      docId: 'doc',
      op: operation({ type: 'delete', node }, knownState(frontier, ranges)),
      proofRef,
    }),
  ).toThrow(/Number\.MAX_SAFE_INTEGER/);
});

test('signature policy only allows knownState on deletes', async () => {
  const privateKey = ed25519Utils.randomSecretKey();
  const publicKey = await getPublicKey(privateKey);
  const state = knownState();
  const nonDeleteKinds: Operation['kind'][] = [
    {
      type: 'insert',
      parent: '00000000000000000000000000000000',
      node,
      orderKey: new Uint8Array([1]),
    },
    {
      type: 'move',
      node,
      newParent: '00000000000000000000000000000000',
      orderKey: new Uint8Array([1]),
    },
    { type: 'payload', node, payload: null },
    { type: 'tombstone', node },
  ];

  for (const kind of nonDeleteKinds) {
    await expect(
      signTreecrdtOp({ docId: 'doc', op: operation(kind, state), proofRef, privateKey }),
    ).rejects.toThrow(/only allowed on delete/i);
  }

  const tombstone = operation({ type: 'tombstone', node });
  expect(encodeTreecrdtOpSigInput({ docId: 'doc', op: tombstone, proofRef }).at(-1)).toBe(0);
  const signature = await signTreecrdtOp({ docId: 'doc', op: tombstone, proofRef, privateKey });
  await expect(
    verifyTreecrdtOp({ docId: 'doc', op: tombstone, proofRef, signature, publicKey }),
  ).resolves.toBe(true);
});

import { beforeAll, expect, test } from 'vitest';
import {
  hashes as ed25519Hashes,
  getPublicKey,
  sign as signEd25519,
  utils as ed25519Utils,
} from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha512';

import type { Operation } from '@treecrdt/interface';
import {
  loadVersionVectorCodec,
  type VersionVectorCodec,
  type VersionVectorRange,
} from '@treecrdt/wasm/codec';
import {
  encodeTreecrdtOpSigInput,
  signTreecrdtOp,
  verifyTreecrdtOp,
} from '../dist/treecrdt-auth.js';

ed25519Hashes.sha512 = sha512;

let codec: VersionVectorCodec;
beforeAll(async () => {
  codec = await loadVersionVectorCodec();
});

const node = '00112233445566778899aabbccddeeff';
const meta = {
  id: { replica: new Uint8Array(32), counter: 1 },
  lamport: 1,
};

function operation(kind: Operation['kind'], state?: Uint8Array): Operation {
  return { meta: { ...meta, ...(state ? { knownState: state } : {}) }, kind };
}

function knownState(frontier = 0n): Uint8Array {
  return codec.encodeVersionVectorV0({
    entries: frontier === 0n ? [] : [{ replica: meta.id.replica, frontier, ranges: [] }],
  });
}

function versionVectorState(
  entries: Array<{
    replica: Uint8Array;
    frontier: bigint;
    ranges: readonly VersionVectorRange[];
  }>,
): Uint8Array {
  return codec.encodeVersionVectorV0({ entries });
}

test('one signature format binds the explicit knownState field', async () => {
  const privateKey = ed25519Utils.randomSecretKey();
  const publicKey = await getPublicKey(privateKey);
  const state = knownState();
  const op = operation({ type: 'delete', node }, state);

  const input = await encodeTreecrdtOpSigInput({ docId: 'doc', op });
  const domain = new TextEncoder().encode('treecrdt/op-sig/v1');
  expect(input.slice(0, domain.length + 1)).toEqual(Uint8Array.from([...domain, 0]));
  expect(input.slice(-(state.length + 5), -state.length)).toEqual(
    Uint8Array.from([1, 0, 0, 0, state.length]),
  );
  expect(input.slice(-state.length)).toEqual(state);

  const signature = await signTreecrdtOp({ docId: 'doc', op, privateKey });
  await expect(verifyTreecrdtOp({ docId: 'doc', op, signature, publicKey })).resolves.toBe(true);
  await expect(
    verifyTreecrdtOp({
      docId: 'doc',
      op: { ...op, meta: { ...op.meta, knownState: knownState(1n) } },
      signature,
      publicKey,
    }),
  ).resolves.toBe(false);
  await expect(
    verifyTreecrdtOp({
      docId: 'doc',
      op: operation(op.kind),
      signature,
      publicKey,
    }),
  ).rejects.toThrow(/require.*knownState/i);
});

test('strict verification rejects small-order Ed25519 identities', async () => {
  const op = operation({ type: 'tombstone', node });

  await expect(
    verifyTreecrdtOp({
      docId: 'doc',
      op,
      signature: new Uint8Array(64),
      publicKey: new Uint8Array(32),
    }),
  ).resolves.toBe(false);
});

test.each(['counter', 'lamport'] as const)(
  'operation signature input rejects an unsafe %s',
  async (field) => {
    const op = operation({ type: 'tombstone', node });
    if (field === 'counter') op.meta.id.counter = Number.MAX_SAFE_INTEGER + 1;
    else op.meta.lamport = Number.MAX_SAFE_INTEGER + 1;

    await expect(encodeTreecrdtOpSigInput({ docId: 'doc', op })).rejects.toThrow(
      /safe non-negative integer/i,
    );
  },
);

test('operation signature input accepts the maximum safe operation clocks', async () => {
  const op = operation({ type: 'tombstone', node });
  op.meta.id.counter = Number.MAX_SAFE_INTEGER;
  op.meta.lamport = Number.MAX_SAFE_INTEGER;

  await expect(encodeTreecrdtOpSigInput({ docId: 'doc', op })).resolves.toBeInstanceOf(Uint8Array);
});

test('invalid signatures are rejected before knownState is parsed', async () => {
  const privateKey = ed25519Utils.randomSecretKey();
  const publicKey = await getPublicKey(privateKey);
  const kind = { type: 'delete', node } as const;
  const signature = await signTreecrdtOp({
    docId: 'doc',
    op: operation(kind, knownState()),
    privateKey,
  });
  await expect(
    verifyTreecrdtOp({
      docId: 'doc',
      op: operation(kind, new Uint8Array([0xff])),
      signature,
      publicKey,
    }),
  ).resolves.toBe(false);
});

test('valid signatures are followed by canonical knownState validation', async () => {
  const privateKey = ed25519Utils.randomSecretKey();
  const publicKey = await getPublicKey(privateKey);
  const kind = { type: 'delete', node } as const;
  const canonicalState = versionVectorState([]);
  const malformedState = canonicalState.slice();
  malformedState[0] ^= 0xff;

  const input = await encodeTreecrdtOpSigInput({
    docId: 'doc',
    op: operation(kind, canonicalState),
  });
  input.set(malformedState, input.length - malformedState.length);
  const signature = signEd25519(input, privateKey);

  await expect(
    verifyTreecrdtOp({
      docId: 'doc',
      op: operation(kind, malformedState),
      signature,
      publicKey,
    }),
  ).rejects.toThrow(/canonical/i);
});

test('verification owns the signed knownState bytes across the async boundary', async () => {
  const privateKey = ed25519Utils.randomSecretKey();
  const publicKey = await getPublicKey(privateKey);
  const state = knownState(1n);
  const op = operation({ type: 'delete', node }, state);
  const signature = await signTreecrdtOp({ docId: 'doc', op, privateKey });

  const verification = verifyTreecrdtOp({ docId: 'doc', op, signature, publicKey });
  state.fill(0xff);

  await expect(verification).resolves.toBe(true);
});

test('signing snapshots the operation and key before asynchronous validation', async () => {
  const privateKey = ed25519Utils.randomSecretKey();
  const publicKey = await getPublicKey(privateKey);
  const state = knownState(1n);
  const op = operation({ type: 'delete', node }, state);
  const original = structuredClone(op);

  const signing = signTreecrdtOp({ docId: 'doc', op, privateKey });
  privateKey.fill(0);
  state.fill(0xff);
  op.meta.lamport = 42;
  op.kind = { type: 'tombstone', node };

  await expect(
    verifyTreecrdtOp({ docId: 'doc', op: original, signature: await signing, publicKey }),
  ).resolves.toBe(true);
});

test.each([0, 1, 31, 33])('auth profile rejects %i-byte replica ids', async (replicaLength) => {
  const state = versionVectorState([
    { replica: new Uint8Array(replicaLength).fill(1), frontier: 1n, ranges: [] },
  ]);
  await expect(
    encodeTreecrdtOpSigInput({
      docId: 'doc',
      op: operation({ type: 'delete', node }, state),
    }),
  ).rejects.toThrow(/32-byte replica ids/i);
});

test('knownState accepts the full u64 counter range', async () => {
  const state = knownState((1n << 64n) - 1n);
  await expect(
    encodeTreecrdtOpSigInput({
      docId: 'doc',
      op: operation({ type: 'delete', node }, state),
    }),
  ).resolves.toBeInstanceOf(Uint8Array);
});

test('knownState byte limit is enforced before signature work', async () => {
  const oversizedState = new Uint8Array(1024 * 1024 + 1);
  const oversizedOp = operation({ type: 'delete', node }, oversizedState);
  await expect(encodeTreecrdtOpSigInput({ docId: 'doc', op: oversizedOp })).rejects.toThrow(
    /1048576-byte operation-signature limit/i,
  );
  await expect(
    verifyTreecrdtOp({
      docId: 'doc',
      op: oversizedOp,
      signature: new Uint8Array(64),
      publicKey: meta.id.replica,
    }),
  ).rejects.toThrow(/1048576-byte operation-signature limit/i);
});

test('knownState entry limit is enforced by the auth profile', async () => {
  const entries = Array.from({ length: 4097 }, (_, index) => {
    const replica = new Uint8Array(32);
    replica[30] = (index >>> 8) & 0xff;
    replica[31] = index & 0xff;
    return { replica, frontier: 1n, ranges: [] };
  });
  const tooManyEntries = versionVectorState(entries);
  expect(tooManyEntries.length).toBeLessThanOrEqual(1024 * 1024);
  await expect(
    encodeTreecrdtOpSigInput({
      docId: 'doc',
      op: operation({ type: 'delete', node }, versionVectorState(entries.slice(0, 4096))),
    }),
  ).resolves.toBeInstanceOf(Uint8Array);
  await expect(
    encodeTreecrdtOpSigInput({
      docId: 'doc',
      op: operation({ type: 'delete', node }, tooManyEntries),
    }),
  ).rejects.toThrow(/4096-entry operation-signature limit/i);
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
      signTreecrdtOp({ docId: 'doc', op: operation(kind, state), privateKey }),
    ).rejects.toThrow(/only allowed on delete/i);
  }

  await expect(
    encodeTreecrdtOpSigInput({
      docId: 'doc',
      op: operation({ type: 'tombstone', node }, new Uint8Array()),
    }),
  ).rejects.toThrow(/only allowed on delete/i);

  const tombstone = operation({ type: 'tombstone', node });
  expect((await encodeTreecrdtOpSigInput({ docId: 'doc', op: tombstone })).at(-1)).toBe(0);
  const signature = await signTreecrdtOp({ docId: 'doc', op: tombstone, privateKey });
  await expect(
    verifyTreecrdtOp({ docId: 'doc', op: tombstone, signature, publicKey }),
  ).resolves.toBe(true);
});

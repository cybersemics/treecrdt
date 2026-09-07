import { expect, test, vi } from 'vitest';

import type { Operation } from '@treecrdt/interface';
import { loadVersionVectorCodec } from '@treecrdt/wasm/codec';
import { getEd25519PublicKey } from '../dist/ed25519.js';
import {
  encodeTreecrdtOpSigInput,
  signTreecrdtOp,
  verifyTreecrdtOp,
} from '../dist/treecrdt-auth.js';

vi.mock('@treecrdt/wasm/codec', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@treecrdt/wasm/codec')>()),
  loadVersionVectorCodec: vi.fn(),
}));

test('auth loads the codec only for canonical knownState validation', async () => {
  const loadCodec = vi.mocked(loadVersionVectorCodec);
  const unavailable = new Error('WASM unavailable');
  loadCodec.mockRejectedValue(unavailable);
  expect(loadCodec).not.toHaveBeenCalled();

  const privateKey = new Uint8Array(32).fill(1);
  const publicKey = await getEd25519PublicKey(privateKey);
  const op: Operation = {
    meta: { id: { replica: publicKey, counter: 1 }, lamport: 1 },
    kind: { type: 'tombstone', node: '00112233445566778899aabbccddeeff' },
  };
  const signature = await signTreecrdtOp({ docId: 'doc', op, privateKey });
  await expect(verifyTreecrdtOp({ docId: 'doc', op, signature, publicKey })).resolves.toBe(true);

  const deletion: Operation = {
    ...op,
    meta: { ...op.meta, knownState: new Uint8Array([0xff]) },
    kind: { ...op.kind, type: 'delete' },
  };
  await expect(
    verifyTreecrdtOp({ docId: 'doc', op: deletion, signature, publicKey }),
  ).resolves.toBe(false);
  await expect(
    encodeTreecrdtOpSigInput({
      docId: 'doc',
      op: { ...op, meta: deletion.meta },
    }),
  ).rejects.toThrow(/only allowed on delete/i);
  expect(loadCodec).not.toHaveBeenCalled();

  await expect(encodeTreecrdtOpSigInput({ docId: 'doc', op: deletion })).rejects.toBe(unavailable);
  expect(loadCodec).toHaveBeenCalledOnce();
});

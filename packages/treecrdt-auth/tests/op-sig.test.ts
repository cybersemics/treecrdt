import { expect, test } from 'vitest';

import type { Operation } from '@treecrdt/interface';
import { encodeTreecrdtOpSigInputV1, encodeTreecrdtOpSigInputV2 } from '../dist/treecrdt-auth.js';

const DOC_ID = 'doc-vector';
const CLAIMS = { authoredAtMs: 1_700_000_000_123 };

function hex(value: Uint8Array): string {
  return Buffer.from(value).toString('hex');
}

function deleteOp(knownState: Uint8Array | undefined): Operation {
  const replica = Uint8Array.from({ length: 32 }, (_, index) => index);
  return {
    meta: {
      id: { replica, counter: 9 },
      lamport: 17,
      ...(knownState === undefined ? {} : { knownState }),
    },
    kind: { type: 'delete', node: '00112233445566778899aabbccddeeff' },
  };
}

test('op signature inputs have stable v1 and v2 vectors', () => {
  const op = deleteOp(new TextEncoder().encode('{"a":1}'));

  expect(hex(encodeTreecrdtOpSigInputV1({ docId: DOC_ID, op }))).toBe(
    '74726565637264742f6f702d7369672f7631000000000a646f632d766563746f7200000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f000000000000000900000000000000110300112233445566778899aabbccddeeff',
  );
  expect(hex(encodeTreecrdtOpSigInputV2({ docId: DOC_ID, op, claims: CLAIMS }))).toBe(
    '74726565637264742f6f702d7369672f7632000000000a646f632d766563746f7200000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f000000000000000900000000000000110300112233445566778899aabbccddeeff01000000077b2261223a317d010000018bcfe5687b',
  );
});

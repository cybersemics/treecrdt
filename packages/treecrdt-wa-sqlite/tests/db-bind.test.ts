import vm from 'node:vm';
import { expect, test } from 'vitest';
import { makeDbAdapter } from '../src/db.js';

test('makeDbAdapter normalizes cross-realm typed array bind values to blobs', async () => {
  const foreignBytes = vm.runInNewContext('new Uint8Array([1, 2, 3])') as Uint8Array;
  let bound: unknown;

  const db = makeDbAdapter(
    {
      bind: (_stmt: number, _index: number, value: unknown) => {
        bound = value;
      },
      statements: () => ({
        next: async () => ({ value: 1 }),
        return: async () => undefined,
      }),
      step: async () => 101,
      column_text: async () => '',
      finalize: async () => undefined,
      exec: async () => undefined,
      close: async () => undefined,
    },
    1,
  );

  await db.bind(1, 1, foreignBytes);

  expect(foreignBytes).not.toBeInstanceOf(Uint8Array);
  expect(bound).toBeInstanceOf(Uint8Array);
  expect(Array.from(bound as Uint8Array)).toEqual([1, 2, 3]);
});

import vm from 'node:vm';
import { expect, test } from 'vitest';
import { CompiledQuery } from '@treecrdt/interface/sqlite';
import { createDatabase } from '../src/db.js';

test('createDatabase normalizes cross-realm typed array bind values to blobs', async () => {
  const foreignBytes = vm.runInNewContext('new Uint8Array([1, 2, 3])') as Uint8Array;
  let bound: unknown;

  const database = createDatabase(
    {
      bind: (_stmt: number, _index: number, value: unknown) => {
        bound = value;
      },
      statements: () => ({
        next: async () => ({ value: 1 }),
        return: async () => undefined,
      }),
      step: async () => 101,
      column_type: () => 5,
      column_count: () => 0,
      column_name: () => 'x',
      column_text: async () => '',
      finalize: async () => undefined,
      exec: async () => undefined,
      close: async () => undefined,
    },
    1,
  );

  await database.db.executeQuery(CompiledQuery.raw('select ?', [foreignBytes]));

  expect(foreignBytes).not.toBeInstanceOf(Uint8Array);
  expect(bound).toBeInstanceOf(Uint8Array);
  expect(Array.from(bound as Uint8Array)).toEqual([1, 2, 3]);
});

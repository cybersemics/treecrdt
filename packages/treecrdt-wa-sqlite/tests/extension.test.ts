import { expect, test, vi } from 'vitest';
import { createTreecrdtDb } from '@treecrdt/interface/sqlite';
import { initializeTreecrdtExtension } from '../src/extension.js';
import { createFakeModule } from './fake-sqlite.js';
import type { Database } from '../src/types.js';

const schema = 'CREATE TABLE example (id INTEGER PRIMARY KEY)';

function mockDatabase(opts: {
  schemaText?: string | null;
  onExec?: (sql: string) => Promise<void>;
} = {}): Database & { execBatch: ReturnType<typeof vi.fn> } {
  const schemaText = opts.schemaText === undefined ? schema : opts.schemaText;
  const execBatch = vi.fn(async (sql: string) => {
    await opts.onExec?.(sql);
  });
  return {
    db: createTreecrdtDb({
      query: async (sql) => {
        if (/treecrdt_schema/i.test(sql)) {
          return schemaText ? [{ value: schemaText }] : [{ value: null }];
        }
        return [{ value: null }];
      },
    }),
    execBatch,
  };
}

test('registers each handle and initializes its schema in a transaction', async () => {
  const module = createFakeModule();
  const db = mockDatabase();

  await initializeTreecrdtExtension(module, 11, db);
  await initializeTreecrdtExtension(module, 12, db);

  expect(module.init.mock.calls).toEqual([[11], [12]]);
  expect(db.execBatch.mock.calls.map(([sql]) => sql)).toEqual([
    'BEGIN IMMEDIATE',
    schema,
    'COMMIT',
    'BEGIN IMMEDIATE',
    schema,
    'COMMIT',
  ]);
});

test('awaits SQL completion before committing or returning', async () => {
  let complete!: () => void;
  const pending = new Promise<void>((resolve) => {
    complete = resolve;
  });
  let started!: () => void;
  const executing = new Promise<void>((resolve) => {
    started = resolve;
  });
  const db = mockDatabase({
    onExec: async (sql) => {
      if (sql === schema) {
        started();
        await pending;
      }
    },
  });
  let initialized = false;
  const initialization = initializeTreecrdtExtension(createFakeModule(), 21, db).then(() => {
    initialized = true;
  });
  await executing;
  expect(initialized).toBe(false);
  expect(db.execBatch).not.toHaveBeenCalledWith('COMMIT');
  complete();
  await initialization;
  expect(db.execBatch).toHaveBeenLastCalledWith('COMMIT');
});

test.each([schema, 'COMMIT'])(
  'rolls back if %s fails and preserves the error',
  async (failingSql) => {
    const failure = new Error('storage failed');
    const db = mockDatabase({
      onExec: async (sql) => {
        if (sql === failingSql) throw failure;
        if (sql === 'ROLLBACK') throw new Error('rollback also failed');
      },
    });
    await expect(initializeTreecrdtExtension(createFakeModule(), 22, db)).rejects.toBe(failure);
    expect(db.execBatch).toHaveBeenLastCalledWith('ROLLBACK');
  },
);

test('does not roll back a transaction it failed to begin', async () => {
  const db = mockDatabase({
    onExec: async () => {
      throw new Error('lock failed');
    },
  });
  await expect(initializeTreecrdtExtension(createFakeModule(), 23, db)).rejects.toThrow(
    'lock failed',
  );
  expect(db.execBatch.mock.calls).toEqual([['BEGIN IMMEDIATE']]);
});

test('does not execute SQL after registration fails', async () => {
  const db = mockDatabase();
  await expect(initializeTreecrdtExtension(createFakeModule(10), 31, db)).rejects.toThrow(
    'TreeCRDT SQLite extension init failed (rc=10)',
  );
  expect(db.execBatch).not.toHaveBeenCalled();
});

test('rejects missing schema before beginning a transaction', async () => {
  const db = mockDatabase({ schemaText: '' });
  await expect(initializeTreecrdtExtension(createFakeModule(), 32, db)).rejects.toThrow(
    'TreeCRDT extension did not provide its schema',
  );
  expect(db.execBatch).not.toHaveBeenCalled();
});

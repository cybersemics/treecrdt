import { expect, test, vi } from 'vitest';
import { initializeTreecrdtExtension } from '../src/extension.js';
import { createFakeModule } from './fake-sqlite.js';

const schema = 'CREATE TABLE example (id INTEGER PRIMARY KEY)';
const runner = () => ({
  getText: vi.fn(async () => schema),
  exec: vi.fn(async (_sql: string) => {}),
});

test('registers each handle and initializes its schema in a transaction', async () => {
  const module = createFakeModule();
  const db = runner();

  await initializeTreecrdtExtension(module, 11, db);
  await initializeTreecrdtExtension(module, 12, db);

  expect(module.init.mock.calls).toEqual([[11], [12]]);
  expect(db.getText).toHaveBeenCalledWith('SELECT treecrdt_schema()');
  expect(db.exec.mock.calls.map(([sql]) => sql)).toEqual([
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
  const db = runner();
  db.exec.mockImplementation(async (sql) => {
    if (sql === schema) {
      started();
      await pending;
    }
  });
  let initialized = false;
  const initialization = initializeTreecrdtExtension(createFakeModule(), 21, db).then(() => {
    initialized = true;
  });
  await executing;
  expect(initialized).toBe(false);
  expect(db.exec).not.toHaveBeenCalledWith('COMMIT');
  complete();
  await initialization;
  expect(db.exec).toHaveBeenLastCalledWith('COMMIT');
});

test.each([schema, 'COMMIT'])(
  'rolls back if %s fails and preserves the error',
  async (failingSql) => {
    const failure = new Error('storage failed');
    const db = runner();
    db.exec.mockImplementation(async (sql) => {
      if (sql === failingSql) throw failure;
      if (sql === 'ROLLBACK') throw new Error('rollback also failed');
    });
    await expect(initializeTreecrdtExtension(createFakeModule(), 22, db)).rejects.toBe(failure);
    expect(db.exec).toHaveBeenLastCalledWith('ROLLBACK');
  },
);

test('does not roll back a transaction it failed to begin', async () => {
  const db = runner();
  db.exec.mockRejectedValue(new Error('lock failed'));
  await expect(initializeTreecrdtExtension(createFakeModule(), 23, db)).rejects.toThrow(
    'lock failed',
  );
  expect(db.exec.mock.calls).toEqual([['BEGIN IMMEDIATE']]);
});

test('does not execute SQL after registration fails', async () => {
  const db = runner();
  await expect(initializeTreecrdtExtension(createFakeModule(10), 31, db)).rejects.toThrow(
    'TreeCRDT SQLite extension init failed (rc=10)',
  );
  expect(db.getText).not.toHaveBeenCalled();
  expect(db.exec).not.toHaveBeenCalled();
});

test('rejects missing schema before beginning a transaction', async () => {
  const db = runner();
  db.getText.mockResolvedValue('');
  await expect(initializeTreecrdtExtension(createFakeModule(), 32, db)).rejects.toThrow(
    'TreeCRDT extension did not provide its schema',
  );
  expect(db.exec).not.toHaveBeenCalled();
});

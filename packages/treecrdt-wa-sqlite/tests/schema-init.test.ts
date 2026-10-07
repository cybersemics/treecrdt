import { expect, test, vi } from 'vitest';
import { loadWaSqliteNode } from '../dist/node/load-wa-sqlite.js';
import { createDatabase } from '../src/db.js';
import { openTreecrdtDbWithLoader } from '../src/open-core.js';

test('registration provides schema SQL without creating tables', async () => {
  const { sqlite3, module } = await loadWaSqliteNode();
  const handle = await sqlite3.open_v2(':memory:');
  try {
    expect(module._treecrdt_sqlite_init(handle)).toBe(0);
    const db = createDatabase(sqlite3, handle);
    expect(await db.getText('SELECT treecrdt_schema()')).toBeTruthy();
    expect(await db.getText('SELECT count(*) FROM sqlite_schema')).toBe('0');
  } finally {
    await sqlite3.close(handle);
  }
});

test('failed schema initialization rolls back changes and closes the handle', async () => {
  const loaded = await loadWaSqliteNode();
  const { sqlite3 } = loaded;
  const open = sqlite3.open_v2.bind(sqlite3);
  const columnText = sqlite3.column_text.bind(sqlite3);
  const close = vi.spyOn(sqlite3, 'close');
  // Keep the database in the memory filesystem so rollback can be inspected after close.
  vi.spyOn(sqlite3, 'open_v2').mockImplementation(() => open('/failed-schema.db'));
  vi.spyOn(sqlite3, 'column_text').mockImplementation((stmt: number, column: number) => {
    const text = columnText(stmt, column);
    return typeof text === 'string' && text.includes('CREATE TABLE IF NOT EXISTS meta')
      ? text + '\nINSERT INTO missing_table VALUES (1);'
      : text;
  });
  await expect(
    openTreecrdtDbWithLoader({ storage: 'memory', docId: 'schema-failure' }, async () => loaded),
  ).rejects.toThrow(/missing_table/);
  expect(close).toHaveBeenCalledTimes(1);

  const handle = await open('/failed-schema.db');
  try {
    const db = createDatabase(sqlite3, handle);
    expect(await db.getText('SELECT count(*) FROM sqlite_schema')).toBe('0');
  } finally {
    await sqlite3.close(handle);
  }
});

test('a document ID mismatch closes the database without replacing its data', async () => {
  const loaded = await loadWaSqliteNode();
  const { sqlite3 } = loaded;
  const open = sqlite3.open_v2.bind(sqlite3);
  const close = vi.spyOn(sqlite3, 'close');
  vi.spyOn(sqlite3, 'open_v2').mockImplementation(() => open('/doc-id.db'));
  const openDocument = (docId: string) =>
    openTreecrdtDbWithLoader({ storage: 'memory', docId }, async () => loaded);
  const original = await openDocument('original');
  await original.db.close?.();
  close.mockClear();

  await expect(openDocument('different')).rejects.toThrow(/doc_id already set/);
  expect(close).toHaveBeenCalledTimes(1);

  const reopened = await openDocument('original');
  try {
    expect(await reopened.db.getText('SELECT treecrdt_doc_id()')).toBe('original');
  } finally {
    await reopened.db.close?.();
  }
});

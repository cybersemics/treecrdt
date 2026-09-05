import { beforeEach, expect, test, vi } from 'vitest';

vi.mock('../src/opfs.js', () => ({ createOpfsVfs: vi.fn() }));

import { createOpfsVfs } from '../src/opfs.js';
import { openTreecrdtDbWithLoader } from '../src/open-core.js';
import { createFakeModule, createFakeSqlite } from './fake-sqlite.js';

beforeEach(() => {
  vi.mocked(createOpfsVfs).mockReset();
});

test('initializes the extension after opening a memory database', async () => {
  const sqlite3 = createFakeSqlite();
  const module = createFakeModule();
  const load = vi.fn(async () => ({ sqlite3, module }));

  const opened = await openTreecrdtDbWithLoader(
    { storage: 'memory', docId: 'memory-explicit-init' },
    load,
  );

  expect(sqlite3.open_v2).toHaveBeenCalledWith(':memory:');
  expect(sqlite3.vfs_register).not.toHaveBeenCalled();
  expect(createOpfsVfs).not.toHaveBeenCalled();
  expect(load).toHaveBeenCalledOnce();
  expect(module.init).toHaveBeenCalledWith(1);
  expect(module.init.mock.invocationCallOrder[0]).toBeLessThan(
    sqlite3.statements.mock.invocationCallOrder[0]!,
  );
  await opened.db.close?.();
  expect(sqlite3.close).toHaveBeenCalledWith(1);
});

test('closes the database when explicit extension initialization fails', async () => {
  const sqlite3 = createFakeSqlite();
  const module = createFakeModule(10);

  await expect(
    openTreecrdtDbWithLoader(
      { storage: 'memory', docId: 'memory-explicit-init-failure' },
      async () => ({ sqlite3, module }),
    ),
  ).rejects.toThrow('TreeCRDT SQLite extension init failed (rc=10)');

  expect(sqlite3.close).toHaveBeenCalledWith(1);
});

test('uses the named OPFS VFS and closes partial resources when required initialization fails', async () => {
  const sqlite3 = createFakeSqlite();
  const module = createFakeModule(10);
  const vfs = { close: vi.fn() };
  const load = vi.fn(async () => ({ sqlite3, module }));
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);

  await expect(
    openTreecrdtDbWithLoader(
      {
        storage: 'opfs',
        filename: '/explicit-init-failure.db',
        docId: 'opfs-explicit-init-failure',
        requireOpfs: true,
      },
      load,
    ),
  ).rejects.toThrow('TreeCRDT SQLite extension init failed (rc=10)');

  expect(sqlite3.vfs_register).toHaveBeenCalledWith(vfs, false);
  expect(sqlite3.open_v2).toHaveBeenCalledWith('/explicit-init-failure.db', undefined, 'opfs');
  expect(sqlite3.close).toHaveBeenCalledWith(1);
  expect(vfs.close).toHaveBeenCalledOnce();
  expect(load).toHaveBeenCalledOnce();
});

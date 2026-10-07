import { beforeEach, expect, test, vi } from 'vitest';

vi.mock('../src/opfs.js', () => ({ createOpfsVfs: vi.fn() }));

import { createOpfsVfs } from '../src/opfs.js';
import { openTreecrdtDbWithLoader } from '../src/open-core.js';
import { createFakeModule, createFakeSqlite } from './fake-sqlite.js';

beforeEach(() => {
  vi.mocked(createOpfsVfs).mockReset();
});

test('rejects without a memory retry when opening the OPFS database fails', async () => {
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const sqlite3 = createFakeSqlite({ failOpen: '/open-failure.db' });
  const load = vi.fn(async () => ({ sqlite3, module: createFakeModule() }));

  const result = openTreecrdtDbWithLoader(
    { storage: 'opfs', filename: '/open-failure.db', docId: 'open-failure' },
    load,
  );

  await expect(result).rejects.toThrow(
    'OPFS requested but could not be initialized: OPFS open failed',
  );
  expect(sqlite3.open_v2).toHaveBeenCalledOnce();
  expect(sqlite3.open_v2).toHaveBeenCalledWith('/open-failure.db', undefined, 'opfs');
  expect(sqlite3.vfs_register).toHaveBeenCalledWith(vfs, false);
  expect(vfs.close).toHaveBeenCalledOnce();
  expect(load).toHaveBeenCalledOnce();
});

test('rejects without a memory retry when initializing the OPFS VFS fails', async () => {
  const vfsFailure = new Error('OPFS VFS unavailable');
  vi.mocked(createOpfsVfs).mockRejectedValue(vfsFailure);
  const sqlite3 = createFakeSqlite();
  const load = vi.fn(async () => ({ sqlite3, module: createFakeModule() }));

  const result = openTreecrdtDbWithLoader(
    { storage: 'opfs', filename: '/vfs-failure.db', docId: 'vfs-failure' },
    load,
  );

  await expect(result).rejects.toThrow(
    'OPFS requested but could not be initialized: OPFS VFS unavailable',
  );
  expect(sqlite3.open_v2).not.toHaveBeenCalled();
  expect(load).toHaveBeenCalledOnce();
});

test('closes the database and VFS when extension initialization fails', async () => {
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const sqlite3 = createFakeSqlite();
  const load = vi.fn(async () => ({ sqlite3, module: createFakeModule(10) }));

  const result = openTreecrdtDbWithLoader(
    { storage: 'opfs', filename: '/extension-failure.db', docId: 'extension-failure' },
    load,
  );

  await expect(result).rejects.toThrow('TreeCRDT SQLite extension init failed (rc=10)');
  expect(sqlite3.close).toHaveBeenCalledWith(1);
  expect(vfs.close).toHaveBeenCalledOnce();
});

test('closes the database and VFS when configuring the TreeCRDT adapter fails', async () => {
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const sqlite3 = createFakeSqlite({ failDocId: 'adapter-failure' });
  const load = vi.fn(async () => ({ sqlite3, module: createFakeModule() }));

  const result = openTreecrdtDbWithLoader(
    { storage: 'opfs', filename: '/adapter-failure.db', docId: 'adapter-failure' },
    load,
  );

  await expect(result).rejects.toThrow('setDocId failed');
  expect(sqlite3.close).toHaveBeenCalledWith(1);
  expect(vfs.close).toHaveBeenCalledOnce();
});

test('keeps the successful OPFS path single-pass and closes its database and VFS once', async () => {
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const sqlite3 = createFakeSqlite();
  const load = vi.fn(async () => ({ sqlite3, module: createFakeModule() }));

  const opened = await openTreecrdtDbWithLoader(
    { storage: 'opfs', filename: '/success.db', docId: 'success' },
    load,
  );

  expect(opened.storage).toBe('opfs');
  expect(sqlite3.open_v2).toHaveBeenCalledOnce();
  expect(sqlite3.open_v2).toHaveBeenCalledWith('/success.db', undefined, 'opfs');
  expect(sqlite3.vfs_register).toHaveBeenCalledWith(vfs, false);
  expect(load).toHaveBeenCalledOnce();

  await opened.db.close?.();
  await opened.db.close?.();
  expect(sqlite3.close).toHaveBeenCalledOnce();
  expect(vfs.close).toHaveBeenCalledOnce();
});

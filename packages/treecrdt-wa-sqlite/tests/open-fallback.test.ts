import { beforeEach, expect, test, vi } from 'vitest';

vi.mock('../src/opfs.js', () => ({ createOpfsVfs: vi.fn() }));

import { createOpfsVfs } from '../src/opfs.js';
import { openTreecrdtDbWithLoader } from '../src/open-core.js';
import { createFakeModule, createFakeSqlite } from './fakeSqlite.js';

beforeEach(() => {
  vi.mocked(createOpfsVfs).mockReset();
});

test('falls back to memory when opening the OPFS database fails', async () => {
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const sqlite3 = createFakeSqlite({ failOpen: '/fallback.db' });
  const memorySqlite3 = createFakeSqlite();
  const load = vi
    .fn()
    .mockResolvedValueOnce({ sqlite3, module: createFakeModule() })
    .mockResolvedValueOnce({ sqlite3: memorySqlite3, module: createFakeModule() });

  const opened = await openTreecrdtDbWithLoader(
    {
      storage: 'opfs',
      filename: '/fallback.db',
      docId: 'fallback-open',
      requireOpfs: false,
    },
    load,
  );

  expect(opened.storage).toBe('memory');
  expect(opened.filename).toBe(':memory:');
  expect(opened.opfsError).toBe('OPFS open failed');
  expect(sqlite3.open_v2).toHaveBeenCalledOnce();
  expect(sqlite3.open_v2).toHaveBeenCalledWith('/fallback.db', undefined, 'opfs');
  expect(memorySqlite3.open_v2).toHaveBeenCalledOnce();
  expect(memorySqlite3.open_v2).toHaveBeenCalledWith(':memory:');
  expect(sqlite3.vfs_register).toHaveBeenCalledWith(vfs, false);
  expect(vfs.close).toHaveBeenCalledOnce();
  expect(load).toHaveBeenCalledTimes(2);
});

test('uses a separate loader for memory fallback', async () => {
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const opfsSqlite3 = createFakeSqlite({ failOpen: '/separate-loader.db' });
  const memorySqlite3 = createFakeSqlite();
  const load = vi.fn(async () => ({ sqlite3: opfsSqlite3, module: createFakeModule() }));
  const loadMemoryFallback = vi.fn(async () => ({
    sqlite3: memorySqlite3,
    module: createFakeModule(),
  }));

  const opened = await openTreecrdtDbWithLoader(
    {
      storage: 'opfs',
      filename: '/separate-loader.db',
      docId: 'separate-loader',
      requireOpfs: false,
    },
    load,
    loadMemoryFallback,
  );

  expect(opened.storage).toBe('memory');
  expect(load).toHaveBeenCalledOnce();
  expect(loadMemoryFallback).toHaveBeenCalledOnce();
  expect(memorySqlite3.open_v2).toHaveBeenCalledWith(':memory:');
});

test('falls back to memory when initializing the OPFS VFS fails', async () => {
  const vfsFailure = new Error('OPFS VFS unavailable');
  vi.mocked(createOpfsVfs).mockRejectedValue(vfsFailure);
  const sqlite3 = createFakeSqlite();
  const memorySqlite3 = createFakeSqlite();
  const load = vi
    .fn()
    .mockResolvedValueOnce({ sqlite3, module: createFakeModule() })
    .mockResolvedValueOnce({ sqlite3: memorySqlite3, module: createFakeModule() });

  const opened = await openTreecrdtDbWithLoader(
    {
      storage: 'opfs',
      filename: '/fallback-vfs.db',
      docId: 'fallback-vfs',
      requireOpfs: false,
    },
    load,
  );

  expect(opened.storage).toBe('memory');
  expect(opened.opfsError).toBe(vfsFailure.message);
  expect(sqlite3.open_v2).not.toHaveBeenCalled();
  expect(memorySqlite3.open_v2).toHaveBeenCalledWith(':memory:');
  expect(load).toHaveBeenCalledTimes(2);
});

test('does not fall back after explicit extension initialization fails', async () => {
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const sqlite3 = createFakeSqlite();
  const memorySqlite3 = createFakeSqlite();
  const load = vi
    .fn()
    .mockResolvedValueOnce({ sqlite3, module: createFakeModule(10) })
    .mockResolvedValueOnce({ sqlite3: memorySqlite3, module: createFakeModule() });

  const result = openTreecrdtDbWithLoader(
    {
      storage: 'opfs',
      filename: '/fallback-extension.db',
      docId: 'fallback-extension',
      requireOpfs: false,
    },
    load,
  );

  await expect(result).rejects.toThrow('TreeCRDT SQLite extension init failed (rc=10)');
  expect(sqlite3.close).toHaveBeenCalledWith(1);
  expect(vfs.close).toHaveBeenCalledOnce();
  expect(memorySqlite3.open_v2).not.toHaveBeenCalled();
  expect(load).toHaveBeenCalledOnce();
});

test('does not fall back after configuring the TreeCRDT adapter fails', async () => {
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const sqlite3 = createFakeSqlite({ failDocId: 'adapter-failure' });
  const memorySqlite3 = createFakeSqlite();
  const load = vi
    .fn()
    .mockResolvedValueOnce({ sqlite3, module: createFakeModule() })
    .mockResolvedValueOnce({ sqlite3: memorySqlite3, module: createFakeModule() });

  const result = openTreecrdtDbWithLoader(
    {
      storage: 'opfs',
      filename: '/adapter-failure.db',
      docId: 'adapter-failure',
      requireOpfs: false,
    },
    load,
  );

  await expect(result).rejects.toThrow('setDocId failed');
  expect(sqlite3.close).toHaveBeenCalledWith(1);
  expect(vfs.close).toHaveBeenCalledOnce();
  expect(memorySqlite3.open_v2).not.toHaveBeenCalled();
  expect(load).toHaveBeenCalledOnce();
});

test('reports both errors when the fresh memory fallback also fails', async () => {
  const opfsFailure = new Error('OPFS open failed');
  const fallbackFailure = new Error('memory module load failed');
  const sqlite3 = createFakeSqlite({ failOpen: '/both-fail.db', failOpenError: opfsFailure });
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);

  const result = openTreecrdtDbWithLoader(
    {
      storage: 'opfs',
      filename: '/both-fail.db',
      docId: 'both-fail',
      requireOpfs: false,
    },
    vi
      .fn()
      .mockResolvedValueOnce({ sqlite3, module: createFakeModule() })
      .mockRejectedValueOnce(fallbackFailure),
  );

  await expect(result).rejects.toThrow(
    'OPFS initialization failed: OPFS open failed; memory fallback failed: memory module load failed',
  );
  expect(vfs.close).toHaveBeenCalledOnce();
});

test('keeps the successful OPFS path single-pass and closes its database and VFS once', async () => {
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const sqlite3 = createFakeSqlite();
  const load = vi.fn(async () => ({ sqlite3, module: createFakeModule() }));

  const opened = await openTreecrdtDbWithLoader(
    {
      storage: 'opfs',
      filename: '/success.db',
      docId: 'success',
      requireOpfs: true,
    },
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

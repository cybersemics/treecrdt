import { beforeEach, expect, test, vi } from 'vitest';

vi.mock('../src/opfs.js', () => ({ createOpfsVfs: vi.fn() }));

import { createOpfsVfs } from '../src/opfs.js';
import { openTreecrdtDbFromLoaded } from '../src/open-core.js';

function createFakeModule(initResult = 0) {
  const init = vi.fn(async () => initResult);
  return {
    cwrap: vi.fn(() => init),
    init,
    retryOps: [] as Promise<unknown>[],
    pendingOps: [] as Promise<unknown>[],
  };
}

function createFakeSqlite() {
  let nextStatement = 100;
  return {
    vfs_register: vi.fn(),
    open_v2: vi.fn(async () => 1),
    statements: vi.fn(() => {
      const statement = nextStatement++;
      return {
        next: async () => ({ value: statement }),
        return: async () => undefined,
      };
    }),
    bind: vi.fn(),
    step: vi.fn(async () => 101),
    column_text: vi.fn(),
    finalize: vi.fn(),
    exec: vi.fn(),
    close: vi.fn(),
  };
}

beforeEach(() => {
  vi.mocked(createOpfsVfs).mockReset();
});

test('initializes the extension after opening a memory database', async () => {
  const sqlite3 = createFakeSqlite();
  const module = createFakeModule();

  const opened = await openTreecrdtDbFromLoaded(
    { storage: 'memory', docId: 'memory-explicit-init' },
    { sqlite3, module },
  );

  expect(sqlite3.open_v2).toHaveBeenCalledWith(':memory:');
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
    openTreecrdtDbFromLoaded(
      { storage: 'memory', docId: 'memory-explicit-init-failure' },
      { sqlite3, module },
    ),
  ).rejects.toThrow('TreeCRDT SQLite extension init failed (rc=10)');

  expect(sqlite3.close).toHaveBeenCalledWith(1);
});

test('uses the named OPFS VFS and closes partial resources when initialization fails', async () => {
  const sqlite3 = createFakeSqlite();
  const module = createFakeModule(10);
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);

  await expect(
    openTreecrdtDbFromLoaded(
      {
        storage: 'opfs',
        filename: '/explicit-init-failure.db',
        docId: 'opfs-explicit-init-failure',
      },
      { sqlite3, module },
    ),
  ).rejects.toThrow('TreeCRDT SQLite extension init failed (rc=10)');

  expect(sqlite3.vfs_register).toHaveBeenCalledWith(vfs, false);
  expect(sqlite3.open_v2).toHaveBeenCalledWith('/explicit-init-failure.db', undefined, 'opfs');
  expect(sqlite3.close).toHaveBeenCalledWith(1);
  expect(vfs.close).toHaveBeenCalledOnce();
});

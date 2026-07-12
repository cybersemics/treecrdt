import { beforeEach, expect, test, vi } from 'vitest';

vi.mock('../src/opfs.js', () => ({ createOpfsVfs: vi.fn() }));

import { createOpfsVfs } from '../src/opfs.js';
import { openTreecrdtDbFromLoaded } from '../src/open-core.js';

function createFakeModule(initResult = 0) {
  return {
    cwrap: vi.fn(() => vi.fn(async () => initResult)),
    retryOps: [] as Promise<unknown>[],
  };
}

function createFakeSqlite(
  opts: { failOpen?: string; failOpenError?: Error; failInitializationHandle?: number } = {},
) {
  const statementHandles = new Map<number, number>();
  let nextHandle = 1;
  let nextStatement = 100;

  return {
    vfs_register: vi.fn(),
    open_v2: vi.fn(async (filename: string, _flags?: number, _vfs?: string) => {
      if (filename === opts.failOpen) throw opts.failOpenError ?? new Error('OPFS open failed');
      return nextHandle++;
    }),
    statements: vi.fn((handle: number) => {
      const statement = nextStatement++;
      statementHandles.set(statement, handle);
      return {
        next: async () => ({ value: statement }),
        return: async () => undefined,
      };
    }),
    bind: vi.fn(),
    step: vi.fn(async (statement: number) => {
      if (statementHandles.get(statement) === opts.failInitializationHandle) {
        throw new Error('TreeCRDT initialization failed');
      }
      return 101;
    }),
    column_text: vi.fn(),
    finalize: vi.fn(),
    exec: vi.fn(),
    close: vi.fn(),
  };
}

beforeEach(() => {
  vi.mocked(createOpfsVfs).mockReset();
});

test('keeps the memory path single-pass without creating an OPFS VFS', async () => {
  const sqlite3 = createFakeSqlite();
  const loadFresh = vi.fn();

  const opened = await openTreecrdtDbFromLoaded(
    { storage: 'memory', docId: 'memory-fast-path' },
    { sqlite3, module: createFakeModule() },
    loadFresh,
  );

  expect(opened.storage).toBe('memory');
  expect(sqlite3.open_v2).toHaveBeenCalledOnce();
  expect(sqlite3.open_v2).toHaveBeenCalledWith(':memory:');
  expect(sqlite3.vfs_register).not.toHaveBeenCalled();
  expect(createOpfsVfs).not.toHaveBeenCalled();
  expect(loadFresh).not.toHaveBeenCalled();
});

test('falls back to memory when opening the OPFS database fails', async () => {
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const sqlite3 = createFakeSqlite({ failOpen: '/fallback.db' });
  const memorySqlite3 = createFakeSqlite();
  const loadFresh = vi.fn(async () => ({ sqlite3: memorySqlite3, module: createFakeModule() }));

  const opened = await openTreecrdtDbFromLoaded(
    {
      storage: 'opfs',
      filename: '/fallback.db',
      docId: 'fallback-open',
      requireOpfs: false,
    },
    { sqlite3, module: createFakeModule() },
    loadFresh,
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
  expect(loadFresh).toHaveBeenCalledOnce();
});

test('closes a partially initialized OPFS database before falling back', async () => {
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const sqlite3 = createFakeSqlite({ failInitializationHandle: 1 });
  const memorySqlite3 = createFakeSqlite();
  const loadFresh = vi.fn(async () => ({ sqlite3: memorySqlite3, module: createFakeModule() }));

  const opened = await openTreecrdtDbFromLoaded(
    {
      storage: 'opfs',
      filename: '/fallback-init.db',
      docId: 'fallback-init',
      requireOpfs: false,
    },
    { sqlite3, module: createFakeModule() },
    loadFresh,
  );

  expect(opened.storage).toBe('memory');
  expect(opened.opfsError).toBe('TreeCRDT initialization failed');
  expect(sqlite3.close).toHaveBeenCalledWith(1);
  expect(memorySqlite3.open_v2).toHaveBeenCalledWith(':memory:');
  expect(vfs.close).toHaveBeenCalledOnce();
  expect(loadFresh).toHaveBeenCalledOnce();
});

test('closes the OPFS database when explicit extension initialization fails', async () => {
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const sqlite3 = createFakeSqlite();
  const memorySqlite3 = createFakeSqlite();
  const loadFresh = vi.fn(async () => ({
    sqlite3: memorySqlite3,
    module: createFakeModule(),
  }));

  const opened = await openTreecrdtDbFromLoaded(
    {
      storage: 'opfs',
      filename: '/fallback-extension.db',
      docId: 'fallback-extension',
      requireOpfs: false,
    },
    { sqlite3, module: createFakeModule(10) },
    loadFresh,
  );

  expect(opened.storage).toBe('memory');
  expect(opened.opfsError).toBe('TreeCRDT SQLite extension init failed (rc=10)');
  expect(sqlite3.close).toHaveBeenCalledWith(1);
  expect(vfs.close).toHaveBeenCalledOnce();
  expect(memorySqlite3.open_v2).toHaveBeenCalledWith(':memory:');
});

test('preserves both errors when the fresh memory fallback also fails', async () => {
  const opfsFailure = new Error('OPFS open failed');
  const fallbackFailure = new Error('memory module load failed');
  const sqlite3 = createFakeSqlite({ failOpen: '/both-fail.db', failOpenError: opfsFailure });
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);

  const result = openTreecrdtDbFromLoaded(
    {
      storage: 'opfs',
      filename: '/both-fail.db',
      docId: 'both-fail',
      requireOpfs: false,
    },
    { sqlite3, module: createFakeModule() },
    vi.fn().mockRejectedValue(fallbackFailure),
  );

  await expect(result).rejects.toMatchObject({
    message:
      'OPFS initialization failed: OPFS open failed; memory fallback failed: memory module load failed',
    cause: fallbackFailure,
    opfsCause: opfsFailure,
  });
  expect(vfs.close).toHaveBeenCalledOnce();
});

test('keeps the successful OPFS path single-pass and closes its database and VFS once', async () => {
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const sqlite3 = createFakeSqlite();
  const loadFresh = vi.fn();

  const opened = await openTreecrdtDbFromLoaded(
    {
      storage: 'opfs',
      filename: '/success.db',
      docId: 'success',
      requireOpfs: true,
    },
    { sqlite3, module: createFakeModule() },
    loadFresh,
  );

  expect(opened.storage).toBe('opfs');
  expect(sqlite3.open_v2).toHaveBeenCalledOnce();
  expect(sqlite3.open_v2).toHaveBeenCalledWith('/success.db', undefined, 'opfs');
  expect(sqlite3.vfs_register).toHaveBeenCalledWith(vfs, false);
  expect(loadFresh).not.toHaveBeenCalled();

  await opened.db.close?.();
  await opened.db.close?.();
  expect(sqlite3.close).toHaveBeenCalledOnce();
  expect(vfs.close).toHaveBeenCalledOnce();
});

test('required OPFS closes the VFS and does not attempt memory fallback', async () => {
  const vfs = {
    close: vi.fn(() => {
      throw new Error('VFS close failed');
    }),
  };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const opfsFailure = new Error('OPFS open failed');
  const sqlite3 = createFakeSqlite({ failOpen: '/required.db', failOpenError: opfsFailure });
  const loadFresh = vi.fn();

  await expect(
    openTreecrdtDbFromLoaded(
      {
        storage: 'opfs',
        filename: '/required.db',
        docId: 'required',
        requireOpfs: true,
      },
      { sqlite3, module: createFakeModule() },
      loadFresh,
    ),
  ).rejects.toMatchObject({
    message: 'OPFS requested but could not be initialized: OPFS open failed',
    cause: opfsFailure,
  });

  expect(sqlite3.open_v2).toHaveBeenCalledOnce();
  expect(vfs.close).toHaveBeenCalledOnce();
  expect(loadFresh).not.toHaveBeenCalled();
});

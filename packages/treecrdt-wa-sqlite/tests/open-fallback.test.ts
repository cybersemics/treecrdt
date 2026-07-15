import { beforeEach, expect, test, vi } from 'vitest';

vi.mock('../src/opfs.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/opfs.js')>();
  return {
    ...actual,
    accessHandlePoolVfsNameForFilename: vi.fn(actual.accessHandlePoolVfsNameForFilename),
    createOpfsVfs: vi.fn(),
  };
});

import { accessHandlePoolVfsNameForFilename, createOpfsVfs } from '../src/opfs.js';
import { openTreecrdtDbFromLoaded } from '../src/open-core.js';

function createFakeModule(initResult = 0, events?: string[]) {
  return {
    cwrap: vi.fn(() =>
      vi.fn(async () => {
        events?.push('extension-init');
        return initResult;
      }),
    ),
    retryOps: [] as Promise<unknown>[],
    pendingOps: [] as Promise<unknown>[],
  };
}

function createFakeSqlite(
  opts: {
    failOpen?: string;
    failOpenError?: Error;
    failInitializationHandle?: number;
    queryText?: Record<string, string | null | Array<string | null>>;
    events?: string[];
  } = {},
) {
  const statementHandles = new Map<number, number>();
  const statementText = new Map<number, string | null>();
  let nextHandle = 1;
  let nextStatement = 100;

  return {
    vfs_register: vi.fn(),
    open_v2: vi.fn(async (filename: string, _flags?: number, _vfs?: string) => {
      if (filename === opts.failOpen) throw opts.failOpenError ?? new Error('OPFS open failed');
      return nextHandle++;
    }),
    statements: vi.fn((handle: number, sql: string) => {
      const statement = nextStatement++;
      statementHandles.set(statement, handle);
      if (opts.queryText && Object.prototype.hasOwnProperty.call(opts.queryText, sql)) {
        const configured = opts.queryText?.[sql];
        const value = Array.isArray(configured) ? (configured.shift() ?? null) : configured;
        statementText.set(statement, value ?? null);
      }
      opts.events?.push(`prepare:${sql}`);
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
      return statementText.has(statement) ? 100 : 101;
    }),
    column_text: vi.fn((statement: number) => statementText.get(statement)),
    finalize: vi.fn(),
    exec: vi.fn(),
    close: vi.fn(),
  };
}

beforeEach(() => {
  vi.mocked(accessHandlePoolVfsNameForFilename).mockClear();
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

test('falls back before VFS creation when WAL storage naming fails', async () => {
  vi.mocked(accessHandlePoolVfsNameForFilename).mockRejectedValueOnce(
    new Error('storage naming failed'),
  );
  const sqlite3 = createFakeSqlite();
  const memorySqlite3 = createFakeSqlite();
  const loadFresh = vi.fn(async () => ({
    sqlite3: memorySqlite3,
    module: createFakeModule(),
  }));

  const opened = await openTreecrdtDbFromLoaded(
    {
      storage: 'opfs',
      filename: '/fallback-name.db',
      docId: 'fallback-name',
      requireOpfs: false,
      opfsWriteMode: 'single-owner-wal',
    },
    { sqlite3, module: createFakeModule() },
    loadFresh,
  );

  expect(opened.storage).toBe('memory');
  expect(opened.opfsError).toBe('storage naming failed');
  expect(createOpfsVfs).not.toHaveBeenCalled();
  expect(sqlite3.open_v2).not.toHaveBeenCalled();
  expect(memorySqlite3.open_v2).toHaveBeenCalledWith(':memory:');
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

test('falls back after explicit extension initialization fails', async () => {
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
  expect(createOpfsVfs).toHaveBeenCalledWith(expect.anything(), {
    name: 'opfs',
    kind: 'coop-sync',
  });
  expect(sqlite3.statements.mock.calls.some(([, sql]) => String(sql).startsWith('PRAGMA'))).toBe(
    false,
  );
  expect(loadFresh).not.toHaveBeenCalled();

  await opened.db.close?.();
  await opened.db.close?.();
  expect(sqlite3.close).toHaveBeenCalledOnce();
  expect(vfs.close).toHaveBeenCalledOnce();
});

test('configures WAL before extension initialization and selects the VFS explicitly', async () => {
  const events: string[] = [];
  const vfs = { close: vi.fn() };
  vi.mocked(createOpfsVfs).mockResolvedValue(vfs);
  const sqlite3 = createFakeSqlite({
    events,
    queryText: {
      'PRAGMA locking_mode=EXCLUSIVE': 'exclusive',
      'PRAGMA journal_mode': ['delete', 'wal'],
      'PRAGMA journal_mode=WAL': 'wal',
      'PRAGMA locking_mode': 'exclusive',
    },
  });
  const filename = '/wal.db';

  const opened = await openTreecrdtDbFromLoaded(
    {
      storage: 'opfs',
      filename,
      docId: 'wal',
      requireOpfs: true,
      opfsWriteMode: 'single-owner-wal',
    },
    { sqlite3, module: createFakeModule(0, events) },
    vi.fn(),
  );

  expect(opened.opfsVfsKind).toBe('access-handle-pool');
  expect(opened.opfsVfsName).toMatch(/^opfs-ahp-[0-9a-f]{64}$/);
  expect(createOpfsVfs).toHaveBeenCalledWith(expect.anything(), {
    name: opened.opfsVfsName,
    kind: 'access-handle-pool',
  });
  expect(sqlite3.vfs_register).toHaveBeenCalledWith(vfs, false);
  expect(sqlite3.open_v2).toHaveBeenCalledWith(filename, undefined, opened.opfsVfsName);
  expect(events.indexOf('prepare:PRAGMA locking_mode=EXCLUSIVE')).toBeLessThan(
    events.indexOf('extension-init'),
  );
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

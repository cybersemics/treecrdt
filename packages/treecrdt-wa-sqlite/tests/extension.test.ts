import { expect, test, vi } from 'vitest';
import { initializeTreecrdtExtension } from '../src/extension.js';

test('initializes each database handle and caches the module wrapper', async () => {
  const init = vi.fn(async () => 0);
  const module = {
    cwrap: vi.fn(() => init),
    retryOps: [] as Promise<unknown>[],
  };

  await initializeTreecrdtExtension(module, 11);
  await initializeTreecrdtExtension(module, 12);

  expect(module.cwrap).toHaveBeenCalledOnce();
  expect(module.cwrap).toHaveBeenCalledWith('treecrdt_sqlite_init', 'number', ['number'], {
    async: true,
  });
  expect(init).toHaveBeenNthCalledWith(1, 11);
  expect(init).toHaveBeenNthCalledWith(2, 12);
});

test('waits and retries when an async VFS operation requests it', async () => {
  let attempt = 0;
  const module = {
    cwrap: vi.fn(() =>
      vi.fn(async () => {
        attempt += 1;
        if (attempt === 1) {
          module.retryOps.push(Promise.resolve());
          return 5;
        }
        return 0;
      }),
    ),
    retryOps: [] as Promise<unknown>[],
  };

  await initializeTreecrdtExtension(module, 21);
  expect(attempt).toBe(2);
  expect(module.retryOps).toEqual([]);
});

test('clears a rejected retry operation without calling the initializer', async () => {
  const retryFailure = new Error('retry failed');
  const init = vi.fn(async () => 0);
  const module = {
    cwrap: vi.fn(() => init),
    retryOps: [Promise.reject(retryFailure)],
  };

  await expect(initializeTreecrdtExtension(module, 22)).rejects.toBe(retryFailure);
  expect(module.retryOps).toEqual([]);
  expect(init).not.toHaveBeenCalled();
});

test('continues through multiple queued retry phases before succeeding', async () => {
  const phases: string[] = [];
  let attempt = 0;
  const init = vi.fn(async () => {
    attempt += 1;
    phases.push(`init-${attempt}`);
    if (attempt <= 3) {
      const phase = attempt;
      module.retryOps.push(
        Promise.resolve().then(() => {
          phases.push(`retry-${phase}`);
        }),
      );
      return 5;
    }
    return 0;
  });
  const module = {
    cwrap: vi.fn(() => init),
    retryOps: [] as Promise<unknown>[],
  };

  await initializeTreecrdtExtension(module, 23);

  expect(init).toHaveBeenCalledTimes(4);
  expect(phases).toEqual(['init-1', 'retry-1', 'init-2', 'retry-2', 'init-3', 'retry-3', 'init-4']);
  expect(module.retryOps).toEqual([]);
});

test('waits for pending VFS work before reporting successful initialization', async () => {
  let releasePending!: () => void;
  const pending = new Promise<void>((resolve) => {
    releasePending = resolve;
  });
  const module = {
    cwrap: vi.fn(() =>
      vi.fn(async () => {
        module.pendingOps.push(pending);
        return 0;
      }),
    ),
    retryOps: [] as Promise<unknown>[],
    pendingOps: [] as Promise<unknown>[],
  };

  let initialized = false;
  const initialization = initializeTreecrdtExtension(module, 24).then(() => {
    initialized = true;
  });
  await Promise.resolve();
  expect(initialized).toBe(false);

  releasePending();
  await initialization;
  expect(module.pendingOps).toEqual([]);
});

test.each([
  { failure: Object.assign(new Error('checkpoint failed'), { code: 10 }), expectedCode: 10 },
  { failure: new Error('checkpoint failed'), expectedCode: 1 },
])(
  'maps a rejected pending operation to SQLite code $expectedCode',
  async ({ failure, expectedCode }) => {
    const module = {
      cwrap: vi.fn(() =>
        vi.fn(async () => {
          module.pendingOps.push(Promise.reject(failure));
          return 0;
        }),
      ),
      retryOps: [] as Promise<unknown>[],
      pendingOps: [] as Promise<unknown>[],
    };

    await expect(initializeTreecrdtExtension(module, 25)).rejects.toThrow(
      `TreeCRDT SQLite extension init failed (rc=${expectedCode})`,
    );
    expect(module.pendingOps).toEqual([]);
  },
);

test('fails clearly when initialization returns an SQLite error code', async () => {
  const module = {
    cwrap: vi.fn(() => vi.fn(async () => 10)),
    retryOps: [] as Promise<unknown>[],
  };

  await expect(initializeTreecrdtExtension(module, 31)).rejects.toThrow(
    'TreeCRDT SQLite extension init failed (rc=10)',
  );
});

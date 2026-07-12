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

test('fails clearly when initialization returns an SQLite error code', async () => {
  const module = {
    cwrap: vi.fn(() => vi.fn(async () => 10)),
    retryOps: [] as Promise<unknown>[],
  };

  await expect(initializeTreecrdtExtension(module, 31)).rejects.toThrow(
    'TreeCRDT SQLite extension init failed (rc=10)',
  );
});

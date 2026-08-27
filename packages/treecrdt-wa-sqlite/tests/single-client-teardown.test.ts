import { afterEach, beforeEach, expect, test, vi } from 'vitest';

import { createTreecrdtClient } from '../src/client.browser.js';
import { buildDirectClient, CLIENT_CLOSED_ERROR, type OpenDbFn } from '../src/client.js';
import { clearOpfsStorage } from '../src/opfs.js';
import type { Database, TreecrdtClient } from '../src/types.js';
import { createMockConnection, installDedicatedWorker } from './mockWorker.js';

vi.mock('../src/opfs.js', async (importOriginal) => {
  const original = await importOriginal<typeof import('../src/opfs.js')>();
  return { ...original, clearOpfsStorage: vi.fn() };
});

const mockedClearOpfsStorage = vi.mocked(clearOpfsStorage);

async function createDirectClientHarness(opts: { storage: 'memory' | 'opfs'; closeError?: Error }) {
  const closeDatabase = vi.fn(async () => {
    if (opts.closeError) throw opts.closeError;
  });
  const filename = opts.storage === 'opfs' ? '/teardown-failure.db' : ':memory:';

  // Inject a minimal database so these tests isolate client lifecycle behavior
  // from the SQLite implementation itself.
  const openDb: OpenDbFn = async () => ({
    db: { close: closeDatabase } as unknown as Database,
    filename,
    storage: opts.storage,
  });
  const client = await buildDirectClient(
    { docId: `direct-${opts.storage}-teardown`, filename, storage: opts.storage },
    openDb,
  );
  return { client, closeDatabase };
}

async function expectClientToBeTerminal(client: TreecrdtClient): Promise<void> {
  await expect(client.tree.nodeCount()).rejects.toThrow(CLIENT_CLOSED_ERROR);
}

beforeEach(() => {
  mockedClearOpfsStorage.mockReset();
  mockedClearOpfsStorage.mockResolvedValue(undefined);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

test('direct close failure leaves the handle terminal without retrying teardown', async () => {
  const { client, closeDatabase } = await createDirectClientHarness({
    storage: 'memory',
    closeError: new Error('close failed'),
  });

  // close is best-effort at the public API, even when the database close fails.
  await expect(client.close()).resolves.toBeUndefined();
  await expect(client.close()).resolves.toBeUndefined();
  await expect(client.drop()).resolves.toBeUndefined();

  await expectClientToBeTerminal(client);
  expect(closeDatabase).toHaveBeenCalledTimes(1);
  expect(mockedClearOpfsStorage).not.toHaveBeenCalled();
});

test('direct drop failure leaves the handle terminal without retrying teardown', async () => {
  mockedClearOpfsStorage.mockRejectedValueOnce(new Error('drop failed'));
  const { client, closeDatabase } = await createDirectClientHarness({
    storage: 'opfs',
  });

  // The database closes successfully, then OPFS deletion fails. Repeating drop
  // returns the original rejection instead of touching the released handle again.
  await expect(client.drop()).rejects.toThrow('drop failed');
  await expect(client.drop()).rejects.toThrow('drop failed');
  await expect(client.close()).resolves.toBeUndefined();

  await expectClientToBeTerminal(client);
  expect(closeDatabase).toHaveBeenCalledTimes(1);
  expect(mockedClearOpfsStorage).toHaveBeenCalledTimes(1);
});

test('dedicated-worker init failure terminates the endpoint and removes its listeners', async () => {
  const connection = createMockConnection('init');
  const endpoint = installDedicatedWorker(connection);

  await expect(
    createTreecrdtClient({
      docId: 'dedicated-init-failure',
      runtime: { type: 'dedicated-worker' },
      storage: { type: 'opfs' },
    }),
  ).rejects.toThrow('init failed');

  expect(endpoint.terminated).toBe(true);
  expect(connection.calls).toEqual(['init']);
});

test('dedicated-worker close failure terminates the endpoint without retrying', async () => {
  const connection = createMockConnection('close');
  const endpoint = installDedicatedWorker(connection);
  const client = await createTreecrdtClient({
    docId: 'dedicated-close-failure',
    runtime: { type: 'dedicated-worker' },
    storage: { type: 'memory' },
  });

  await expect(client.close()).resolves.toBeUndefined();
  await expect(client.close()).resolves.toBeUndefined();
  await expect(client.drop()).resolves.toBeUndefined();

  await expectClientToBeTerminal(client);
  expect(endpoint.terminated).toBe(true);
  expect(connection.calls).toEqual(['init', 'close']);
});

test('dedicated-worker drop failure terminates the endpoint without retrying', async () => {
  const connection = createMockConnection('drop');
  const endpoint = installDedicatedWorker(connection);
  const client = await createTreecrdtClient({
    docId: 'dedicated-drop-failure',
    runtime: { type: 'dedicated-worker' },
    storage: { type: 'memory' },
  });

  await expect(client.drop()).rejects.toThrow('drop failed');
  await expect(client.drop()).rejects.toThrow('drop failed');
  await expect(client.close()).resolves.toBeUndefined();

  await expectClientToBeTerminal(client);
  expect(endpoint.terminated).toBe(true);
  expect(connection.calls).toEqual(['init', 'drop']);
});

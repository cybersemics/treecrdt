import { afterEach, expect, test, vi } from 'vitest';

import { createClientFromBackend, type OpenDbFn } from '../src/client.js';
import { sharedWorkerStrategy } from '../src/runtime/shared-worker.js';
import { createMockConnection, installSharedWorker } from './mock-worker.js';

// The shared-worker host owns the real opener; the strategy never calls this one.
const openDb: OpenDbFn = async () => {
  throw new Error('openDb must not run on the client side');
};

// Shared-worker is internal-only (not reachable from public options), so these
// lifecycle tests connect through the strategy directly.
const connect = () =>
  sharedWorkerStrategy.connect({ storage: 'memory', docId: 'cleanup-shared-worker', openDb });

afterEach(() => {
  vi.unstubAllGlobals();
});

test('shared-worker cleans up after rejected initialization', async () => {
  const connection = createMockConnection('init');
  const worker = installSharedWorker(connection);

  await expect(connect()).rejects.toThrow('init failed');

  expect(worker.isClosed()).toBe(true);
  expect(connection.calls).toEqual(['init', 'close']);
});

test('shared-worker cleans up when close RPC fails', async () => {
  const connection = createMockConnection('close');
  const worker = installSharedWorker(connection);
  const client = await createClientFromBackend(await connect());

  await client.close();

  expect(worker.isClosed()).toBe(true);
  expect(connection.calls).toEqual(['init', 'close']);
});

test('shared-worker cleans up when drop RPC fails', async () => {
  const connection = createMockConnection('drop');
  const worker = installSharedWorker(connection);
  const client = await createClientFromBackend(await connect());

  await expect(client.drop()).rejects.toThrow('drop failed');

  expect(worker.isClosed()).toBe(true);
  expect(connection.calls).toEqual(['init', 'drop']);
});

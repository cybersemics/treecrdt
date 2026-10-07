import { afterEach, expect, test, vi } from 'vitest';

import { createTreecrdtClient } from '../src/client.browser.js';
import { createMockConnection, installSharedWorker } from './mock-worker.js';

const connect = () => createTreecrdtClient({ docId: 'cleanup-shared-worker', crossTab: true });

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
  const client = await connect();

  await client.close();

  expect(worker.isClosed()).toBe(true);
  expect(connection.calls).toEqual(['init', 'close']);
});

test('shared-worker cleans up when drop RPC fails', async () => {
  const connection = createMockConnection('drop');
  const worker = installSharedWorker(connection);
  const client = await connect();

  await expect(client.drop()).rejects.toThrow('drop failed');

  expect(worker.isClosed()).toBe(true);
  expect(connection.calls).toEqual(['init', 'drop']);
});

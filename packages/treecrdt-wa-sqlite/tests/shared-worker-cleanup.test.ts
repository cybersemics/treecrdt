import { afterEach, expect, test, vi } from 'vitest';

import { createTreecrdtClient } from '../src/client.browser.js';
import type { RpcRequest } from '../src/rpc.js';

type RpcResponse =
  | { id: number; ok: true; result?: unknown }
  | { id: number; ok: false; error: string };

/** Minimal SharedWorker message port that exposes RPC requests and cleanup state. */
class FakeSharedWorkerPort {
  readonly listeners = new Map<string, Set<(event: any) => void>>();
  readonly requests: RpcRequest[] = [];
  closed = false;

  constructor(private readonly respond: (request: RpcRequest) => RpcResponse) {}

  addEventListener(type: string, listener: (event: any) => void) {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: (event: any) => void) {
    this.listeners.get(type)?.delete(listener);
  }

  postMessage(request: RpcRequest) {
    this.requests.push(request);
    const response = this.respond(request);
    queueMicrotask(() => {
      for (const listener of this.listeners.get('message') ?? []) listener({ data: response });
    });
  }

  start() {}

  close() {
    this.closed = true;
  }
}

function installSharedWorkerPort(respond: (request: RpcRequest) => RpcResponse) {
  const port = new FakeSharedWorkerPort(respond);
  vi.stubGlobal(
    'SharedWorker',
    class {
      port = port;
    },
  );
  return port;
}

const clientOptions = {
  storage: { type: 'memory' as const },
  runtime: { type: 'shared-worker' as const, name: 'cleanup-test' },
  docId: 'cleanup-shared-worker',
};

function expectCleaned(port: FakeSharedWorkerPort) {
  expect(port.closed).toBe(true);
  expect([...port.listeners.values()].every((listeners) => listeners.size === 0)).toBe(true);
}

afterEach(() => {
  vi.unstubAllGlobals();
});

test('shared-worker cleans up after rejected initialization', async () => {
  const port = installSharedWorkerPort((request) => ({
    id: request.id,
    ok: false,
    error: 'init failed',
  }));

  await expect(
    createTreecrdtClient({
      ...clientOptions,
      storage: { type: 'opfs' },
      docId: 'cleanup-shared-worker-strict-opfs',
    }),
  ).rejects.toThrow('init failed');

  expectCleaned(port);
  expect(port.requests.map((request) => request.method)).toEqual(['init', 'close']);
  expect(port.requests[0]?.params).toEqual([
    '/',
    undefined,
    'opfs',
    'cleanup-shared-worker-strict-opfs',
    'throw',
  ]);
});

test('shared-worker cleans up when close RPC fails', async () => {
  const port = installSharedWorkerPort((request) =>
    request.method === 'init'
      ? { id: request.id, ok: true, result: { storage: 'memory', filename: ':memory:' } }
      : { id: request.id, ok: false, error: 'close failed' },
  );
  const client = await createTreecrdtClient(clientOptions);

  await client.close();

  expectCleaned(port);
  expect(port.requests.map((request) => request.method)).toEqual(['init', 'close']);
});

test('shared-worker cleans up when drop RPC fails', async () => {
  const port = installSharedWorkerPort((request) =>
    request.method === 'init'
      ? { id: request.id, ok: true, result: { storage: 'memory', filename: ':memory:' } }
      : { id: request.id, ok: false, error: 'drop failed' },
  );
  const client = await createTreecrdtClient(clientOptions);

  await expect(client.drop()).rejects.toThrow('drop failed');

  expectCleaned(port);
  expect(port.requests.map((request) => request.method)).toEqual(['init', 'drop']);
});

import { afterEach, expect, test, vi } from 'vitest';

import { createTreecrdtClient } from '../src/client.js';
import type { RpcRequest } from '../src/rpc.js';

type Runtime = 'dedicated-worker' | 'shared-worker';
type RpcResponse =
  | { id: number; ok: true; result?: unknown }
  | { id: number; ok: false; error: string };

class FakeEndpoint {
  readonly listeners = new Map<string, Set<(event: any) => void>>();
  readonly requests: RpcRequest[] = [];
  terminated = false;
  portClosed = false;

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

  terminate() {
    this.terminated = true;
  }

  close() {
    this.portClosed = true;
  }
}

function installEndpoint(runtime: Runtime, respond: (request: RpcRequest) => RpcResponse) {
  const endpoint = new FakeEndpoint(respond);
  if (runtime === 'dedicated-worker') {
    vi.stubGlobal(
      'Worker',
      class {
        constructor() {
          return endpoint;
        }
      },
    );
  } else {
    vi.stubGlobal(
      'SharedWorker',
      class {
        port = endpoint;
      },
    );
  }
  return endpoint;
}

function clientOptions(runtime: Runtime) {
  return {
    storage: { type: 'memory' as const },
    runtime:
      runtime === 'dedicated-worker'
        ? ({ type: runtime } as const)
        : ({ type: runtime, name: 'cleanup-test' } as const),
    docId: `cleanup-${runtime}`,
  };
}

function expectCleaned(runtime: Runtime, endpoint: FakeEndpoint) {
  expect(runtime === 'dedicated-worker' ? endpoint.terminated : endpoint.portClosed).toBe(true);
  expect([...endpoint.listeners.values()].every((listeners) => listeners.size === 0)).toBe(true);
}

afterEach(() => {
  vi.unstubAllGlobals();
});

for (const runtime of ['dedicated-worker', 'shared-worker'] as const) {
  test(`${runtime} cleans up after rejected initialization`, async () => {
    const endpoint = installEndpoint(runtime, (request) => ({
      id: request.id,
      ok: false,
      error: 'init failed',
    }));

    await expect(createTreecrdtClient(clientOptions(runtime))).rejects.toThrow('init failed');

    expectCleaned(runtime, endpoint);
    expect(endpoint.requests.map((request) => request.method)).toEqual(
      runtime === 'shared-worker' ? ['init', 'close'] : ['init'],
    );
  });

  test(`${runtime} cleans up when close RPC fails`, async () => {
    const endpoint = installEndpoint(runtime, (request) =>
      request.method === 'init'
        ? { id: request.id, ok: true, result: { storage: 'memory', filename: ':memory:' } }
        : { id: request.id, ok: false, error: 'close failed' },
    );
    const client = await createTreecrdtClient(clientOptions(runtime));

    await client.close();

    expectCleaned(runtime, endpoint);
  });

  test(`${runtime} cleans up when drop RPC fails`, async () => {
    const endpoint = installEndpoint(runtime, (request) =>
      request.method === 'init'
        ? { id: request.id, ok: true, result: { storage: 'memory', filename: ':memory:' } }
        : { id: request.id, ok: false, error: 'drop failed' },
    );
    const client = await createTreecrdtClient(clientOptions(runtime));

    await expect(client.drop()).rejects.toThrow('drop failed');

    expectCleaned(runtime, endpoint);
  });
}

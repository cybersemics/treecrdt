import { afterEach, expect, test, vi } from 'vitest';

import { createTreecrdtClient } from '../src/client.js';
import { SHARED_WORKER_DROPPED_ERROR, type RpcRequest } from '../src/rpc.js';

type Runtime = 'dedicated-worker' | 'shared-worker';
type RpcResponse =
  | { id: number; ok: true; result?: unknown }
  | { id: number; ok: false; error: string };

class FakeEndpoint {
  readonly listeners = new Map<string, Set<(event: any) => void>>();
  readonly workerErrorListeners = new Set<(event: any) => void>();
  readonly requests: RpcRequest[] = [];
  terminated = false;
  portClosed = false;

  constructor(private readonly respond: (request: RpcRequest) => RpcResponse | undefined) {}

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
    if (!response) return;
    queueMicrotask(() => {
      for (const listener of this.listeners.get('message') ?? []) listener({ data: response });
    });
  }

  emit(message: unknown) {
    for (const listener of this.listeners.get('message') ?? []) listener({ data: message });
  }

  emitMessageError() {
    for (const listener of this.listeners.get('messageerror') ?? []) listener({});
  }

  emitWorkerError(message: string) {
    for (const listener of this.workerErrorListeners) listener({ message });
  }

  start() {}

  terminate() {
    this.terminated = true;
  }

  close() {
    this.portClosed = true;
  }
}

function installEndpoint(
  runtime: Runtime,
  respond: (request: RpcRequest) => RpcResponse | undefined,
) {
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

        addEventListener(type: string, listener: (event: any) => void) {
          if (type === 'error') endpoint.workerErrorListeners.add(listener);
        }

        removeEventListener(type: string, listener: (event: any) => void) {
          if (type === 'error') endpoint.workerErrorListeners.delete(listener);
        }
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
  if (runtime === 'shared-worker') expect(endpoint.workerErrorListeners.size).toBe(0);
}

async function openSharedClientWithoutRpcResponses() {
  const endpoint = installEndpoint('shared-worker', (request) =>
    request.method === 'init'
      ? { id: request.id, ok: true, result: { storage: 'memory', filename: ':memory:' } }
      : undefined,
  );
  const client = await createTreecrdtClient(clientOptions('shared-worker'));
  return { client, endpoint };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

test('single-owner WAL rejects direct and shared-worker runtimes before startup', async () => {
  vi.stubGlobal('window', { crossOriginIsolated: true });
  vi.stubGlobal('navigator', { storage: { getDirectory: vi.fn() } });
  vi.stubGlobal('SharedWorker', class {});

  for (const runtime of ['direct', 'shared-worker'] as const) {
    await expect(
      createTreecrdtClient({
        storage: { type: 'opfs', writeMode: 'single-owner-wal' },
        runtime: { type: runtime },
      }),
    ).rejects.toThrow(
      'OPFS storage.writeMode "single-owner-wal" requires runtime "dedicated-worker" or runtime "auto"',
    );
  }
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

test('shared-worker terminal invalidation rejects pending calls and closes the client', async () => {
  const { client, endpoint } = await openSharedClientWithoutRpcResponses();
  const pending = client.meta.headLamport();
  await vi.waitFor(() => {
    expect(endpoint.requests.some((request) => request.method === 'headLamport')).toBe(true);
  });

  endpoint.emit({ type: 'terminal', error: SHARED_WORKER_DROPPED_ERROR });

  await expect(pending).rejects.toThrow(SHARED_WORKER_DROPPED_ERROR);
  await expect(client.meta.headLamport()).rejects.toThrow(SHARED_WORKER_DROPPED_ERROR);
  expectCleaned('shared-worker', endpoint);
  await client.close();
  await client.close();
});

test('shared-worker runtime errors reject pending calls and close the client', async () => {
  const { client, endpoint } = await openSharedClientWithoutRpcResponses();
  const pending = client.meta.headLamport();
  await vi.waitFor(() => {
    expect(endpoint.requests.some((request) => request.method === 'headLamport')).toBe(true);
  });

  endpoint.emitWorkerError('shared worker script failed');

  await expect(pending).rejects.toThrow('shared worker script failed');
  expectCleaned('shared-worker', endpoint);
});

test('shared-worker message errors send a best-effort close before cleanup', async () => {
  const { client, endpoint } = await openSharedClientWithoutRpcResponses();

  endpoint.emitMessageError();

  expect(endpoint.requests.at(-1)?.method).toBe('close');
  await expect(client.meta.headLamport()).rejects.toThrow('shared worker message error');
  expectCleaned('shared-worker', endpoint);
});

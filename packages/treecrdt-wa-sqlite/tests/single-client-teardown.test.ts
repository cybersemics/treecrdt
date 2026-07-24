import type { TreecrdtAdapter } from '@treecrdt/interface';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';

import {
  buildDirectClient,
  CLIENT_CLOSED_ERROR,
  createTreecrdtClient,
  type OpenDbFn,
} from '../src/client.js';
import { clearOpfsStorage } from '../src/opfs.js';
import type { RpcRequest } from '../src/rpc.js';
import type { Database, TreecrdtClient } from '../src/types.js';

vi.mock('../src/opfs.js', async (importOriginal) => {
  const original = await importOriginal<typeof import('../src/opfs.js')>();
  return { ...original, clearOpfsStorage: vi.fn() };
});

const mockedClearOpfsStorage = vi.mocked(clearOpfsStorage);

type TeardownMethod = 'close' | 'drop';

type TestRpcResponse =
  | { id: number; ok: true; result?: unknown }
  | { id: number; ok: false; error: string };

type TestWorkerEvent = { data: TestRpcResponse } | ErrorEvent;
type TestWorkerListener = (event: TestWorkerEvent) => void;

/**
 * Minimal Worker endpoint for testing the dedicated-worker client's RPC boundary.
 *
 * It does not run SQLite or any worker code. It only reproduces the behavior the
 * client depends on: asynchronous responses, event listeners, and termination.
 */
class TestDedicatedWorkerEndpoint {
  private readonly listeners = new Map<string, Set<TestWorkerListener>>();
  private readonly requests: RpcRequest[] = [];
  terminated = false;

  constructor(private readonly respond: (request: RpcRequest) => TestRpcResponse) {}

  addEventListener(type: string, listener: TestWorkerListener): void {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: TestWorkerListener): void {
    this.listeners.get(type)?.delete(listener);
  }

  postMessage(request: RpcRequest): void {
    this.requests.push(request);
    const response = this.respond(request);
    // A real Worker responds asynchronously. Keep that boundary so the test also
    // exercises the client's pending-request and cleanup ordering.
    queueMicrotask(() => {
      for (const listener of this.listeners.get('message') ?? []) listener({ data: response });
    });
  }

  terminate(): void {
    this.terminated = true;
  }

  get listenerCount(): number {
    return [...this.listeners.values()].reduce((total, listeners) => total + listeners.size, 0);
  }

  get teardownMethods(): RpcRequest['method'][] {
    return this.requests
      .filter((request) => request.method === 'close' || request.method === 'drop')
      .map((request) => request.method);
  }
}

function installDedicatedWorkerThatRejects(teardown: TeardownMethod): TestDedicatedWorkerEndpoint {
  const endpoint = new TestDedicatedWorkerEndpoint((request) => {
    // Initialization succeeds so the client reaches the teardown path under test.
    // Only the selected teardown RPC fails.
    if (request.method === 'init') {
      return {
        id: request.id,
        ok: true,
        result: { storage: 'memory', filename: ':memory:' },
      };
    }
    if (request.method === teardown) {
      return { id: request.id, ok: false, error: `${teardown} failed` };
    }
    return { id: request.id, ok: true, result: 1 };
  });

  // createTreecrdtClient constructs the Worker internally, so replace the global
  // constructor with one that returns our observable endpoint.
  vi.stubGlobal(
    'Worker',
    class {
      constructor() {
        return endpoint;
      }
    },
  );

  return endpoint;
}

async function createDirectClientHarness(opts: { storage: 'memory' | 'opfs'; closeError?: Error }) {
  const closeDatabase = vi.fn(async () => {
    if (opts.closeError) throw opts.closeError;
  });
  const readNodeCount = vi.fn(async () => 1);
  const filename = opts.storage === 'opfs' ? '/teardown-failure.db' : ':memory:';

  // Inject a minimal database so these tests isolate client lifecycle behavior
  // from the SQLite implementation itself.
  const openDb: OpenDbFn = async () => ({
    api: { treeNodeCount: readNodeCount } as unknown as TreecrdtAdapter,
    db: { close: closeDatabase } as unknown as Database,
    filename,
    storage: opts.storage,
  });
  const client = await buildDirectClient(
    { docId: `direct-${opts.storage}-teardown`, filename, storage: opts.storage },
    openDb,
  );
  return { client, closeDatabase, readNodeCount };
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
  const { client, closeDatabase, readNodeCount } = await createDirectClientHarness({
    storage: 'memory',
    closeError: new Error('close failed'),
  });

  // close is best-effort at the public API, even when the database close fails.
  await expect(client.close()).resolves.toBeUndefined();
  await expect(client.close()).resolves.toBeUndefined();
  await expect(client.drop()).resolves.toBeUndefined();

  await expectClientToBeTerminal(client);
  expect(closeDatabase).toHaveBeenCalledTimes(1);
  expect(readNodeCount).not.toHaveBeenCalled();
  expect(mockedClearOpfsStorage).not.toHaveBeenCalled();
});

test('direct drop failure leaves the handle terminal without retrying teardown', async () => {
  mockedClearOpfsStorage.mockRejectedValueOnce(new Error('drop failed'));
  const { client, closeDatabase, readNodeCount } = await createDirectClientHarness({
    storage: 'opfs',
  });

  // The database closes successfully, then OPFS deletion fails. Repeating drop
  // returns the original rejection instead of touching the released handle again.
  await expect(client.drop()).rejects.toThrow('drop failed');
  await expect(client.drop()).rejects.toThrow('drop failed');
  await expect(client.close()).resolves.toBeUndefined();

  await expectClientToBeTerminal(client);
  expect(closeDatabase).toHaveBeenCalledTimes(1);
  expect(readNodeCount).not.toHaveBeenCalled();
  expect(mockedClearOpfsStorage).toHaveBeenCalledTimes(1);
});

test('dedicated-worker close failure terminates the endpoint without retrying', async () => {
  const endpoint = installDedicatedWorkerThatRejects('close');
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
  expect(endpoint.listenerCount).toBe(0);
  expect(endpoint.teardownMethods).toEqual(['close']);
});

test('dedicated-worker drop failure terminates the endpoint without retrying', async () => {
  const endpoint = installDedicatedWorkerThatRejects('drop');
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
  expect(endpoint.listenerCount).toBe(0);
  expect(endpoint.teardownMethods).toEqual(['drop']);
});

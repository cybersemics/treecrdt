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
import type { Database } from '../src/types.js';

vi.mock('../src/opfs.js', async (importOriginal) => {
  const original = await importOriginal<typeof import('../src/opfs.js')>();
  return { ...original, clearOpfsStorage: vi.fn() };
});

const mockedClearOpfsStorage = vi.mocked(clearOpfsStorage);

type RpcResponse =
  | { id: number; ok: true; result?: unknown }
  | { id: number; ok: false; error: string };

class FakeWorker {
  readonly listeners = new Map<string, Set<(event: any) => void>>();
  readonly requests: RpcRequest[] = [];
  terminated = false;

  constructor(private readonly respond: (request: RpcRequest) => RpcResponse) {}

  addEventListener(type: string, listener: (event: any) => void): void {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: (event: any) => void): void {
    this.listeners.get(type)?.delete(listener);
  }

  postMessage(request: RpcRequest): void {
    this.requests.push(request);
    const response = this.respond(request);
    queueMicrotask(() => {
      for (const listener of this.listeners.get('message') ?? []) listener({ data: response });
    });
  }

  terminate(): void {
    this.terminated = true;
  }
}

function installDedicatedWorker(teardown: 'close' | 'drop'): FakeWorker {
  const worker = new FakeWorker((request) => {
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

  vi.stubGlobal(
    'Worker',
    class {
      constructor() {
        return worker;
      }
    },
  );

  return worker;
}

async function createDirectClient(teardown: 'close' | 'drop') {
  const close = vi.fn(async () => {
    if (teardown === 'close') throw new Error('close failed');
  });
  const treeNodeCount = vi.fn(async () => 1);
  const storage = teardown === 'drop' ? 'opfs' : 'memory';
  const filename = storage === 'opfs' ? '/drop-failure.db' : ':memory:';
  const openDb: OpenDbFn = async () => ({
    api: { treeNodeCount } as unknown as TreecrdtAdapter,
    db: { close } as unknown as Database,
    filename,
    storage,
  });
  const client = await buildDirectClient(
    { docId: `direct-${teardown}-failure`, filename, storage },
    openDb,
  );
  return { client, close, treeNodeCount };
}

beforeEach(() => {
  mockedClearOpfsStorage.mockReset();
  mockedClearOpfsStorage.mockRejectedValue(new Error('drop failed'));
});

afterEach(() => {
  vi.unstubAllGlobals();
});

test.each(['close', 'drop'] as const)(
  'direct %s failure leaves the handle terminal without retrying teardown',
  async (teardown) => {
    const { client, close, treeNodeCount } = await createDirectClient(teardown);

    if (teardown === 'close') {
      await expect(client.close()).resolves.toBeUndefined();
      await expect(client.drop()).resolves.toBeUndefined();
    } else {
      await expect(client.drop()).rejects.toThrow('drop failed');
      await expect(client.drop()).rejects.toThrow('drop failed');
      await expect(client.close()).resolves.toBeUndefined();
    }

    await expect(client.tree.nodeCount()).rejects.toThrow(CLIENT_CLOSED_ERROR);
    expect(close).toHaveBeenCalledTimes(1);
    expect(treeNodeCount).not.toHaveBeenCalled();
    expect(mockedClearOpfsStorage).toHaveBeenCalledTimes(teardown === 'drop' ? 1 : 0);
  },
);

test.each(['close', 'drop'] as const)(
  'dedicated-worker %s failure terminates the endpoint without retrying teardown',
  async (teardown) => {
    const worker = installDedicatedWorker(teardown);
    const client = await createTreecrdtClient({
      docId: `dedicated-${teardown}-failure`,
      runtime: { type: 'dedicated-worker' },
      storage: { type: 'memory' },
    });

    if (teardown === 'close') {
      await expect(client.close()).resolves.toBeUndefined();
      await expect(client.drop()).resolves.toBeUndefined();
    } else {
      await expect(client.drop()).rejects.toThrow('drop failed');
      await expect(client.drop()).rejects.toThrow('drop failed');
      await expect(client.close()).resolves.toBeUndefined();
    }

    await expect(client.tree.nodeCount()).rejects.toThrow(CLIENT_CLOSED_ERROR);
    expect(worker.terminated).toBe(true);
    expect([...worker.listeners.values()].every((listeners) => listeners.size === 0)).toBe(true);
    expect(worker.requests.filter((request) => request.method === teardown)).toHaveLength(1);
    expect(
      worker.requests.filter((request) => request.method === 'close' || request.method === 'drop'),
    ).toHaveLength(1);
  },
);

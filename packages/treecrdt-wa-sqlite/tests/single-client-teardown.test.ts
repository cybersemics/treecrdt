import * as Comlink from 'comlink';
import type { TreecrdtAdapter } from '@treecrdt/interface';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';

import { createTreecrdtClient } from '../src/client.browser.js';
import { buildDirectClient, CLIENT_CLOSED_ERROR, type OpenDbFn } from '../src/client.js';
import type { TreecrdtConnection } from '../src/connection.js';
import { clearOpfsStorage } from '../src/opfs.js';
import type { Database, TreecrdtClient } from '../src/types.js';

vi.mock('../src/opfs.js', async (importOriginal) => {
  const original = await importOriginal<typeof import('../src/opfs.js')>();
  return { ...original, clearOpfsStorage: vi.fn() };
});

const mockedClearOpfsStorage = vi.mocked(clearOpfsStorage);

type TeardownMethod = 'close' | 'drop';
type RejectedWorkerMethod = 'init' | TeardownMethod;

type MockConnection = Pick<
  TreecrdtConnection,
  | 'init'
  | 'close'
  | 'drop'
  | 'subscribeMaterialized'
  | 'unsubscribeMaterialized'
  | 'notifyMaterialized'
  | 'session'
> & { calls: string[] };

function createMockConnection(rejectMethod: RejectedWorkerMethod): MockConnection {
  const calls: string[] = [];
  return {
    calls,
    async init(config) {
      calls.push('init');
      if (rejectMethod === 'init') throw new Error('init failed');
      expect(config.fallback).toBe(config.storage === 'opfs' ? 'throw' : 'memory');
      return { storage: 'memory', filename: ':memory:' };
    },
    async close() {
      calls.push('close');
      if (rejectMethod === 'close') throw new Error('close failed');
    },
    async drop() {
      calls.push('drop');
      if (rejectMethod === 'drop') throw new Error('drop failed');
    },
    subscribeMaterialized() {},
    unsubscribeMaterialized() {},
    async notifyMaterialized() {},
    session: Comlink.proxy({
      async sqlExec() {},
      async sqlGetText() {
        return null;
      },
      async append() {
        return { headSeq: 0, changes: [] };
      },
      async appendMany() {
        return { headSeq: 0, changes: [] };
      },
      async opsSince() {
        return [];
      },
      async opRefsAll() {
        return [];
      },
      async opRefsChildren() {
        return [];
      },
      async opsByOpRefs() {
        return [];
      },
      async treeChildren() {
        return [];
      },
      async treeChildrenPage() {
        return [];
      },
      async treeDump() {
        return [];
      },
      async treePayload() {
        return null;
      },
      async treeNodeCount() {
        return 1;
      },
      async treeParent() {
        return null;
      },
      async treeExists() {
        return false;
      },
      async headLamport() {
        return 0;
      },
      async replicaMaxCounter() {
        return 0;
      },
    }),
  };
}

function installDedicatedWorker(connection: MockConnection) {
  let terminated = false;

  vi.stubGlobal(
    'Worker',
    class {
      private readonly port: MessagePort;

      constructor() {
        const channel = new MessageChannel();
        Comlink.expose(connection, channel.port1);
        channel.port1.start();
        this.port = channel.port2;
        this.port.start();
      }

      postMessage(message: unknown, transfer?: Transferable[]) {
        this.port.postMessage(message as any, transfer as any);
      }

      addEventListener(type: string, listener: EventListener) {
        this.port.addEventListener(type, listener);
      }

      removeEventListener(type: string, listener: EventListener) {
        this.port.removeEventListener(type, listener);
      }

      terminate() {
        terminated = true;
        this.port.close();
      }
    },
  );

  return {
    get terminated() {
      return terminated;
    },
  };
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

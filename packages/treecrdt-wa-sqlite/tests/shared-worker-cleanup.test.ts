import * as Comlink from 'comlink';
import { afterEach, expect, test, vi } from 'vitest';

import { createTreecrdtClient } from '../src/client.browser.js';
import type { TreecrdtConnection } from '../src/connection.js';
import type { BackendInitConfig, BackendInitResult, MaterializationListener } from '../src/session.js';
import type { TreecrdtSession } from '../src/session.js';

type MockConnection = TreecrdtConnection & {
  readonly calls: string[];
};

function createMockSession(): TreecrdtSession {
  return {
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
      return 0;
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
  };
}

function createMockSharedConnection(opts: {
  onInit?: (config: BackendInitConfig) => Promise<BackendInitResult>;
  onClose?: () => Promise<void>;
  onDrop?: () => Promise<void>;
}): MockConnection {
  const calls: string[] = [];
  const listeners = new Set<MaterializationListener>();
  const session = Comlink.proxy(createMockSession());

  return {
    calls,
    session,
    async init(config) {
      calls.push('init');
      if (opts.onInit) return opts.onInit(config);
      return { storage: 'memory', filename: ':memory:' };
    },
    async close() {
      calls.push('close');
      if (opts.onClose) await opts.onClose();
    },
    async drop() {
      calls.push('drop');
      if (opts.onDrop) await opts.onDrop();
    },
    subscribeMaterialized(listener) {
      listeners.add(listener);
    },
    unsubscribeMaterialized(listener) {
      listeners.delete(listener);
    },
    async notifyMaterialized() {},
  };
}

function installSharedWorker(connection: MockConnection) {
  let closed = false;

  vi.stubGlobal(
    'SharedWorker',
    class {
      port: MessagePort;

      constructor() {
        const channel = new MessageChannel();
        Comlink.expose(connection, channel.port1);
        channel.port1.start();
        this.port = channel.port2;
        const originalClose = channel.port2.close.bind(channel.port2);
        channel.port2.close = () => {
          closed = true;
          originalClose();
        };
      }
    },
  );

  return {
    isClosed: () => closed,
    get calls() {
      return connection.calls;
    },
  };
}

const clientOptions = {
  storage: { type: 'memory' as const },
  runtime: { type: 'shared-worker' as const, name: 'cleanup-test' },
  docId: 'cleanup-shared-worker',
};

afterEach(() => {
  vi.unstubAllGlobals();
});

test('shared-worker cleans up after rejected initialization', async () => {
  const connection = createMockSharedConnection({
    onInit: async () => {
      throw new Error('init failed');
    },
  });
  const worker = installSharedWorker(connection);

  await expect(
    createTreecrdtClient({
      ...clientOptions,
      storage: { type: 'opfs' },
      docId: 'cleanup-shared-worker-strict-opfs',
    }),
  ).rejects.toThrow('init failed');

  expect(worker.isClosed()).toBe(true);
  expect(worker.calls).toEqual(['init', 'close']);
});

test('shared-worker cleans up when close RPC fails', async () => {
  const connection = createMockSharedConnection({
    onClose: async () => {
      throw new Error('close failed');
    },
  });
  const worker = installSharedWorker(connection);
  const client = await createTreecrdtClient(clientOptions);

  await client.close();

  expect(worker.isClosed()).toBe(true);
  expect(worker.calls).toEqual(['init', 'close']);
});

test('shared-worker cleans up when drop RPC fails', async () => {
  const connection = createMockSharedConnection({
    onDrop: async () => {
      throw new Error('drop failed');
    },
  });
  const worker = installSharedWorker(connection);
  const client = await createTreecrdtClient(clientOptions);

  await expect(client.drop()).rejects.toThrow('drop failed');

  expect(worker.isClosed()).toBe(true);
  expect(worker.calls).toEqual(['init', 'drop']);
});

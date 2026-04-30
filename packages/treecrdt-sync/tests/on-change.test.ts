import { expect, test, vi } from 'vitest';

import { createTreecrdtWebSocketSyncFromTransport } from '../src/create-sync-from-transport.js';
import type { TreecrdtWebSocketSyncClient } from '../src/types.js';
import type { Operation } from '@treecrdt/interface';
import type { DuplexTransport } from '@treecrdt/sync-protocol/transport';
import type { SyncMessage } from '@treecrdt/sync-protocol';

test('onChange delegates to client.onMaterialized', () => {
  const unsub = vi.fn();
  const onMaterialized = vi.fn().mockReturnValue(unsub);
  const client = {
    docId: 'test-doc',
    onMaterialized,
    meta: { headLamport: async () => 0 },
    runner: undefined,
    opRefs: { all: async () => [] as Uint8Array[], children: async () => [] as Uint8Array[] },
    ops: {
      get: async () => [] as Operation[],
      append: async () => {},
      appendMany: async () => {},
      all: async () => [] as Operation[],
      since: async () => [] as Operation[],
      children: async () => [] as Operation[],
    },
    mode: 'memory',
    storage: 'memory',
    tree: {
      children: async () => [],
      dump: async () => [],
      nodeCount: async () => 0,
      parent: async () => null,
      exists: async () => false,
      getPayload: async () => null,
    },
    local: {
      insert: async () => {
        throw new Error('not used');
      },
      move: async () => {
        throw new Error('not used');
      },
      delete: async () => {
        throw new Error('not used');
      },
      payload: async () => {
        throw new Error('not used');
      },
    },
    close: async () => {},
  } as unknown as TreecrdtWebSocketSyncClient;

  const transport: DuplexTransport<SyncMessage<Operation>> = {
    send: async () => {},
    onMessage: () => () => {},
  };

  const sync = createTreecrdtWebSocketSyncFromTransport(client, transport, undefined);

  const l1 = () => {};
  const u1 = sync.onChange(l1);
  expect(onMaterialized).toHaveBeenLastCalledWith(l1);
  u1();

  onMaterialized.mockClear();
  const l2 = () => {};
  const u2 = sync.onChange(l2);
  expect(onMaterialized).toHaveBeenLastCalledWith(l2);
  u2();

  void sync.close();
});

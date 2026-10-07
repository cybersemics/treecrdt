import Database from 'better-sqlite3';
import { expect, test, vi } from 'vitest';
import { createInMemoryConnectedPeers } from '@treecrdt/sync-protocol/in-memory';
import { treecrdtSyncV0ProtobufCodec } from '@treecrdt/sync-protocol/protobuf';
import { createTreecrdtSyncBackendFromClient } from '@treecrdt/sync-sqlite/backend';
import { createMemoryClient } from '@treecrdt/wasm/memory';
import { createMemorySyncBackend } from '@treecrdt/wasm/sync';
import { createTreecrdtClient, loadTreecrdtExtension } from '../dist/index.js';

test('hydrates memory from SQLite and converges exact local operations and incoming edits', async () => {
  const database = new Database(':memory:');
  const memory = await createMemoryClient();
  const root = '0'.repeat(32);
  const parent = '1'.repeat(32);
  const child = '2'.repeat(32);
  try {
    loadTreecrdtExtension(database);
    const persistent = await createTreecrdtClient(database, { docId: 'loopback' });
    const writer = persistent.local.forReplica(new Uint8Array(32).fill(9));
    const initial = await writer.insert(root, parent, { type: 'first' }, Uint8Array.of(1));
    const peers = createInMemoryConnectedPeers({
      backendA: createMemorySyncBackend(memory, { docId: 'loopback' }),
      backendB: createTreecrdtSyncBackendFromClient(persistent, 'loopback'),
      codec: treecrdtSyncV0ProtobufCodec,
    });
    let syncTail = Promise.resolve();
    const unsubscribe = persistent.onMaterialized(() => {
      syncTail = syncTail.then(() => peers.peerB.notifyLocalUpdate());
    });
    const subscription = peers.peerA.subscribe(peers.transportA, { all: {} });
    try {
      await subscription.ready;
      expect(memory.get(parent)?.payload).toEqual(Uint8Array.of(1));
      expect(memory.operationsFrom(0)).toEqual([initial]);

      const publish = vi.fn();
      const unsubscribeView = memory.subscribe(publish);
      const committed = memory.transact((transaction) => {
        transaction.local.insert(parent, child, null, Uint8Array.of(2));
        transaction.local.payload(child, Uint8Array.of(3));
      });
      expect(memory.get(child)?.payload).toEqual(Uint8Array.of(3));
      expect(publish).toHaveBeenCalledTimes(1);
      expect(await persistent.tree.get(child)).toBeUndefined();

      // Persistence retains the authored IDs, rather than minting the same edits a second time.
      await persistent.ops.appendMany(committed.operations);
      await syncTail;
      expect(await persistent.ops.all()).toEqual([initial, ...committed.operations]);
      expect(publish).toHaveBeenCalledTimes(1);

      await writer.move(child, root, { type: 'after', after: parent });
      await writer.payload(child, Uint8Array.of(4));
      await syncTail;
      await vi.waitFor(() => {
        expect(memory.get(root)?.children).toEqual([parent, child]);
        expect(memory.get(child)?.payload).toEqual(Uint8Array.of(4));
      });
      expect(memory.operationsFrom(0)).toEqual(await persistent.ops.all());
      unsubscribeView();
    } finally {
      unsubscribe();
      subscription.stop();
      await subscription.done;
      peers.detach();
    }
  } finally {
    memory.close();
    database.close();
  }
});

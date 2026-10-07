import { afterEach, expect, test, vi } from 'vitest';
import { createInMemoryConnectedPeers } from '@treecrdt/sync-protocol/in-memory';
import { treecrdtSyncV0ProtobufCodec } from '@treecrdt/sync-protocol/protobuf';
import { createMemorySyncBackend } from '../src/sync.js';
import { createMemoryClient } from '../src/memory-node.js';
import { WasmTree } from '../pkg/treecrdt_wasm.js';
import {
  createInitializedMemoryClient,
  type MemoryClient,
  type MemoryTransaction,
} from '../src/memory-client.js';

const root = '0'.repeat(32);
const a = '1'.padStart(32, '0');
const b = '2'.padStart(32, '0');
const clients: MemoryClient[] = [];
function open() {
  const native = new WasmTree('01'.repeat(32));
  const client = createInitializedMemoryClient(native);
  clients.push(client);
  return { client, native };
}
afterEach(() => {
  for (const client of clients.splice(0)) client.close();
  vi.restoreAllMocks();
});

test('composed commands read live rows and publish only owned changed records', () => {
  const { client, native } = open();
  client.transact(({ local }) => {
    local.insert('0', '1', undefined, Uint8Array.of(1));
    local.insert(root, b);
  });
  const enumeration = vi.spyOn(native, 'nodeIds');
  const listener = vi.fn();
  client.subscribe(listener);
  const retainedRow = client.get(a)!;
  let escaped!: MemoryTransaction;
  const result = client.transact((transaction) => {
    escaped = transaction;
    transaction.local.move('1', '2');
    expect(transaction.get(a)?.parentId).toBe(b);
    const moved = transaction.getChanges();
    transaction.local.payload(a, Uint8Array.of(2));
    expect(transaction.get(a)?.payload).toEqual(Uint8Array.of(2));
    expect(moved.changes.find((change) => change.id === a)?.after?.payload).toEqual(
      Uint8Array.of(1),
    );
    expect(listener).not.toHaveBeenCalled();
    expect(() => client.operationsFrom(0)).toThrow('during a transaction');
  });
  const change = result.changes.changes.find((change) => change.id === a)!;
  expect(change.before).toMatchObject({ parentId: root, payload: Uint8Array.of(1) });
  expect(change.after).toMatchObject({ parentId: b, payload: Uint8Array.of(2) });
  expect(result.operations.map((operation) => operation.kind.type)).toEqual(['move', 'payload']);
  expect(listener).toHaveBeenCalledTimes(1);
  expect(listener).toHaveBeenCalledWith(result.changes);
  expect(enumeration).not.toHaveBeenCalled();
  expect(retainedRow.parentId).toBe(root);
  retainedRow.payload!.fill(99);
  expect(retainedRow.payload).toEqual(Uint8Array.of(1));
  expect(() => (retainedRow.children as string[]).push('invalid')).toThrow();
  change.after!.payload!.fill(99);
  expect(change.after!.payload).toEqual(Uint8Array.of(2));
  expect(() => escaped.get(a)).toThrow('finished');
  expect(() => escaped.local.insert(root, a)).toThrow('finished');
  expect(() => escaped.getChanges()).toThrow('finished');
  for (const cursor of [NaN, 0.9, 2 ** 32]) {
    expect(() => client.operationsFrom(cursor)).toThrow();
  }
});

test.each(['callback', 'caught native error', 'caught boundary error'])(
  '%s failure discards provisional deltas and restores the live tree',
  (failure) => {
    const { client } = open();
    const { client: control } = open();
    const listener = vi.fn();
    client.subscribe(listener);
    expect(() =>
      client.transact((transaction) => {
        transaction.local.insert(root, a);
        transaction.getChanges();
        if (failure === 'callback') throw new Error('abort');
        try {
          if (failure === 'caught native error') transaction.local.move(a, root, b);
          else transaction.local.insert(root, null as unknown as string);
        } catch {
          /* Catching a failed write must not commit previous writes. */
        }
      }),
    ).toThrow();
    expect(client.get(a)).toBeUndefined();
    expect(client.operationsFrom(0)).toEqual([]);
    expect(client.revision).toBe(0);
    expect(listener).not.toHaveBeenCalled();
    expect(client.local.insert(root, b)).toEqual(control.local.insert(root, b));
    expect(
      listener.mock.calls[0][0].changes.map((change: { id: string }) => change.id),
    ).not.toContain(a);
  },
);

test('async transaction callbacks are rejected and late writes cannot escape the transaction', async () => {
  const { client } = open();
  let continuation!: Promise<unknown>;
  expect(() =>
    client.transact(async (transaction) => {
      transaction.local.insert(root, a);
      continuation = Promise.resolve().then(() => transaction.local.insert(root, b));
      await continuation;
    }),
  ).toThrow('must be synchronous');
  await expect(continuation).rejects.toThrow('finished');
  expect(client.get(a)).toBeUndefined();
});

test('net-unchanged edits retain operations without publishing a new revision', () => {
  const { client } = open();
  client.local.insert(root, a, undefined, Uint8Array.of(1));
  const revision = client.revision;
  const listener = vi.fn();
  client.subscribe(listener);
  const result = client.transact(({ local, getChanges }) => {
    local.payload(a, Uint8Array.of(2));
    getChanges();
    local.payload(a, Uint8Array.of(1));
  });
  expect(result.operations).toHaveLength(2);
  expect(result.changes.changes).toEqual([]);
  expect(client.revision).toBe(revision);
  expect(listener).not.toHaveBeenCalled();
});

test('undo and redo publish real row changes and roll back together with a failed command', async () => {
  const client = await createMemoryClient({ replicaId: new Uint8Array(32).fill(1) });
  clients.push(client);
  client.transact(({ local }) => {
    local.insert(root, a, undefined, Uint8Array.of(1));
    local.insert(root, b);
  });
  const edits = client.transact(({ local }) => {
    local.move(a, b);
    local.payload(a, Uint8Array.of(2));
  });
  const undone = client.transact((transaction) => {
    const inverses = transaction.revert(edits.operations.map((op) => op.meta.id));
    expect(transaction.get(a)).toMatchObject({ parentId: root, payload: Uint8Array.of(1) });
    expect(transaction.getChanges().changes.find((change) => change.id === a)).toMatchObject({
      before: { parentId: b, payload: Uint8Array.of(2) },
      after: { parentId: root, payload: Uint8Array.of(1) },
    });
    return inverses.map((op) => op.meta.id);
  });
  const operations = client.operationsFrom(0);
  const revision = client.revision;
  expect(() =>
    client.transact((transaction) => {
      transaction.revert(undone.value);
      transaction.getChanges();
      throw new Error('cancel');
    }),
  ).toThrow('cancel');
  expect(client.operationsFrom(0)).toEqual(operations);
  expect(client.revision).toBe(revision);
  expect(client.get(a)).toMatchObject({ parentId: root, payload: Uint8Array.of(1) });
  client.revert(undone.value);
  expect(client.get(a)).toMatchObject({ parentId: b, payload: Uint8Array.of(2) });
});

test('late remote replay reports actual changed rows; duplicate operations do not notify', () => {
  const { client: source } = open();
  const { client: receiver } = open();
  const insert = source.local.insert(root, a);
  const payload = source.local.payload(a, Uint8Array.of(42));
  receiver.appendOperations([payload]);
  const before = receiver.get(a);
  const listener = vi.fn();
  receiver.subscribe(listener);
  receiver.appendOperations([insert]);
  expect(receiver.get(a)?.payload).toEqual(Uint8Array.of(42));
  expect(listener.mock.calls[0][0].changes).toContainEqual(
    expect.objectContaining({
      id: a,
      before,
      after: expect.objectContaining({ payload: Uint8Array.of(42) }),
    }),
  );
  receiver.appendOperations([insert, payload]);
  expect(listener).toHaveBeenCalledTimes(1);
  receiver.close();
  expect(() => receiver.get(a)).toThrow('closed');
});

test('reentrant writes preserve ordered event records and isolate subscriber failures', () => {
  const { client } = open();
  const error = vi.spyOn(console, 'error').mockImplementation(() => {});
  const seen: Array<[number, boolean]> = [];
  client.subscribe((batch) => {
    if (batch.revision === 1) client.local.insert(root, b);
    throw new Error('observer failure');
  });
  client.subscribe((batch) => seen.push([batch.revision, !!client.get(b)]));
  const receipt = client.transact(({ local }) => local.insert(root, a));
  expect(seen).toEqual([
    [1, true],
    [2, true],
  ]);
  expect(receipt.operations).toHaveLength(1);
  expect(receipt.changes.changes.map((change) => change.id)).not.toContain(b);
  expect(client.revision).toBe(2);
  expect(error).toHaveBeenCalledTimes(2);
});

test('live reads work with the existing loopback sync protocol', async () => {
  const { client: memory } = open();
  const storagePeer = createInitializedMemoryClient(new WasmTree('02'.repeat(32)));
  clients.push(storagePeer);
  storagePeer.local.insert(root, a, undefined, Uint8Array.of(1));
  const peers = createInMemoryConnectedPeers({
    backendA: createMemorySyncBackend(memory, { docId: 'memory-sync' }),
    backendB: createMemorySyncBackend(storagePeer, { docId: 'memory-sync' }),
    codec: treecrdtSyncV0ProtobufCodec,
  });
  let syncTail = Promise.resolve();
  const unsubscribe = storagePeer.subscribe(() => {
    syncTail = syncTail.then(() => peers.peerB.notifyLocalUpdate());
  });
  const subscription = peers.peerA.subscribe(peers.transportA, { all: {} });
  try {
    await subscription.ready;
    expect(memory.get(a)?.payload).toEqual(Uint8Array.of(1));
    const receipt = memory.transact(({ local }) => local.payload(a, Uint8Array.of(2)));
    const revision = memory.revision;
    storagePeer.appendOperations(receipt.operations);
    await syncTail;
    expect(memory.revision).toBe(revision);
    storagePeer.local.move(a, root, null);
    storagePeer.local.payload(a, Uint8Array.of(3));
    await syncTail;
    await vi.waitFor(() => expect(memory.get(a)?.payload).toEqual(Uint8Array.of(3)));
    expect(memory.operationsFrom(0)).toEqual(storagePeer.operationsFrom(0));
  } finally {
    unsubscribe();
    subscription.stop();
    await subscription.done;
    peers.detach();
  }
});

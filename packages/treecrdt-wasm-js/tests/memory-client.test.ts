import { afterEach, expect, test, vi } from 'vitest';
import { createInMemoryConnectedPeers } from '@treecrdt/sync-protocol/in-memory';
import { treecrdtSyncV0ProtobufCodec } from '@treecrdt/sync-protocol/protobuf';
import { createMemorySyncBackend } from '../src/sync.js';
import { createMemoryClient } from '../src/memory-node.js';
import { WasmHistoryReader, WasmTree } from '../pkg/treecrdt_wasm.js';
import {
  createInitializedMemoryClient,
  type MemoryClient,
  type MemoryReader,
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

test('batched history reproduces accepted boundaries, skipping incoming gaps without exporting the log', () => {
  const { client, native } = open();
  const rows = (reader: MemoryReader) =>
    reader.nodeIds().map((id) => ({
      row: reader.get(id),
      content: reader.getContent(id),
      children: reader.getChildren(id),
      position: reader.getPosition(id),
    }));
  const spans: { from: number; to: number }[] = [];
  const expected: {
    before: ReturnType<typeof rows>;
    after: ReturnType<typeof rows>;
    changes: unknown;
  }[] = [];
  const record = <T>(work: (transaction: MemoryTransaction) => T) => {
    const from = client.operationCount();
    const before = rows(client);
    const result = client.transact(work);
    spans.push({ from, to: client.operationCount() });
    expected.push({ before, after: rows(client), changes: result.changes.changes });
    return result.value;
  };
  client.local.insert(root, b, undefined, Uint8Array.of(0));
  record(({ local }) => local.insert(root, a, undefined, Uint8Array.of(1)));
  record(({ local }) => {
    local.move(a, b);
    local.payload(a, Uint8Array.of(2));
  });
  const remote = createInitializedMemoryClient(new WasmTree('02'.repeat(32)));
  clients.push(remote);
  client.appendOperations([remote.local.insert(root, '3', null, Uint8Array.of(3))]);
  record(({ local }) => local.payload(a, Uint8Array.of(4)));
  const deletion = record(({ local }) => local.delete(a));
  const undo = record((transaction) => transaction.revert([deletion.meta.id]));
  record((transaction) => transaction.revert(undo.map((operation) => operation.meta.id)));
  record(({ local }) => {
    local.payload(b, Uint8Array.of(5));
    local.payload(b, Uint8Array.of(0));
  });
  record(() => {});
  const initial = rows(client);
  const revision = client.revision;
  const exportLog = vi.spyOn(native, 'operationsFrom');
  const exportRows = vi.spyOn(native, 'operationsAt');
  const listener = vi.fn();
  client.subscribe(listener);
  client.readHistory(spans, ({ before, after, changes }, index) => {
    expect({ before: rows(before), after: rows(after), changes }).toEqual(expected[index]);
    expect(before.get('1')).toEqual(before.get(a));
    expect(before.getContent('1')).toEqual(before.getContent(a));
    expect(before.getChildren('1')).toBe(before.getChildren(a));
    expect(before.getPosition('1')).toBe(before.getPosition(a));
  });
  expect(exportLog).not.toHaveBeenCalled();
  expect(exportRows).not.toHaveBeenCalled();
  expect(rows(client)).toEqual(initial);
  expect(client.revision).toBe(revision);
  expect(listener).not.toHaveBeenCalled();
});

test.each([
  { from: -1, to: 0 },
  { from: 0, to: 1 },
  { from: 2, to: 1 },
  { from: 1, to: 3 },
  { from: 1, to: NaN },
  { from: 1, to: 1.5 },
])('history validates all spans before visiting: %j', (invalid) => {
  const { client } = open();
  client.local.insert(root, a);
  const visit = vi.fn();
  expect(() => client.readHistory([{ from: 0, to: 1 }, invalid], visit)).toThrow('History spans');
  expect(visit).not.toHaveBeenCalled();
});

test('history readers expire and native resources are freed on successful, throwing and async callbacks', async () => {
  const { client } = open();
  client.local.insert(root, a, undefined, Uint8Array.of(1));
  const freed = vi.spyOn(WasmHistoryReader.prototype, 'free');
  let escaped!: MemoryReader;
  let continuation!: Promise<unknown>;
  client.readHistory([{ from: 0, to: 1 }], ({ after }) => {
    escaped = after;
    expect(after.getContent(a)?.parentId).toBe(root);
    expect(after.getChildren(root)).toEqual([a]);
    expect(after.getPosition(a)).toBe(0);
  });
  expect(() => escaped.get(a)).toThrow('finished');
  expect(() => escaped.getContent(a)).toThrow('finished');
  expect(() => escaped.getChildren(root)).toThrow('finished');
  expect(() => escaped.getPosition(a)).toThrow('finished');
  expect(() => escaped.nodeIds()).toThrow('finished');
  expect(() =>
    client.readHistory([{ from: 0, to: 1 }], ({ before }) => {
      escaped = before;
      expect(before.getChildren(root)).toEqual([]);
      expect(before.getPosition(a)).toBeUndefined();
      throw new Error('callback failure');
    }),
  ).toThrow('callback failure');
  expect(() => escaped.get(a)).toThrow('finished');
  expect(() => escaped.getChildren(root)).toThrow('finished');
  expect(() => escaped.getPosition(a)).toThrow('finished');
  expect(() =>
    client.readHistory([{ from: 0, to: 1 }], ({ after }) => {
      continuation = Promise.resolve().then(() => after.get(a));
      return continuation;
    }),
  ).toThrow('must be synchronous');
  await expect(continuation).rejects.toThrow('finished');
  expect(freed).toHaveBeenCalledTimes(3);
  expect(() => client.transact(() => client.readHistory([], () => {}))).toThrow(
    'during a transaction',
  );
});

test('history captures its ranges and remains readable after a callback changes and closes the source', () => {
  const { client } = open();
  client.local.insert(root, a, undefined, Uint8Array.of(1));
  client.local.payload(a, Uint8Array.of(2));
  const spans = [
    { from: 0, to: 1 },
    { from: 1, to: 2 },
  ];
  client.readHistory(spans, ({ after }, index) => {
    if (index === 0) {
      spans[1].to = 999;
      client.local.payload(a, Uint8Array.of(3));
      client.close();
    }
    expect(after.get(a)?.payload).toEqual(Uint8Array.of(index + 1));
    expect(after.getContent(a)?.payload).toEqual(Uint8Array.of(index + 1));
    expect(after.getChildren(root)).toEqual([a]);
    expect(after.getPosition(a)).toBe(0);
  });
});

test('composed commands read live rows and publish only owned changed records', () => {
  const { client, native } = open();
  client.transact(({ local }) => {
    local.insert('0', '1', undefined, Uint8Array.of(1));
    local.insert(root, b);
  });
  const enumeration = vi.spyOn(native, 'nodeIds');
  const drainChanges = vi.spyOn(native, 'drainReadChanges');
  const listener = vi.fn();
  client.subscribe(listener);
  const retainedRow = client.get(a)!;
  const retainedContent = client.getContent(a)!;
  const retainedChildren = client.getChildren(root);
  expect(retainedContent).not.toHaveProperty('children');
  expect(retainedChildren).toEqual([a, b]);
  expect(client.getChildren('0')).toBe(retainedChildren);
  expect(client.getPosition(b)).toBe(1);
  let escaped!: MemoryTransaction;
  const result = client.transact((transaction) => {
    escaped = transaction;
    transaction.local.move('1', '2');
    expect(transaction.get(a)?.parentId).toBe(b);
    expect(transaction.getContent(a)?.parentId).toBe(b);
    expect(transaction.getChildren(root)).toEqual([b]);
    const children = transaction.getChildren(b);
    expect(children).toEqual([a]);
    expect(transaction.getChildren('2')).toBe(children);
    expect(transaction.getPosition(a)).toBe(0);
    expect(transaction.getPosition(b)).toBe(0);
    drainChanges.mockClear();
    const moved = transaction.getChanges();
    expect(transaction.getChanges()).toBe(moved);
    expect(drainChanges).not.toHaveBeenCalled();
    transaction.local.payload(a, Uint8Array.of(2));
    expect(transaction.getChanges()).not.toBe(moved);
    expect(transaction.get(a)?.payload).toEqual(Uint8Array.of(2));
    expect(transaction.getContent(a)?.payload).toEqual(Uint8Array.of(2));
    expect(transaction.getChildren(b)).toBe(children);
    expect(moved.changes.find((change) => change.id === a)?.after?.payload).toEqual(
      Uint8Array.of(1),
    );
    expect(listener).not.toHaveBeenCalled();
    expect(() => client.operationsFrom(0)).toThrow('during a transaction');
  });
  expect(drainChanges).toHaveBeenCalledTimes(1);
  const change = result.changes.changes.find((change) => change.id === a)!;
  expect(change.before).toMatchObject({ parentId: root, payload: Uint8Array.of(1) });
  expect(change.after).toMatchObject({ parentId: b, payload: Uint8Array.of(2) });
  expect(result.operations.map((operation) => operation.kind.type)).toEqual(['move', 'payload']);
  expect(listener).toHaveBeenCalledTimes(1);
  expect(listener).toHaveBeenCalledWith(result.changes);
  expect(enumeration).not.toHaveBeenCalled();
  expect(retainedRow.parentId).toBe(root);
  expect(retainedContent.parentId).toBe(root);
  retainedContent.payload!.fill(99);
  expect(client.getContent(a)?.payload).toEqual(Uint8Array.of(2));
  expect(retainedChildren).toEqual([a, b]);
  expect(() => (retainedChildren as string[]).push('invalid')).toThrow();
  retainedRow.payload!.fill(99);
  expect(retainedRow.payload).toEqual(Uint8Array.of(1));
  expect(() => (retainedRow.children as string[]).push('invalid')).toThrow();
  change.after!.payload!.fill(99);
  expect(change.after!.payload).toEqual(Uint8Array.of(2));
  expect(() => escaped.get(a)).toThrow('finished');
  expect(() => escaped.getContent(a)).toThrow('finished');
  expect(() => escaped.getChildren(b)).toThrow('finished');
  expect(() => escaped.getPosition(a)).toThrow('finished');
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
        expect(transaction.getChildren(root)).toEqual([a]);
        expect(transaction.getPosition(a)).toBe(0);
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
    expect(client.getContent(a)).toBeUndefined();
    expect(client.getChildren(root)).toEqual([]);
    expect(client.getPosition(a)).toBeUndefined();
    expect(client.operationsFrom(0)).toEqual([]);
    expect(client.revision).toBe(0);
    expect(listener).not.toHaveBeenCalled();
    expect(client.transact(({ getChanges }) => getChanges()).value.changes).toEqual([]);
    expect(client.local.insert(root, b)).toEqual(control.local.insert(root, b));
    expect(client.getChildren(root)).toEqual([b]);
    expect(client.getPosition(b)).toBe(0);
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
  client.local.insert(root, b);
  const revision = client.revision;
  const listener = vi.fn();
  client.subscribe(listener);
  const result = client.transact(({ local, getChanges, getChildren, getPosition }) => {
    local.payload(a, Uint8Array.of(2));
    local.move(a, b);
    expect(getChildren(root)).toEqual([b]);
    expect(getChildren(b)).toEqual([a]);
    expect(getPosition(b)).toBe(0);
    const changed = getChanges();
    local.payload(a, Uint8Array.of(1));
    local.move(a, root, null);
    expect(getChildren(root)).toEqual([a, b]);
    expect(getChildren(b)).toEqual([]);
    expect(getPosition(b)).toBe(1);
    const restored = getChanges();
    expect(restored).not.toBe(changed);
    expect(restored.changes).toEqual([]);
    expect(getChanges()).toBe(restored);
  });
  expect(result.operations).toHaveLength(4);
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
    transaction.getChanges();
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
  expect(receiver.getChildren(root)).toEqual([]);
  expect(receiver.getPosition(a)).toBeUndefined();
  const listener = vi.fn();
  receiver.subscribe(listener);
  receiver.appendOperations([insert]);
  expect(receiver.get(a)?.payload).toEqual(Uint8Array.of(42));
  expect(receiver.getContent(a)?.payload).toEqual(Uint8Array.of(42));
  expect(receiver.getChildren(root)).toEqual([a]);
  expect(receiver.getPosition(a)).toBe(0);
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
  expect(() => receiver.getContent(a)).toThrow('closed');
  expect(() => receiver.getChildren(root)).toThrow('closed');
  expect(() => receiver.getPosition(a)).toThrow('closed');
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

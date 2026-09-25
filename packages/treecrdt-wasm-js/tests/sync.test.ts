import { afterEach, expect, test, vi } from 'vitest';
import { bytesToHex } from '@treecrdt/interface/ids';
import { deriveOpRefV0 } from '@treecrdt/sync-protocol';
import type { Operation } from '@treecrdt/interface';
import { createMemoryClient, type MemoryClient } from '../dist/index.node.js';
import { createMemorySyncBackend } from '../dist/sync.js';

const root = '0'.repeat(32);
const a = '1'.repeat(32);
const b = '2'.repeat(32);
const clients: MemoryClient[] = [];
async function open() {
  const client = await createMemoryClient();
  clients.push(client);
  return client;
}
afterEach(() => {
  for (const client of clients.splice(0)) client.close();
});

test('uses canonical refs, returns requested operation order and rejects missing or unsupported filters', async () => {
  const client = await open();
  const one = client.local.insert(root, a, null, new Uint8Array([1]));
  const two = client.local.insert(root, b, a, new Uint8Array([2]));
  const backend = createMemorySyncBackend(client, { docId: 'document' });
  const refs = await backend.listOpRefs({ all: {} });
  expect(refs.map(bytesToHex)).toEqual(
    [one, two].map((operation) => bytesToHex(deriveOpRefV0('document', operation.meta.id))),
  );
  expect(await backend.getOpsByOpRefs([refs[1]!, refs[0]!, refs[1]!])).toEqual([two, one, two]);
  await expect(backend.getOpsByOpRefs([new Uint8Array(16)])).rejects.toThrow(/Missing/);
  await expect(backend.listOpRefs({ children: { parent: new Uint8Array(16) } })).rejects.toThrow(
    /full-document/,
  );
  expect(await backend.maxLamport()).toBe(BigInt(two.meta.lamport));
  const other = createMemorySyncBackend(client, { docId: 'other' });
  expect((await other.listOpRefs({ all: {} })).map(bytesToHex)).not.toEqual(refs.map(bytesToHex));
});

test('applies a remote batch atomically, publishes once, and treats duplicate delivery as a no-op', async () => {
  const source = await open();
  const receiver = await open();
  const operations = source.transact((client) => {
    client.local.insert(root, a, null, new Uint8Array([1]));
    client.local.insert(a, b, null, new Uint8Array([2]));
  }).operations;
  const backend = createMemorySyncBackend(receiver, { docId: 'document' });
  const listener = vi.fn();
  receiver.subscribe(listener);
  await backend.applyOps(operations);
  const snapshot = receiver.getSnapshot();
  expect(snapshot.get(a)?.children).toEqual([b]);
  expect(listener).toHaveBeenCalledTimes(1);
  await backend.applyOps(operations);
  expect(receiver.getSnapshot()).toBe(snapshot);
  expect(listener).toHaveBeenCalledTimes(1);
  const invalid = { ...operations[0]!, kind: { type: 'delete', node: 'invalid' } };
  const extra = source.local.payload(b, new Uint8Array([9]));
  await expect(backend.applyOps([extra, invalid as typeof extra])).rejects.toThrow();
  expect(receiver.getSnapshot()).toBe(snapshot);
  expect(receiver.operationCount()).toBe(2);
  expect(listener).toHaveBeenCalledTimes(1);
});

test('indexes committed native log entries only, including local writes after synchronization begins', async () => {
  const client = await open();
  const backend = createMemorySyncBackend(client, { docId: 'document' });
  expect(await backend.listOpRefs({ all: {} })).toEqual([]);
  expect(() =>
    client.transact((document) => {
      document.local.insert(root, a, null, new Uint8Array([1]));
      document.operationsFrom(0);
    }),
  ).toThrow(/during a transaction/);
  expect(await backend.listOpRefs({ all: {} })).toEqual([]);
  const operation = client.local.insert(root, b, null, new Uint8Array([2]));
  const refs = await backend.listOpRefs({ all: {} });
  expect(await backend.getOpsByOpRefs(refs)).toEqual([operation]);
});

test('reconciles a shuffled batch without changing retained arrival order', async () => {
  const source = await open();
  const receiver = await open();
  const insertedA = source.local.insert(root, a, null, new Uint8Array([1]));
  const insertedB = source.local.insert(root, b, a, new Uint8Array([2]));
  const renamedA = source.local.payload(a, new Uint8Array([3]));
  const first: Operation = {
    ...insertedA,
    meta: {
      ...insertedA.meta,
      lamport: 1,
      id: { replica: new Uint8Array(32).fill(2), counter: 1 },
    },
  };
  const second: Operation = {
    ...insertedB,
    meta: {
      ...insertedB.meta,
      lamport: 1,
      id: { replica: new Uint8Array(32).fill(1), counter: 9 },
    },
  };
  const renamed: Operation = {
    ...renamedA,
    meta: { ...renamedA.meta, lamport: 1, id: { replica: first.meta.id.replica, counter: 2 } },
  };
  const later: Operation = {
    ...source.local.payload(b, new Uint8Array([4])),
    meta: { ...insertedB.meta, lamport: 2 },
  };
  const operations = [later, renamed, first, second];
  const backend = createMemorySyncBackend(receiver, { docId: 'shuffled' });
  const listener = vi.fn();
  receiver.subscribe(listener);
  await backend.applyOps(operations);
  expect(operations).toEqual([later, renamed, first, second]);
  expect(await backend.getOpsByOpRefs(await backend.listOpRefs({ all: {} }))).toEqual(operations);
  expect(receiver.tree.children(root)).toEqual([a, b]);
  expect(receiver.tree.payload(a)).toEqual(new Uint8Array([3]));
  expect(receiver.tree.payload(b)).toEqual(new Uint8Array([4]));
  expect(listener).toHaveBeenCalledTimes(1);
});

test('ingests historical operations behind a newer head and keeps the first duplicate envelope', async () => {
  const source = await open();
  const receiver = await open();
  const first = source.local.insert(root, a, null, new Uint8Array([1]));
  const second = source.local.insert(root, b, a, new Uint8Array([2]));
  const renamed = source.local.payload(a, new Uint8Array([3]));
  const newest = source.local.payload(root, new Uint8Array([4]));
  const conflicting = {
    ...first,
    kind: { ...first.kind, payload: new Uint8Array([9]) },
  } as Operation;
  const backend = createMemorySyncBackend(receiver, { docId: 'historical' });
  const listener = vi.fn();
  receiver.subscribe(listener);
  await backend.applyOps([newest]);
  await backend.applyOps([renamed, first, second, conflicting, renamed]);
  expect(receiver.tree.children(root)).toEqual([a, b]);
  expect(receiver.tree.payload(a)).toEqual(new Uint8Array([3]));
  expect(await backend.getOpsByOpRefs(await backend.listOpRefs({ all: {} }))).toEqual([
    newest,
    renamed,
    first,
    second,
  ]);
  const snapshot = receiver.getSnapshot();
  await backend.applyOps([conflicting, renamed]);
  expect(receiver.getSnapshot()).toBe(snapshot);
  expect(listener).toHaveBeenCalledTimes(2);
  const next = receiver.local.payload(b, new Uint8Array([5]));
  expect(next.meta.id.counter).toBe(1);
  expect(next.meta.lamport).toBe(newest.meta.lamport + 1);
});

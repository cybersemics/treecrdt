import { afterEach, expect, test, vi } from 'vitest';
import { createMemoryClient, type MemoryClient } from '../dist/index.node.js';
import { createWasmAdapter } from '../dist/adapter.js';
import { decodeSqliteOps } from '@treecrdt/interface/sqlite';
import { WasmTree } from '../pkg-web/treecrdt_wasm.js';

const root = '0'.repeat(32);
const a = '1'.repeat(32);
const b = '2'.repeat(32);
const payload = new Uint8Array([0, 127, 255]);
const clients: MemoryClient[] = [];
async function open() {
  const client = await createMemoryClient();
  clients.push(client);
  return client;
}
afterEach(() => {
  for (const client of clients.splice(0)) client.close();
});

test('initializes explicitly and returns synchronous typed local operations and reads', async () => {
  const client = await open();
  const operation = client.local.insert(root, a, null, payload);
  expect(operation).not.toBeInstanceOf(Promise);
  expect(operation.kind).toMatchObject({ type: 'insert', parent: root, node: a, payload });
  expect(operation.meta.id.replica).toBeInstanceOf(Uint8Array);
  expect(client.tree.children(root)).toEqual([a]);
  expect(client.tree.get(a)?.payload).toEqual(payload);
  expect(client.operationsFrom(0)).toEqual([operation]);
  expect(client.operationsAt([0, 0])).toEqual([operation, operation]);
  expect(() => client.operationsAt([1])).toThrow();
});

test('composes read-your-writes commands and publishes one completed snapshot', async () => {
  const client = await open();
  const initial = client.getSnapshot();
  const listener = vi.fn(() => expect(client.tree.children(a)).toEqual([b]));
  client.subscribe(listener);
  const result = client.transact((document) => {
    document.local.insert(root, a, null, payload);
    expect(document.getSnapshot().get(a)?.payload).toEqual(payload);
    expect(listener).not.toHaveBeenCalled();
    document.local.insert(a, b, null, payload);
    expect(document.getSnapshot().get(a)?.children).toEqual([b]);
    expect(listener).not.toHaveBeenCalled();
    return 42;
  });
  expect(result.value).toBe(42);
  expect(result.operations).toHaveLength(2);
  expect(listener).toHaveBeenCalledTimes(1);
  expect(initial.has(a)).toBe(false);
  expect(client.getSnapshot()).toBe(client.getSnapshot());
});

test('restores snapshot identity, native log and clock after a transaction throws following intermediate reads', async () => {
  const client = await open();
  client.local.insert(root, a, null, payload);
  const initial = client.getSnapshot();
  const count = client.operationCount();
  const lamport = client.maxLamport();
  const listener = vi.fn();
  client.subscribe(listener);
  const failure = new Error('rejected');
  expect(() =>
    client.transact((document) => {
      document.local.payload(a, new Uint8Array([3]));
      expect(document.getSnapshot()).not.toBe(initial);
      document.local.insert(a, b, null, payload);
      expect(document.getSnapshot().has(b)).toBe(true);
      throw failure;
    }),
  ).toThrow(failure);
  expect(client.getSnapshot()).toBe(initial);
  expect(client.operationCount()).toBe(count);
  expect(client.maxLamport()).toBe(lamport);
  expect(client.tree.exists(b)).toBe(false);
  expect(listener).not.toHaveBeenCalled();
  expect(client.local.payload(a, new Uint8Array([4])).meta.id.counter).toBe(count + 1);
});

test('preserves map and unchanged row identity and keeps net no-op transactions quiet', async () => {
  const client = await open();
  client.transact((document) => {
    document.local.insert(root, a, null, payload);
    document.local.insert(root, b, a, payload);
  });
  const initial = client.getSnapshot();
  const listener = vi.fn();
  client.subscribe(listener);
  const invisible = client.transact((document) => document.local.payload(a, payload));
  expect(invisible.operations).toEqual([invisible.value]);
  expect(client.getSnapshot()).toBe(initial);
  client.transact((document) => {
    document.local.payload(a, new Uint8Array([3]));
    expect(document.getSnapshot()).not.toBe(initial);
    document.local.payload(a, payload);
  });
  expect(client.getSnapshot()).toBe(initial);
  expect(listener).not.toHaveBeenCalled();
  client.local.payload(a, new Uint8Array([4]));
  expect(client.getSnapshot()).not.toBe(initial);
  expect(client.getSnapshot().get(b)).toBe(initial.get(b));
  expect(listener).toHaveBeenCalledTimes(1);
});

test('does not expose mutable snapshot maps, rows, children or payload storage', async () => {
  const client = await open();
  client.local.insert(root, a, null, payload);
  const snapshot = client.getSnapshot();
  expect('set' in snapshot).toBe(false);
  expect('delete' in snapshot).toBe(false);
  expect(Object.isFrozen(snapshot.get(a))).toBe(true);
  expect(Object.isFrozen(snapshot.get(a)!.children)).toBe(true);
  snapshot.get(a)!.payload![0] = 9;
  expect(snapshot.get(a)!.payload).toEqual(payload);
  snapshot.forEach((_row, _id, view) => expect(view).toBe(snapshot));
});

test('publishes payload-less nodes and clearing payloads as visible snapshot changes', async () => {
  const client = await open();
  expect(client.getSnapshot().get(root)?.payload).toBeNull();
  const listener = vi.fn();
  client.subscribe(listener);
  client.local.insert(root, a);
  const inserted = client.getSnapshot();
  expect(inserted.get(a)).toMatchObject({ id: a, parentId: root, payload: null });
  expect(inserted.get(root)?.children).toEqual([a]);
  expect(listener).toHaveBeenCalledTimes(1);
  client.local.payload(a, payload);
  expect(client.getSnapshot().get(a)?.payload).toEqual(payload);
  client.local.payload(a, null);
  expect(client.getSnapshot().get(a)?.payload).toBeNull();
  expect(listener).toHaveBeenCalledTimes(3);
});

test('cannot commit earlier writes when a caught mutator error poisons the native transaction', async () => {
  const client = await open();
  const initial = client.getSnapshot();
  const listener = vi.fn();
  client.subscribe(listener);
  expect(() =>
    client.transact((document) => {
      document.local.insert(root, a, null, payload);
      expect(() => document.local.move(a, 'not-a-node')).toThrow();
    }),
  ).toThrow();
  expect(client.getSnapshot()).toBe(initial);
  expect(client.operationCount()).toBe(0);
  expect(listener).not.toHaveBeenCalled();
});

test('rejects nested and asynchronous transactions without retaining earlier writes', async () => {
  const client = await open();
  const initial = client.getSnapshot();
  expect(() =>
    client.transact((document) => {
      document.local.insert(root, a, null, payload);
      document.transact(() => {});
    }),
  ).toThrow(/Nested/);
  expect(() =>
    client.transact((document) => {
      document.local.insert(root, a, null, payload);
      return Promise.resolve();
    }),
  ).toThrow(/synchronous/);
  expect(client.getSnapshot()).toBe(initial);
  expect(client.operationCount()).toBe(0);
});

test('isolates subscriber errors and supports unsubscribing and idempotent close', async () => {
  const client = await open();
  const error = new Error('subscriber failed');
  const report = vi.spyOn(console, 'error').mockImplementation(() => {});
  const listener = vi.fn();
  client.subscribe(() => {
    throw error;
  });
  const unsubscribe = client.subscribe(listener);
  expect(() => client.local.insert(root, a, null, payload)).not.toThrow();
  expect(report).toHaveBeenCalledWith('TreeCRDT subscriber failed', error);
  expect(listener).toHaveBeenCalledTimes(1);
  unsubscribe();
  client.local.payload(a, new Uint8Array([2]));
  expect(listener).toHaveBeenCalledTimes(1);
  client.close();
  client.close();
  expect(() => client.getSnapshot()).toThrow(/closed/);
});

test('keeps the legacy benchmark adapter backed by typed operations including insert payloads', async () => {
  const source = await open();
  const operation = source.local.insert(root, a, null, payload);
  const adapter = await createWasmAdapter();
  try {
    await adapter.appendOps!(
      [operation],
      () => new Uint8Array(),
      (replica) => replica,
    );
    expect(decodeSqliteOps(await adapter.opsSince(0))).toEqual([operation]);
    expect(await adapter.treePayload(new Uint8Array(16).fill(0x11))).toEqual(payload);
  } finally {
    await adapter.close?.();
  }
});

test('rolls back after native ingestion mutated state and a JS boundary throws', async () => {
  const source = await open();
  const client = await open();
  client.local.insert(root, a, null, payload);
  const incoming = source.local.insert(root, b, null, payload);
  const snapshot = client.getSnapshot();
  const count = client.operationCount();
  const append = WasmTree.prototype.appendOperations;
  const failure = new Error('failure after ingestion');
  const listener = vi.fn();
  client.subscribe(listener);
  vi.spyOn(WasmTree.prototype, 'appendOperations').mockImplementationOnce(function (
    this: WasmTree,
    operations,
  ) {
    append.call(this, operations);
    throw failure;
  });
  expect(() => client.appendOperations([incoming])).toThrow(failure);
  expect(client.getSnapshot()).toBe(snapshot);
  expect(client.operationCount()).toBe(count);
  expect(client.tree.exists(b)).toBe(false);
  expect(listener).not.toHaveBeenCalled();
  expect(client.local.payload(a, new Uint8Array([4])).meta.id.counter).toBe(count + 1);
});

test('preserves received history and the next local identity when a later placement is invalid', async () => {
  const source = await open();
  const client = await open();
  const original = source.local.insert(root, a, null, payload);
  client.appendOperations([original]);
  const snapshot = client.getSnapshot();
  expect(() =>
    client.transact((document) => {
      document.local.payload(a, new Uint8Array([9]));
      document.local.insert(root, b, '3'.repeat(32), payload);
    }),
  ).toThrow(/after node/);
  expect(client.getSnapshot()).toBe(snapshot);
  expect(client.operationsFrom(0)).toEqual([original]);
  const next = client.local.payload(a, new Uint8Array([4]));
  expect(next.meta.id.counter).toBe(1);
  expect(next.meta.lamport).toBe(original.meta.lamport + 1);
});
